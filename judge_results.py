"""LLM-as-judge pairwise comparison of search results.

For each query's top-k results, runs pairwise comparisons to build
a preference ranking that can be compared against Elastic's ranking.
Uses position-swapping to control for position bias.
"""

import asyncio
import json
from itertools import combinations
from collections import defaultdict
from openai import OpenAI, AzureOpenAI

import db
import config


def get_llm_client():
    """Get LLM client based on config (Azure OpenAI or OpenAI)."""
    if config.LLM_PROVIDER == "azure":
        return AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION
        )
    else:
        return OpenAI(
            api_key=config.OPENAI_API_KEY,
            base_url=config.OPENAI_BASE_URL
        )


def format_project_for_comparison(project: dict) -> str:
    """Format a project for comparison prompt."""
    skills = project.get("skills", [])
    if isinstance(skills, str):
        skills = json.loads(skills) if skills else []
    skills_str = ", ".join(skills[:8]) if skills else "not specified"
    
    contribution = project.get("contribution", "") or ""
    if len(contribution) > 400:
        contribution = contribution[:400] + "..."
    
    return f"""Industry: {project.get('industry') or 'not specified'}
Position: {project.get('project_position') or 'not specified'}
Skills: {skills_str}
Contribution: {contribution or 'not specified'}"""


def compare_pair(
    client: OpenAI,
    query_text: str,
    project_a: dict,
    project_b: dict,
    swap_order: bool = False
) -> tuple[str, str]:
    """
    Compare two projects for relevance to a query.
    
    Returns (winner, reasoning) where winner is 'A', 'B', or 'TIE'.
    If swap_order is True, presents B first (as A) to control for position bias.
    """
    if swap_order:
        project_a, project_b = project_b, project_a
    
    text_a = format_project_for_comparison(project_a)
    text_b = format_project_for_comparison(project_b)
    
    prompt = f"""You are evaluating which project is more relevant to a search query.

QUERY: "{query_text}"

PROJECT A:
{text_a}

PROJECT B:
{text_b}

Which project is MORE RELEVANT to the query? Consider:
- Does the project match the skills/technologies mentioned?
- Does the industry align with what's being searched for?
- Does the contribution describe relevant work?

Respond in JSON format:
{{"winner": "A" or "B" or "TIE", "reasoning": "brief explanation"}}"""

    response = client.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    winner = result.get("winner", "TIE").upper()
    reasoning = result.get("reasoning", "")
    
    # If we swapped, swap the winner back
    if swap_order:
        if winner == "A":
            winner = "B"
        elif winner == "B":
            winner = "A"
    
    return winner, reasoning


async def judge_query(
    llm_client: OpenAI,
    conn,
    query: dict,
    top_k: int = None
):
    """
    Run pairwise judgments for a single query's results.
    
    Uses position-swapping: runs each comparison twice with swapped order.
    If the two runs disagree, marks as TIE.
    """
    top_k = top_k or config.TOP_K_JUDGE
    
    query_id = query["query_id"]
    query_text = query["query_text"]
    retrieved_ids = query["retrieved_project_ids"][:top_k]
    
    if len(retrieved_ids) < 2:
        return 0
    
    # Load project details
    projects = {}
    for pid in retrieved_ids:
        project = await db.get_project_by_id(conn, pid)
        if project:
            projects[pid] = project
    
    if len(projects) < 2:
        return 0
    
    # Run pairwise comparisons
    judgments_made = 0
    valid_ids = [pid for pid in retrieved_ids if pid in projects]
    
    for id_a, id_b in combinations(valid_ids, 2):
        try:
            # First comparison: A vs B
            winner1, reasoning1 = compare_pair(
                llm_client, query_text, projects[id_a], projects[id_b], swap_order=False
            )
            
            # Second comparison: B vs A (position swap)
            winner2, reasoning2 = compare_pair(
                llm_client, query_text, projects[id_a], projects[id_b], swap_order=True
            )
            
            # Resolve disagreements
            if winner1 == winner2:
                final_winner = winner1
                reasoning = reasoning1
            else:
                # Position bias detected - mark as tie
                final_winner = "TIE"
                reasoning = f"Position bias: run1={winner1}, run2={winner2}"
            
            # Store judgment
            await db.insert_judgment(
                conn,
                query_id,
                id_a,
                id_b,
                final_winner,
                reasoning,
                config.LLM_MODEL
            )
            judgments_made += 1
            
        except Exception as e:
            print(f"    Error comparing {id_a} vs {id_b}: {e}")
            continue
    
    return judgments_made


async def judge_all_queries(
    top_k: int = None,
    batch_size: int = 5,
    max_queries: int = None
):
    """
    Run LLM-as-judge pairwise comparisons for all evaluated queries.
    
    Args:
        top_k: Number of top results to compare per query
        batch_size: Queries to process before committing
        max_queries: Maximum queries to judge (for cost control)
    """
    top_k = top_k or config.TOP_K_JUDGE
    llm_client = get_llm_client()
    
    async with await db.get_connection() as conn:
        # Get queries with evaluation runs but no judgments
        queries = await db.get_unjudged_queries(conn)
        
        if not queries:
            print("No queries to judge. Run evaluate_retrieval.py first.")
            return
        
        if max_queries:
            queries = queries[:max_queries]
        
        print(f"Judging {len(queries)} queries (top-{top_k} results each)...")
        print(f"Comparisons per query: {top_k * (top_k - 1) // 2}")
        
        total_judgments = 0
        
        for i, query in enumerate(queries):
            judgments = await judge_query(llm_client, conn, query, top_k)
            total_judgments += judgments
            
            if (i + 1) % batch_size == 0:
                await conn.commit()
                print(f"  Processed {i + 1}/{len(queries)} queries...")
        
        await conn.commit()
        
        print(f"\nJudging complete:")
        print(f"  Queries judged: {len(queries)}")
        print(f"  Total judgments: {total_judgments}")


async def compute_preference_rankings():
    """
    Compute preference scores from pairwise judgments.
    
    For each project, preference_score = wins / total_comparisons.
    """
    async with await db.get_connection() as conn:
        # Clear existing rankings
        await conn.execute("DELETE FROM preference_rankings")
        
        # Get all queries with judgments
        async with conn.execute(
            "SELECT DISTINCT query_id FROM judgments"
        ) as cursor:
            query_ids = [row[0] for row in await cursor.fetchall()]
        
        if not query_ids:
            print("No judgments found.")
            return
        
        for query_id in query_ids:
            # Get all judgments for this query
            async with conn.execute(
                """
                SELECT project_a_id, project_b_id, winner 
                FROM judgments 
                WHERE query_id = ?
                """,
                (query_id,)
            ) as cursor:
                judgments = await cursor.fetchall()
            
            # Count wins and comparisons for each project
            wins = defaultdict(int)
            comparisons = defaultdict(int)
            
            for a_id, b_id, winner in judgments:
                comparisons[a_id] += 1
                comparisons[b_id] += 1
                
                if winner == "A":
                    wins[a_id] += 1
                elif winner == "B":
                    wins[b_id] += 1
                else:  # TIE
                    wins[a_id] += 0.5
                    wins[b_id] += 0.5
            
            # Get elastic ranks from evaluation_runs
            async with conn.execute(
                """
                SELECT retrieved_project_ids FROM evaluation_runs WHERE query_id = ?
                """,
                (query_id,)
            ) as cursor:
                row = await cursor.fetchone()
                elastic_order = json.loads(row[0]) if row else []
            
            elastic_rank = {pid: i + 1 for i, pid in enumerate(elastic_order)}
            
            # Insert preference rankings
            for project_id in comparisons:
                score = wins[project_id] / comparisons[project_id] if comparisons[project_id] > 0 else 0
                
                await conn.execute(
                    """
                    INSERT INTO preference_rankings (query_id, project_id, preference_score, elastic_rank)
                    VALUES (?, ?, ?, ?)
                    """,
                    (query_id, project_id, score, elastic_rank.get(project_id))
                )
        
        await conn.commit()
        print(f"Computed preference rankings for {len(query_ids)} queries")


async def show_ranking_comparison(n: int = 5):
    """Show queries where LLM ranking differs significantly from Elastic."""
    async with await db.get_connection() as conn:
        # Find queries with ranking disagreements
        async with conn.execute(
            """
            SELECT 
                sq.query_id,
                sq.query_text,
                pr.project_id,
                pr.preference_score,
                pr.elastic_rank
            FROM preference_rankings pr
            JOIN synthetic_queries sq ON pr.query_id = sq.query_id
            WHERE pr.elastic_rank IS NOT NULL
            ORDER BY ABS(pr.preference_score - (1.0 / pr.elastic_rank)) DESC
            LIMIT ?
            """,
            (n * 3,)  # Get more to group by query
        ) as cursor:
            rows = await cursor.fetchall()
        
        if not rows:
            print("No ranking data found. Run judge_results.py and compute_preference_rankings() first.")
            return
        
        print(f"\nRanking Comparisons (LLM preference vs Elastic rank):\n")
        
        # Group by query
        by_query = defaultdict(list)
        for query_id, query_text, project_id, pref_score, elastic_rank in rows:
            by_query[(query_id, query_text)].append((project_id, pref_score, elastic_rank))
        
        shown = 0
        for (query_id, query_text), rankings in by_query.items():
            if shown >= n:
                break
            
            print(f"Query: {query_text[:80]}...")
            print(f"  {'Project ID':<12} {'LLM Pref':>10} {'Elastic Rank':>12}")
            print(f"  {'-'*36}")
            
            # Sort by LLM preference
            for project_id, pref_score, elastic_rank in sorted(rankings, key=lambda x: -x[1])[:5]:
                print(f"  {project_id:<12} {pref_score:>10.2f} {elastic_rank:>12}")
            print()
            shown += 1


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "rankings":
            asyncio.run(compute_preference_rankings())
        elif sys.argv[1] == "compare":
            n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            asyncio.run(show_ranking_comparison(n))
    else:
        # Default: run judgments for first 10 queries (cost control)
        max_q = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        asyncio.run(judge_all_queries(max_queries=max_q))
