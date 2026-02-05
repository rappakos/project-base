"""Evaluate retrieval quality by running synthetic queries against Elasticsearch.

For each synthetic query:
1. Search Elasticsearch for top-k results
2. Find the rank of the ground-truth project (source_project_id)
3. Calculate reciprocal rank
4. Store results in evaluation_runs table
"""

import asyncio
from typing import Optional
import aiosqlite

import db
import config
import elastic_client


def calculate_reciprocal_rank(
    retrieved_ids: list[int],
    ground_truth_id: int
) -> tuple[Optional[int], float]:
    """
    Calculate the rank and reciprocal rank of ground truth in results.
    
    Returns (rank, reciprocal_rank) where rank is 1-indexed or None if not found.
    """
    try:
        rank = retrieved_ids.index(ground_truth_id) + 1  # 1-indexed
        return rank, 1.0 / rank
    except ValueError:
        return None, 0.0


async def evaluate_queries(
    top_k: int = None,
    skip_existing: bool = True
):
    """
    Run evaluation for all synthetic queries.
    
    Args:
        top_k: Number of results to retrieve per query (default from config)
        skip_existing: Skip queries that already have evaluation runs
    """
    top_k = top_k or config.TOP_K_RETRIEVAL
    
    # Check Elastic connection first
    if not elastic_client.check_connection():
        print("Cannot connect to Elasticsearch. Check your configuration.")
        return
    
    es_client = elastic_client.get_client()
    
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        # Get queries to evaluate
        if skip_existing:
            queries = await db.get_unevaluated_queries(conn)
        else:
            queries = await db.get_synthetic_queries(conn)
        
        if not queries:
            print("No queries to evaluate. Generate queries first with generate_queries.py")
            return
        
        print(f"Evaluating {len(queries)} queries against Elasticsearch...")
        print(f"Top-k: {top_k}")
        
        total_rr = 0.0
        hits = 0
        errors = 0
        
        for i, query in enumerate(queries):
            try:
                # Search Elasticsearch
                results = elastic_client.search_projects(
                    query["query_text"],
                    top_k=top_k,
                    client=es_client
                )
                
                # Extract IDs in rank order
                retrieved_ids = [r["id"] for r in results]
                
                # Calculate metrics
                ground_truth_id = query["source_project_id"]
                rank, rr = calculate_reciprocal_rank(retrieved_ids, ground_truth_id)
                
                # Store result
                await db.insert_evaluation_run(
                    conn,
                    query["query_id"],
                    retrieved_ids,
                    rank,
                    rr
                )
                
                total_rr += rr
                if rank is not None:
                    hits += 1
                
                # Progress update
                if (i + 1) % 50 == 0:
                    await conn.commit()
                    current_mrr = total_rr / (i + 1)
                    current_hit_rate = hits / (i + 1)
                    print(f"  {i + 1}/{len(queries)} - MRR: {current_mrr:.4f}, Hit Rate: {current_hit_rate:.2%}")
                    
            except Exception as e:
                errors += 1
                print(f"  Error evaluating query {query['query_id']}: {e}")
                continue
        
        await conn.commit()
        
        # Final metrics
        n_evaluated = len(queries) - errors
        if n_evaluated > 0:
            mrr = total_rr / n_evaluated
            hit_rate = hits / n_evaluated
            
            print(f"\n{'='*50}")
            print(f"Evaluation complete:")
            print(f"  Queries evaluated: {n_evaluated}")
            print(f"  Errors: {errors}")
            print(f"  Mean Reciprocal Rank (MRR): {mrr:.4f}")
            print(f"  Hit Rate @{top_k}: {hit_rate:.2%}")
            print(f"  Hits: {hits}/{n_evaluated}")
            print(f"{'='*50}")
    finally:
        await conn.close()


async def show_metrics():
    """Display evaluation metrics from database views."""
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        # Overall MRR
        async with conn.execute("SELECT * FROM v_mrr_overall") as cursor:
            row = await cursor.fetchone()
            if row:
                mrr, total, hits, hit_rate = row
                print(f"\n{'='*50}")
                print("Overall Metrics:")
                print(f"  MRR: {mrr:.4f}" if mrr else "  MRR: N/A")
                print(f"  Hit Rate: {hit_rate:.2%}" if hit_rate else "  Hit Rate: N/A")
                print(f"  Total Queries: {total}")
            else:
                print("No evaluation data found.")
                return
        
        # Hit rate at different k
        async with conn.execute("SELECT * FROM v_hit_rate_at_k") as cursor:
            row = await cursor.fetchone()
            if row:
                print(f"\nHit Rate by K:")
                print(f"  @1:  {row[0]:.2%}" if row[0] else "  @1:  N/A")
                print(f"  @3:  {row[1]:.2%}" if row[1] else "  @3:  N/A")
                print(f"  @5:  {row[2]:.2%}" if row[2] else "  @5:  N/A")
                print(f"  @10: {row[3]:.2%}" if row[3] else "  @10: N/A")
                print(f"  @20: {row[4]:.2%}" if row[4] else "  @20: N/A")
        
        # MRR by query type
        async with conn.execute("SELECT * FROM v_mrr_by_query_type") as cursor:
            rows = await cursor.fetchall()
            if rows:
                print(f"\nMRR by Query Type:")
                for query_type, mrr, count, hits in rows:
                    print(f"  {query_type}: MRR={mrr:.4f}, n={count}, hits={hits}")
        
        # MRR by industry (top 10)
        async with conn.execute("SELECT * FROM v_mrr_by_industry LIMIT 10") as cursor:
            rows = await cursor.fetchall()
            if rows:
                print(f"\nMRR by Industry (top 10):")
                for industry, mrr, count, hits, hit_rate in rows:
                    print(f"  {industry}: MRR={mrr:.4f}, hit_rate={hit_rate:.2%}, n={count}")
        
        print(f"{'='*50}")
    finally:
        await conn.close()


async def show_failures(limit: int = 10):
    """Show queries where ground truth was not in top-10."""
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        async with conn.execute(
            f"SELECT * FROM v_failure_cases LIMIT {limit}"
        ) as cursor:
            columns = [desc[0] for desc in cursor.description]
            rows = await cursor.fetchall()
        
        if not rows:
            print("No failure cases found.")
            return
        
        print(f"\nFailure Cases (ground truth not in top-10):\n")
        for row in rows:
            case = dict(zip(columns, row))
            print(f"Query: {case['query_text']}")
            print(f"  Type: {case['query_type']}")
            print(f"  Industry: {case['industry']}")
            print(f"  Skills: {case['skills'][:80]}...")
            print(f"  Rank: {case['ground_truth_rank'] or 'NOT FOUND'}")
            print()
    finally:
        await conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "metrics":
            asyncio.run(show_metrics())
        elif sys.argv[1] == "failures":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            asyncio.run(show_failures(limit))
        elif sys.argv[1] == "--all":
            # Evaluate all queries, including those already evaluated
            asyncio.run(evaluate_queries(skip_existing=False))
    else:
        asyncio.run(evaluate_queries())
