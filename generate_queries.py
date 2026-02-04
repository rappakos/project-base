"""Generate synthetic queries from sampled projects using LLM.

For each sampled project, generates two query variants:
- specific: mentions exact skills/industry from the project
- vague: more general query that should still match
"""

import asyncio
import json
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


def format_project_for_prompt(project: dict) -> str:
    """Format a project for the LLM prompt."""
    skills = project.get("skills", [])
    skills_str = ", ".join(skills[:10]) if skills else "not specified"
    
    contribution = project.get("contribution", "") or ""
    if len(contribution) > 500:
        contribution = contribution[:500] + "..."
    
    return f"""Industry: {project.get('industry') or 'not specified'}
Position: {project.get('project_position') or 'not specified'}
Skills: {skills_str}
Contribution: {contribution or 'not specified'}
Duration: {project.get('start_date') or '?'} to {project.get('end_date') or '?'}"""


def generate_queries_for_project(client: OpenAI, project: dict) -> tuple[str, str]:
    """
    Generate specific and vague queries for a project.
    
    Returns (specific_query, vague_query)
    """
    project_text = format_project_for_prompt(project)
    
    prompt = f"""You are helping create test queries for a project search system.

Given this project from someone's work history:
{project_text}

Generate two search queries that a recruiter might use to find this project:

1. SPECIFIC: A detailed query mentioning specific skills, tools, or industry from the project. Should clearly match this project.

2. VAGUE: A more general query that this project would still reasonably match, but uses broader terms or describes the work differently.

Both queries should be realistic - 1-2 sentences, like what a recruiter would actually type.

Respond in JSON format:
{{"specific": "your specific query here", "vague": "your vague query here"}}"""

    response = client.chat.completions.create(
        model=config.LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    return result.get("specific", ""), result.get("vague", "")


async def generate_all_queries(batch_size: int = 10, skip_existing: bool = True):
    """
    Generate synthetic queries for all sampled projects.
    
    Args:
        batch_size: Number of projects to process before committing
        skip_existing: Skip projects that already have queries
    """
    client = get_llm_client()
    
    async with await db.get_connection() as conn:
        # Get sampled projects
        projects = await db.get_sampled_projects(conn)
        
        if not projects:
            print("No sampled projects found. Run sample_projects.py first.")
            return
        
        print(f"Found {len(projects)} sampled projects")
        
        # Get existing queries if skipping
        existing_project_ids = set()
        if skip_existing:
            async with conn.execute(
                "SELECT DISTINCT source_project_id FROM synthetic_queries"
            ) as cursor:
                rows = await cursor.fetchall()
                existing_project_ids = {row[0] for row in rows}
            
            if existing_project_ids:
                print(f"Skipping {len(existing_project_ids)} projects with existing queries")
        
        # Filter to projects needing queries
        projects_to_process = [
            p for p in projects 
            if p["user_project_history_id"] not in existing_project_ids
        ]
        
        if not projects_to_process:
            print("All projects already have queries.")
            return
        
        print(f"Generating queries for {len(projects_to_process)} projects...")
        
        generated = 0
        errors = 0
        
        for i, project in enumerate(projects_to_process):
            try:
                specific, vague = generate_queries_for_project(client, project)
                
                if specific:
                    await db.insert_synthetic_query(
                        conn,
                        project["user_project_history_id"],
                        specific,
                        "specific"
                    )
                
                if vague:
                    await db.insert_synthetic_query(
                        conn,
                        project["user_project_history_id"],
                        vague,
                        "vague"
                    )
                
                generated += 1
                
                # Commit in batches
                if (i + 1) % batch_size == 0:
                    await conn.commit()
                    print(f"  Processed {i + 1}/{len(projects_to_process)}...")
                    
            except Exception as e:
                errors += 1
                print(f"  Error processing project {project['user_project_history_id']}: {e}")
                continue
        
        await conn.commit()
        
        print(f"\nGeneration complete:")
        print(f"  Projects processed: {generated}")
        print(f"  Errors: {errors}")
        
        # Get total query count
        async with conn.execute("SELECT COUNT(*) FROM synthetic_queries") as cursor:
            total = (await cursor.fetchone())[0]
        print(f"  Total queries in database: {total}")


async def show_sample_queries(n: int = 5):
    """Show a sample of generated queries."""
    async with await db.get_connection() as conn:
        async with conn.execute(
            """
            SELECT sq.query_text, sq.query_type, p.industry, p.skills
            FROM synthetic_queries sq
            JOIN projects p ON sq.source_project_id = p.user_project_history_id
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (n,)
        ) as cursor:
            rows = await cursor.fetchall()
        
        if not rows:
            print("No queries found.")
            return
        
        print(f"Sample queries ({n}):\n")
        for query_text, query_type, industry, skills in rows:
            print(f"[{query_type.upper()}] {query_text}")
            print(f"  Industry: {industry}, Skills: {skills[:100]}...")
            print()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "show":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        asyncio.run(show_sample_queries(n))
    else:
        asyncio.run(generate_all_queries())
