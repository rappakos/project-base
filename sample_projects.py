"""Stratified sampling of projects for evaluation.

Groups projects by industry and samples proportionally, with a minimum
per industry to ensure coverage of smaller categories.
"""

import asyncio
import random
from collections import defaultdict

import db
import config


async def sample_projects(
    sample_size: int = None,
    min_per_industry: int = None,
    seed: int = 42
):
    """
    Sample projects with industry stratification.
    
    Args:
        sample_size: Total number of projects to sample (default from config)
        min_per_industry: Minimum samples per industry (default from config)
        seed: Random seed for reproducibility
    """
    sample_size = sample_size or config.SAMPLE_SIZE
    min_per_industry = min_per_industry or config.MIN_PER_INDUSTRY
    
    random.seed(seed)
    
    async with await db.get_connection() as conn:
        # Get all projects
        projects = await db.get_all_projects(conn)
        
        if not projects:
            print("No projects found in database. Load projects first.")
            return
        
        print(f"Total projects: {len(projects)}")
        
        # Group by industry
        by_industry = defaultdict(list)
        for p in projects:
            industry = p.get("industry") or "Unknown"
            by_industry[industry].append(p)
        
        print(f"Industries: {len(by_industry)}")
        for ind, projs in sorted(by_industry.items(), key=lambda x: -len(x[1])):
            print(f"  {ind}: {len(projs)}")
        
        # Calculate proportional allocation
        sampled = []
        remaining_budget = sample_size
        
        # First pass: ensure minimum per industry
        for industry, projs in by_industry.items():
            n_to_sample = min(min_per_industry, len(projs))
            selected = random.sample(projs, n_to_sample)
            sampled.extend(selected)
            remaining_budget -= n_to_sample
        
        # Second pass: distribute remaining budget proportionally
        if remaining_budget > 0:
            # Calculate proportions excluding already sampled
            already_sampled_ids = {p["user_project_history_id"] for p in sampled}
            remaining_projects = [p for p in projects if p["user_project_history_id"] not in already_sampled_ids]
            
            by_industry_remaining = defaultdict(list)
            for p in remaining_projects:
                industry = p.get("industry") or "Unknown"
                by_industry_remaining[industry].append(p)
            
            total_remaining = len(remaining_projects)
            
            if total_remaining > 0:
                for industry, projs in by_industry_remaining.items():
                    proportion = len(projs) / total_remaining
                    n_additional = int(remaining_budget * proportion)
                    n_additional = min(n_additional, len(projs))
                    
                    if n_additional > 0:
                        selected = random.sample(projs, n_additional)
                        sampled.extend(selected)
        
        # Trim to exact sample size if needed
        if len(sampled) > sample_size:
            sampled = random.sample(sampled, sample_size)
        
        print(f"\nSampled {len(sampled)} projects")
        
        # Clear existing samples and insert new ones
        await conn.execute("DELETE FROM sampled_projects")
        
        for p in sampled:
            await db.insert_sampled_project(
                conn,
                p["user_project_history_id"],
                p.get("industry") or "Unknown"
            )
        
        await conn.commit()
        
        # Print coverage stats
        sampled_by_industry = defaultdict(int)
        for p in sampled:
            sampled_by_industry[p.get("industry") or "Unknown"] += 1
        
        print("\nSample coverage by industry:")
        for ind, count in sorted(sampled_by_industry.items(), key=lambda x: -x[1]):
            original_count = len(by_industry[ind])
            pct = count / original_count * 100 if original_count > 0 else 0
            print(f"  {ind}: {count} ({pct:.1f}% of {original_count})")
        
        return sampled


async def get_coverage_stats():
    """Print coverage statistics for current sample."""
    async with await db.get_connection() as conn:
        async with conn.execute(
            """
            SELECT industry, COUNT(*) as count 
            FROM sampled_projects 
            GROUP BY industry 
            ORDER BY count DESC
            """
        ) as cursor:
            rows = await cursor.fetchall()
        
        if not rows:
            print("No sampled projects found.")
            return
        
        print("Current sample coverage:")
        total = sum(row[1] for row in rows)
        for industry, count in rows:
            print(f"  {industry}: {count} ({count/total*100:.1f}%)")
        print(f"Total: {total}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        asyncio.run(get_coverage_stats())
    else:
        asyncio.run(sample_projects())
