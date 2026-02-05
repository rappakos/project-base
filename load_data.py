"""
Load Decidalo projects and requirements into the evaluation database.
"""
import os
import sys
import asyncio
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Add path to import Analyser
decidalo_scripts_path = Path(__file__).resolve().parents[1] / "decidalo-scripts" / "decidalo_v3" / "Utilities" / "rfp-search-requirements" / "rfp-project-search"
sys.path.insert(0, str(decidalo_scripts_path))

from project_data_analysis import Analyser
import db
import config
import aiosqlite


async def load_projects(analyser: Analyser):
    """Load projects from Decidalo database into evaluation database."""
    print("Fetching projects from Decidalo...")
    df_projects = await analyser.get_projects()
    print(f"Found {len(df_projects)} projects")
    
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        loaded_count = 0
        skipped_count = 0
        
        for idx, row in df_projects.iterrows():
            # Convert skills from comma-separated string to list
            skills_str = row.get('Skills', '')
            if pd.isna(skills_str) or not skills_str:
                skills_list = []
            else:
                skills_list = [s.strip() for s in str(skills_str).split(',') if s.strip()]
            
            # Handle nullable dates - convert to ISO string or None
            start_date = None
            end_date = None
            if pd.notna(row.get('StartDate')):
                try:
                    start_date = pd.to_datetime(row['StartDate']).strftime('%Y-%m-%d')
                except (pd.errors.OutOfBoundsDatetime, OverflowError):
                    pass  # Skip invalid dates
            if pd.notna(row.get('EndDate')):
                try:
                    end_date = pd.to_datetime(row['EndDate']).strftime('%Y-%m-%d')
                except (pd.errors.OutOfBoundsDatetime, OverflowError):
                    pass  # Skip invalid dates
            
            try:
                await db.insert_project(
                    conn,
                    user_project_history_id=int(row['UserProjectHistoryID']),
                    user_id=int(row['UserID']),
                    start_date=start_date,
                    end_date=end_date,
                    project_position=row.get('ProjectPosition') if pd.notna(row.get('ProjectPosition')) else None,
                    industry=row.get('IndustryName') if pd.notna(row.get('IndustryName')) else None,
                    skills=skills_list,
                    contribution=row.get('Contribution') if pd.notna(row.get('Contribution')) else None
                )
                loaded_count += 1
                
                # Commit in batches
                if loaded_count % 1000 == 0:
                    await conn.commit()
                    print(f"  Loaded {loaded_count} projects...")
                    
            except Exception as e:
                skipped_count += 1
                if skipped_count <= 5:  # Show first few errors
                    print(f"  Warning: Skipped project {row['UserProjectHistoryID']}: {e}")
        
        await conn.commit()
        print(f"✓ Completed loading {loaded_count} projects ({skipped_count} skipped)")
        
        # Show industry distribution
        print("\nIndustry distribution:")
        cursor = await conn.execute("""
            SELECT industry, COUNT(*) as count 
            FROM projects 
            WHERE industry IS NOT NULL 
            GROUP BY industry 
            ORDER BY count DESC 
            LIMIT 10
        """)
        rows = await cursor.fetchall()
        for industry, count in rows:
            print(f"  {industry}: {count}")
    finally:
        await conn.close()


async def load_requirements(analyser: Analyser):
    """
    Load real requirements as style examples for synthetic query generation.
    These have no source_project_id as they're actual user searches.
    """
    print("\nFetching requirements from Decidalo...")
    df_requirements = await analyser.get_requirements()
    print(f"Found {len(df_requirements)} requirements")
    
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        loaded_count = 0
        
        for idx, row in df_requirements.iterrows():
            query_text = row.get('QueryText', '').strip()
            if not query_text:
                continue
            
            # Insert as 'real' query type with no source project
            await conn.execute(
                """
                INSERT INTO synthetic_queries 
                (source_project_id, query_text, query_type, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    None,  # No source project for real requirements
                    query_text,
                    'real',  # Distinguish from synthetic queries
                    datetime.utcnow().isoformat()
                )
            )
            loaded_count += 1
            
            if loaded_count % 100 == 0:
                await conn.commit()
                print(f"  Loaded {loaded_count} requirements...")
        
        await conn.commit()
        print(f"✓ Completed loading {loaded_count} requirements")
        
        # Show sample queries
        print("\nSample requirements:")
        cursor = await conn.execute("""
            SELECT query_text 
            FROM synthetic_queries 
            WHERE query_type = 'real' 
            LIMIT 5
        """)
        rows = await cursor.fetchall()
        for i, (query_text,) in enumerate(rows, 1):
            print(f"  {i}. {query_text}")
    finally:
        await conn.close()


async def main():
    """Main loader execution."""
    print("=" * 60)
    print("Decidalo Data Loader for Project Evaluation")
    print("=" * 60)
    
    # Initialize database
    print("\nInitializing database...")
    await db.init_db()
    print("✓ Database initialized")
    
    # Create analyser instance
    analyser = Analyser()
    
    # Load data
    await load_projects(analyser)
    await load_requirements(analyser)
    
    # Final summary
    conn = await aiosqlite.connect(config.SQLITE_DB_PATH)
    try:
        cursor = await conn.execute("SELECT COUNT(*) FROM projects")
        project_count = (await cursor.fetchone())[0]
        
        cursor = await conn.execute("SELECT COUNT(*) FROM synthetic_queries WHERE query_type = 'real'")
        requirement_count = (await cursor.fetchone())[0]
    finally:
        await conn.close()
    
    print("\n" + "=" * 60)
    print(f"✓ Load complete: {project_count} projects, {requirement_count} requirements")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. python sample_projects.py          # Sample projects by industry")
    print("  2. python generate_queries.py         # Generate synthetic queries")
    print("  3. python evaluate_retrieval.py       # Evaluate search quality")


if __name__ == '__main__':
    print("Starting load_data.py...")
    try:
        load_dotenv()
        print("Environment loaded")
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()