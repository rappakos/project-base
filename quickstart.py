"""Quick start script to set up and run the entire evaluation pipeline.

This script will:
1. Drop and recreate the database
2. Load projects and requirements from Decidalo
3. Sample projects stratified by industry
4. Generate synthetic queries
5. Run evaluation against Elasticsearch
6. Display results
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime


async def run_step(name: str, command: str):
    """Run a step and report status."""
    print(f"\n{'='*60}")
    print(f"Step: {name}")
    print(f"{'='*60}")
    
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        print(f"[FAILED] Exit code {proc.returncode}")
        if stderr:
            print(f"Error: {stderr.decode('utf-8', errors='replace')}")
        return False
    
    print(stdout.decode('utf-8', errors='replace'))
    print(f"[OK] {name} completed successfully")
    return True


async def main():
    """Run the full pipeline."""
    db_path = Path("evaluation.db")
    
    # Step 0: Backup existing database
    if db_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = Path(f"evaluation_backup_{timestamp}.db")
        db_path.rename(backup_path)
        print(f"Backed up existing database to: {backup_path}")
    
    steps = [
        ("Initialize Database", "python db.py"),
        ("Load Projects & Requirements", "python load_data.py"),
        ("Sample Projects", "python sample_projects.py"),
        ("Generate Synthetic Queries", "python generate_queries.py"),
        ("Run Evaluation", "python evaluate_retrieval.py"),
    ]
    
    for name, command in steps:
        success = await run_step(name, command)
        if not success:
            print(f"\n[FAILED] Pipeline failed at step: {name}")
            sys.exit(1)
    
    # Show final metrics
    await run_step("Display Metrics", "python evaluate_retrieval.py metrics")
    
    print(f"\n{'='*60}")
    print("SUCCESS: Evaluation pipeline completed!")
    print(f"{'='*60}")
    print("\nNext steps:")
    print("  - Review metrics above")
    print("  - Run 'python evaluate_retrieval.py failures 20' to see failure cases")
    print("  - Run 'python export_queries.py' to export queries to Excel")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
