"""Database utilities for evaluation harness."""

import aiosqlite
import json
from pathlib import Path
from typing import Optional

import config


async def get_connection() -> aiosqlite.Connection:
    """Get a connection to the SQLite database."""
    return await aiosqlite.connect(config.SQLITE_DB_PATH)


async def init_db():
    """Initialize the database with schema from init_db.sql."""
    schema_path = Path(__file__).parent / "init_db.sql"
    
    async with await get_connection() as db:
        with open(schema_path, "r") as f:
            schema = f.read()
        await db.executescript(schema)
        await db.commit()
    
    print(f"Database initialized: {config.SQLITE_DB_PATH}")


async def insert_project(
    db: aiosqlite.Connection,
    user_project_history_id: int,
    user_id: int,
    start_date: Optional[str],
    end_date: Optional[str],
    project_position: Optional[str],
    industry: Optional[str],
    skills: list[str],
    contribution: Optional[str]
):
    """Insert a project into the projects table."""
    await db.execute(
        """
        INSERT OR REPLACE INTO projects 
        (user_project_history_id, user_id, start_date, end_date, project_position, industry, skills, contribution)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_project_history_id,
            user_id,
            start_date,
            end_date,
            project_position,
            industry,
            json.dumps(skills) if skills else "[]",
            contribution
        )
    )


async def get_all_projects(db: aiosqlite.Connection) -> list[dict]:
    """Get all projects from the database."""
    async with db.execute("SELECT * FROM projects") as cursor:
        columns = [description[0] for description in cursor.description]
        rows = await cursor.fetchall()
        
    projects = []
    for row in rows:
        project = dict(zip(columns, row))
        project["skills"] = json.loads(project["skills"]) if project["skills"] else []
        projects.append(project)
    
    return projects


async def get_project_by_id(db: aiosqlite.Connection, project_id: int) -> Optional[dict]:
    """Get a project by its ID."""
    async with db.execute(
        "SELECT * FROM projects WHERE user_project_history_id = ?", 
        (project_id,)
    ) as cursor:
        row = await cursor.fetchone()
        if not row:
            return None
        columns = [description[0] for description in cursor.description]
        project = dict(zip(columns, row))
        project["skills"] = json.loads(project["skills"]) if project["skills"] else []
        return project


async def get_sampled_projects(db: aiosqlite.Connection) -> list[dict]:
    """Get all sampled projects with full details."""
    async with db.execute(
        """
        SELECT p.* FROM projects p
        JOIN sampled_projects sp ON p.user_project_history_id = sp.user_project_history_id
        """
    ) as cursor:
        columns = [description[0] for description in cursor.description]
        rows = await cursor.fetchall()
        
    projects = []
    for row in rows:
        project = dict(zip(columns, row))
        project["skills"] = json.loads(project["skills"]) if project["skills"] else []
        projects.append(project)
    
    return projects


async def insert_sampled_project(db: aiosqlite.Connection, project_id: int, industry: str):
    """Mark a project as sampled."""
    await db.execute(
        "INSERT OR IGNORE INTO sampled_projects (user_project_history_id, industry) VALUES (?, ?)",
        (project_id, industry)
    )


async def insert_synthetic_query(
    db: aiosqlite.Connection,
    source_project_id: int,
    query_text: str,
    query_type: str
) -> int:
    """Insert a synthetic query and return its ID."""
    cursor = await db.execute(
        """
        INSERT INTO synthetic_queries (source_project_id, query_text, query_type)
        VALUES (?, ?, ?)
        """,
        (source_project_id, query_text, query_type)
    )
    return cursor.lastrowid


async def get_synthetic_queries(db: aiosqlite.Connection) -> list[dict]:
    """Get all synthetic queries."""
    async with db.execute("SELECT * FROM synthetic_queries") as cursor:
        columns = [description[0] for description in cursor.description]
        rows = await cursor.fetchall()
    
    return [dict(zip(columns, row)) for row in rows]


async def get_unevaluated_queries(db: aiosqlite.Connection) -> list[dict]:
    """Get synthetic queries that haven't been evaluated yet."""
    async with db.execute(
        """
        SELECT sq.* FROM synthetic_queries sq
        LEFT JOIN evaluation_runs er ON sq.query_id = er.query_id
        WHERE er.run_id IS NULL
        """
    ) as cursor:
        columns = [description[0] for description in cursor.description]
        rows = await cursor.fetchall()
    
    return [dict(zip(columns, row)) for row in rows]


async def insert_evaluation_run(
    db: aiosqlite.Connection,
    query_id: int,
    retrieved_project_ids: list[int],
    ground_truth_rank: Optional[int],
    reciprocal_rank: float
) -> int:
    """Insert an evaluation run result."""
    cursor = await db.execute(
        """
        INSERT INTO evaluation_runs (query_id, retrieved_project_ids, ground_truth_rank, reciprocal_rank)
        VALUES (?, ?, ?, ?)
        """,
        (
            query_id,
            json.dumps(retrieved_project_ids),
            ground_truth_rank,
            reciprocal_rank
        )
    )
    return cursor.lastrowid


async def insert_judgment(
    db: aiosqlite.Connection,
    query_id: int,
    project_a_id: int,
    project_b_id: int,
    winner: str,
    reasoning: str,
    judge_model: str
) -> int:
    """Insert a pairwise judgment."""
    cursor = await db.execute(
        """
        INSERT INTO judgments (query_id, project_a_id, project_b_id, winner, reasoning, judge_model)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (query_id, project_a_id, project_b_id, winner, reasoning, judge_model)
    )
    return cursor.lastrowid


async def get_unjudged_queries(db: aiosqlite.Connection) -> list[dict]:
    """Get queries with evaluation runs but no judgments yet."""
    async with db.execute(
        """
        SELECT DISTINCT sq.*, er.retrieved_project_ids
        FROM synthetic_queries sq
        JOIN evaluation_runs er ON sq.query_id = er.query_id
        LEFT JOIN judgments j ON sq.query_id = j.query_id
        WHERE j.judgment_id IS NULL
        """
    ) as cursor:
        columns = [description[0] for description in cursor.description]
        rows = await cursor.fetchall()
    
    results = []
    for row in rows:
        result = dict(zip(columns, row))
        result["retrieved_project_ids"] = json.loads(result["retrieved_project_ids"])
        results.append(result)
    
    return results


if __name__ == "__main__":
    import asyncio
    asyncio.run(init_db())
