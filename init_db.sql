-- Projects table (mirror of source data for evaluation)
CREATE TABLE IF NOT EXISTS projects (
    user_project_history_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    start_date TEXT,
    end_date TEXT,
    project_position TEXT,
    industry TEXT,
    industry_id INTEGER,  -- For ES filtering
    skills TEXT,  -- JSON array of skill names
    skill_ids TEXT,  -- JSON array of skill IDs for ES filtering
    contribution TEXT
);

-- Sampled projects for evaluation
CREATE TABLE IF NOT EXISTS sampled_projects (
    user_project_history_id INTEGER PRIMARY KEY,
    industry TEXT,
    sampled_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_project_history_id) REFERENCES projects(user_project_history_id)
);

-- Synthetic queries generated from projects
CREATE TABLE IF NOT EXISTS synthetic_queries (
    query_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_project_id INTEGER,  -- NULL for real requirements
    query_text TEXT NOT NULL,
    query_type TEXT NOT NULL,  -- 'specific', 'vague', or 'real'
    industry_id INTEGER,  -- From source project for ES filtering
    skill_ids TEXT,  -- JSON array from source project for ES filtering
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_project_id) REFERENCES projects(user_project_history_id)
);

-- Evaluation runs tracking retrieval results
CREATE TABLE IF NOT EXISTS evaluation_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id INTEGER NOT NULL,
    retrieved_project_ids TEXT NOT NULL,  -- JSON array of project IDs in rank order
    ground_truth_rank INTEGER,  -- Position of source project in results (NULL if not found)
    reciprocal_rank REAL,  -- 1/rank or 0 if not found
    run_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (query_id) REFERENCES synthetic_queries(query_id)
);

-- LLM judgments for pairwise comparisons
CREATE TABLE IF NOT EXISTS judgments (
    judgment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id INTEGER NOT NULL,
    project_a_id INTEGER NOT NULL,
    project_b_id INTEGER NOT NULL,
    winner TEXT NOT NULL,  -- 'A', 'B', or 'TIE'
    reasoning TEXT,
    judge_model TEXT,
    judged_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (query_id) REFERENCES synthetic_queries(query_id)
);

-- Aggregated preference rankings from pairwise judgments
CREATE TABLE IF NOT EXISTS preference_rankings (
    ranking_id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id INTEGER NOT NULL,
    project_id INTEGER NOT NULL,
    preference_score REAL NOT NULL,  -- Win rate from pairwise comparisons
    elastic_rank INTEGER,  -- Original rank from Elastic
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (query_id) REFERENCES synthetic_queries(query_id)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_synthetic_queries_source ON synthetic_queries(source_project_id);
CREATE INDEX IF NOT EXISTS idx_evaluation_runs_query ON evaluation_runs(query_id);
CREATE INDEX IF NOT EXISTS idx_judgments_query ON judgments(query_id);
CREATE INDEX IF NOT EXISTS idx_projects_industry ON projects(industry);
CREATE INDEX IF NOT EXISTS idx_projects_industry_id ON projects(industry_id);

-- Views for analysis

-- Mean Reciprocal Rank overall
CREATE VIEW IF NOT EXISTS v_mrr_overall AS
SELECT 
    AVG(reciprocal_rank) as mrr,
    COUNT(*) as total_queries,
    SUM(CASE WHEN ground_truth_rank IS NOT NULL THEN 1 ELSE 0 END) as hit_count,
    CAST(SUM(CASE WHEN ground_truth_rank IS NOT NULL THEN 1 ELSE 0 END) AS REAL) / COUNT(*) as hit_rate
FROM evaluation_runs;

-- MRR by industry
CREATE VIEW IF NOT EXISTS v_mrr_by_industry AS
SELECT 
    p.industry,
    AVG(er.reciprocal_rank) as mrr,
    COUNT(*) as query_count,
    SUM(CASE WHEN er.ground_truth_rank IS NOT NULL THEN 1 ELSE 0 END) as hit_count,
    CAST(SUM(CASE WHEN er.ground_truth_rank IS NOT NULL THEN 1 ELSE 0 END) AS REAL) / COUNT(*) as hit_rate
FROM evaluation_runs er
JOIN synthetic_queries sq ON er.query_id = sq.query_id
JOIN projects p ON sq.source_project_id = p.user_project_history_id
GROUP BY p.industry
ORDER BY mrr DESC;

-- Hit rate at different k values
CREATE VIEW IF NOT EXISTS v_hit_rate_at_k AS
SELECT 
    SUM(CASE WHEN ground_truth_rank <= 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as hit_rate_at_1,
    SUM(CASE WHEN ground_truth_rank <= 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as hit_rate_at_3,
    SUM(CASE WHEN ground_truth_rank <= 5 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as hit_rate_at_5,
    SUM(CASE WHEN ground_truth_rank <= 10 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as hit_rate_at_10,
    SUM(CASE WHEN ground_truth_rank <= 20 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as hit_rate_at_20,
    COUNT(*) as total_queries
FROM evaluation_runs;

-- Failure cases (ground truth not in top-10)
CREATE VIEW IF NOT EXISTS v_failure_cases AS
SELECT 
    sq.query_id,
    sq.query_text,
    sq.query_type,
    p.industry,
    p.skills,
    p.contribution,
    er.ground_truth_rank
FROM evaluation_runs er
JOIN synthetic_queries sq ON er.query_id = sq.query_id
JOIN projects p ON sq.source_project_id = p.user_project_history_id
WHERE er.ground_truth_rank IS NULL OR er.ground_truth_rank > 10
ORDER BY er.ground_truth_rank DESC NULLS FIRST;

-- MRR by query type
CREATE VIEW IF NOT EXISTS v_mrr_by_query_type AS
SELECT 
    sq.query_type,
    AVG(er.reciprocal_rank) as mrr,
    COUNT(*) as query_count,
    SUM(CASE WHEN er.ground_truth_rank IS NOT NULL THEN 1 ELSE 0 END) as hit_count
FROM evaluation_runs er
JOIN synthetic_queries sq ON er.query_id = sq.query_id
GROUP BY sq.query_type;

-- Sample coverage stats
CREATE VIEW IF NOT EXISTS v_sample_coverage AS
SELECT 
    industry,
    COUNT(*) as sample_count
FROM sampled_projects sp
JOIN projects p ON sp.user_project_history_id = p.user_project_history_id
GROUP BY industry
ORDER BY sample_count DESC;
