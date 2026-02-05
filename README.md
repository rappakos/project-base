
# Knowledge base for requirement bases project search

We need to find *all* matching projects for a given input requirement.

We build a knowledge base from the projects.

We transform the requirement

## Previous attempts

* simplified RAG setup: Project text information is concatenated and embedded with ada-2. Vector search was not very reliable, and the following LLM  evaluation was too slow/costly per query.
* attribute extraction: attributes are extracted from the requirements and it is matched to projects. It did not generalize as expected (continuous readjustment of data seems necessary)

## Input data 

### Projects

Most clients work in IT, consulting, or telecommunications.

There are up to 10^5 projects with following attributes:
* UserProjectHistoryId, int not null, key/id
* UserId, int not null, needed only for final aggregation ("experience")
* Duration (Start- and EndDate), nullable, needed usually only for final aggregation ("experience")
* ProjectPosition, string, nullable. May be mapped to the predefined StandardPositions
* Industry, string, nullable, from a predefined list. (E.g. automobiles, medical, transport & logistics etc)
* Skills, list of strings, from a predefined list. Includes tools, methodologies.
* Contribution, string, nullable the users activities in free text


### Requirements

Usually in the form of a Query free text input. Usually 1 sentence defining one or more of the following conditions:
* experience (often expressed in years)
* industry (rarely specific client)
* skills (tools, methods)
* scope (small team, large team, international)
etc.

#### Examples

* "3 years experience with React/Typescript development"
* "S4/Hanna migration and rollout in the transport/logistics branche"
* "Leadership experience including coordinating >5 international teams"

## Evaluation Harness

This project includes a synthetic evaluation framework to measure and improve retrieval quality without labeled data.

### Setup

1. Copy `template.env` to `.env` and configure:
   ```bash
   cp template.env .env
   ```

2. Configure Azure OpenAI credentials in `.env`:
   ```
   LLM_PROVIDER=azure
   LLM_MODEL=gpt-4.1            # your deployment name
   AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
   AZURE_OPENAI_API_KEY=your-key
   ```

3. Configure Elasticsearch (local dev or cloud):
   ```
   ELASTIC_ENV=local                   # or "cloud"
   ELASTIC_LOCAL_URL=http://localhost:9200
   # For cloud:
   # ELASTIC_CLOUD_ID=your-cloud-id
   # ELASTIC_API_KEY=your-elastic-key
   ```

4. Install dependencies and initialize database:
   ```bash
   pip install -r requirements.txt
   python db.py
   ```

### Workflow

1. **Load data** from Decidalo database:
   ```bash
   python load_data.py                 # loads projects and real requirements
   ```
   This imports:
   - All projects with their UserProjectHistoryID, skills, industry, contribution, etc.
   - Real user requirements (stored as `query_type='real'` with `source_project_id=NULL`)

2. **Sample projects** with industry stratification:
   ```bash
   python sample_projects.py           # samples 500 projects
   python sample_projects.py stats     # view coverage stats
   ```

3. **Generate synthetic queries** using LLM (uses real requirements as style examples):
   ```bash
   python generate_queries.py          # generates 2 queries per sampled project
   python generate_queries.py show 10  # preview generated queries
   python export_queries.py            # export all queries to Excel for review
   ```
   Real requirements from the database serve as examples to guide the LLM in generating realistic synthetic queries.

4. **Evaluate retrieval** against Elasticsearch:
   ```bash
   python evaluate_retrieval.py        # run evaluation
   python evaluate_retrieval.py metrics    # view MRR, hit rates
   python evaluate_retrieval.py failures 20  # inspect failure cases
   ```

5. **Run LLM-as-judge** pairwise comparisons (optional, for deeper analysis):
   ```bash
   python judge_results.py             # judge first 10 queries (cost control)
   python judge_results.py rankings    # compute preference scores
   python judge_results.py compare     # compare LLM vs Elastic ranking
   ```

### Key Metrics

- **MRR (Mean Reciprocal Rank)**: Average of 1/rank for ground-truth project
- **Hit Rate @k**: % of queries where ground-truth appears in top-k
- **Preference Score**: Win rate from pairwise LLM judgments

### Customization

Edit `elastic_client.py` to match your index schema:
- Adjust field names in `search_projects()` query
- Enable vector search if your index has embeddings


