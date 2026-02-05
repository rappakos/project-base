"""Elasticsearch client factory and search utilities."""

from elasticsearch import Elasticsearch
from typing import Optional

import config


def get_client() -> Elasticsearch:
    """Get Elasticsearch client based on environment configuration."""
    if config.ELASTIC_ENV == "local":
        #return Elasticsearch(hosts=[config.ELASTIC_LOCAL_URL], verify_certs=False,ssl_show_warn=False,scheme="http")
        return Elasticsearch(config.ELASTIC_LOCAL_URL)
    elif config.ELASTIC_ENV == "cloud":
        if not config.ELASTIC_CLOUD_ID or not config.ELASTIC_API_KEY:
            raise ValueError("ELASTIC_CLOUD_ID and ELASTIC_API_KEY required for cloud environment")
        return Elasticsearch(
            cloud_id=config.ELASTIC_CLOUD_ID,
            api_key=config.ELASTIC_API_KEY
        )
    else:
        raise ValueError(f"Unknown ELASTIC_ENV: {config.ELASTIC_ENV}")


def search_projects(
    query_text: str,
    industry_id: int = None,
    skill_ids: list[int] = None,
    top_k: int = 20,
    client: Optional[Elasticsearch] = None
) -> list[dict]:
    """
    Search projects in Elasticsearch with optional filtering.
    
    Args:
        query_text: The search query
        industry_id: Filter by industry ID
        skill_ids: Boost by skill IDs
        top_k: Number of results to return
        client: ES client instance
        
    Returns list of dicts with 'id' and 'score' keys.
    """
    if client is None:
        client = get_client()
    
    # Build query
    query = {
        "size": top_k,
        "query": {
            "bool": {
                "must": [],
                "filter": [],
                "should": []
            }
        }
    }
    
    # Add text search if enabled
    if config.USE_TEXT_SEARCH:
        query["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query_text,
                "fields": [
                    "text^3",
                    "metadata.referenceName",
                    "metadata.standardPositions"
                ],
                "type": "best_fields"
            }
        })
    else:
        # If no text search, use match_all to get all docs (filtered by industry/skills)
        query["query"]["bool"]["must"].append({"match_all": {}})
    
    # Add industry filter if provided
    if industry_id is not None:
        query["query"]["bool"]["filter"].append({
            "term": {"metadata.industryID": industry_id}
        })
    
    # Add skill boosting if provided
    if skill_ids:
        query["query"]["bool"]["should"].append({
            "terms": {
                "metadata.skillIDs": skill_ids,
                "boost": 2.0
            }
        })
        # Make should clause optional but boost matching docs
        query["query"]["bool"]["minimum_should_match"] = 0
    
    # If your index has a vector field, add kNN search
    # Uncomment and adjust as needed:
    # query["knn"] = {
    #     "field": "contribution_embedding",
    #     "query_vector": get_embedding(query_text),  # You'd need to implement this
    #     "k": top_k,
    #     "num_candidates": top_k * 2
    # }
    
    response = client.search(index=config.ELASTIC_INDEX, body=query)
    
    results = []
    for hit in response["hits"]["hits"]:
        # Extract ID from metadata.userProjectHistoryID
        project_id = hit["_source"].get("metadata", {}).get("userProjectHistoryID")
        if project_id:
            results.append({
                "id": int(project_id),
                "score": hit["_score"],
                "source": hit["_source"]
            })
    
    return results


def get_project_by_id(project_id: int, client: Optional[Elasticsearch] = None) -> Optional[dict]:
    """Get a project document by ID from Elasticsearch."""
    if client is None:
        client = get_client()
    
    try:
        response = client.get(index=config.ELASTIC_INDEX, id=str(project_id))
        return response["_source"]
    except Exception:
        return None


def check_connection() -> bool:
    """Check if Elasticsearch is reachable."""
    try:
        client = get_client()
        result = client.ping()
        #print(f"Ping returned: {result} (type: {type(result)})")
        return result is True or result == True
    except Exception as e:
        print(f"Elasticsearch connection failed: {e}")
        return False


if __name__ == "__main__":
    # Test connection
    if check_connection():
        print(f"Successfully connected to Elasticsearch ({config.ELASTIC_ENV})")
        
        # Test search
        results = search_projects("React development", top_k=5)
        print(f"Found {len(results)} results")
        for r in results:
            print(f"  ID: {r['id']}, Score: {r['score']:.2f}")
    else:
        print("Failed to connect to Elasticsearch")
