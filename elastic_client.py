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
    top_k: int = 20,
    client: Optional[Elasticsearch] = None
) -> list[dict]:
    """
    Search projects in Elasticsearch.
    
    Returns list of dicts with 'id' and 'score' keys.
    
    This is a placeholder implementation - adjust the query structure
    to match your actual Elastic index schema and search requirements.
    """
    if client is None:
        client = get_client()
    
    # BM25 text search on main content and metadata fields
    query = {
        "size": top_k,
        "query": {
            "bool": {
                "should": [
                    # Primary search on text content
                    {
                        "multi_match": {
                            "query": query_text,
                            "fields": [
                                "text^3",  # Main project description
                                "standardPositions^2",  # Job positions
                                "metadata.displayName",  # Display name
                                "metadata.referenceName",  # Reference name
                                "metadata.standardPositions"  # Standard positions in metadata
                            ],
                            "type": "best_fields"
                        }
                    }
                ]
            }
        }
    }
    
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
        print(f"Ping returned: {result} (type: {type(result)})")
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
