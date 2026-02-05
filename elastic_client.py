"""Elasticsearch client factory and search utilities."""

from elasticsearch import Elasticsearch
from typing import Optional

import config
import embedding_client


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


def _search_text(
    query_text: str,
    industry_id: int = None,
    skill_ids: list[int] = None,
    top_k: int = 20,
    client: Elasticsearch = None
) -> list[dict]:
    """Text-based search with filtering."""
    query = {
        "size": top_k,
        "query": {
            "bool": {
                "must": [{
                    "multi_match": {
                        "query": query_text,
                        "fields": [
                            "text^3",
                            "metadata.referenceName",
                            "metadata.standardPositions"
                        ],
                        "type": "best_fields"
                    }
                }],
                "filter": [],
                "should": []
            }
        }
    }
    
    # Add industry filter
    if industry_id is not None:
        query["query"]["bool"]["filter"].append({
            "term": {"metadata.industryID": industry_id}
        })
    
    # Add skill boosting
    if skill_ids:
        query["query"]["bool"]["should"].append({
            "terms": {
                "metadata.skillIDs": skill_ids,
                "boost": 2.0
            }
        })
        query["query"]["bool"]["minimum_should_match"] = 0
    
    response = client.search(index=config.ELASTIC_INDEX, body=query)
    
    results = []
    for hit in response["hits"]["hits"]:
        project_id = hit["_source"].get("metadata", {}).get("userProjectHistoryID")
        if project_id:
            results.append({
                "id": int(project_id),
                "score": hit["_score"],
                "source": hit["_source"]
            })
    
    return results


def _search_vector(
    query_text: str,
    industry_id: int = None,
    skill_ids: list[int] = None,
    top_k: int = 20,
    client: Elasticsearch = None
) -> list[dict]:
    """Vector-based search using script_score with dot_product."""
    # Get query embedding
    query_vector = embedding_client.get_embedding(query_text)
    
    # Build base query with filters
    query = {
        "size": top_k,
        "query": {
            "script_score": {
                "query": {
                    "bool": {
                        "filter": [],
                        "should": []
                    }
                },
                "script": {
                    "source": "dotProduct(params.query_vector, doc[params.vector_field]) + 1.0",
                    "params": {
                        "query_vector": query_vector,
                        "vector_field": config.VECTOR_FIELD
                    }
                }
            }
        }
    }
    
    # Add industry filter
    if industry_id is not None:
        query["query"]["script_score"]["query"]["bool"]["filter"].append({
            "term": {"metadata.industryID": industry_id}
        })
    
    # Add skill boosting (as filter to include in candidates)
    if skill_ids:
        query["query"]["script_score"]["query"]["bool"]["should"].append({
            "terms": {
                "metadata.skillIDs": skill_ids
            }
        })
        query["query"]["script_score"]["query"]["bool"]["minimum_should_match"] = 0
    
    # If no filters, need at least match_all
    if not query["query"]["script_score"]["query"]["bool"]["filter"] and not query["query"]["script_score"]["query"]["bool"]["should"]:
        query["query"]["script_score"]["query"] = {"match_all": {}}
    
    response = client.search(index=config.ELASTIC_INDEX, body=query)
    
    results = []
    for hit in response["hits"]["hits"]:
        project_id = hit["_source"].get("metadata", {}).get("userProjectHistoryID")
        if project_id:
            results.append({
                "id": int(project_id),
                "score": hit["_score"],
                "source": hit["_source"]
            })
    
    return results


def _combine_rrf(
    text_results: list[dict],
    vector_results: list[dict],
    k: int = 60,
    top_k: int = 20
) -> list[dict]:
    """
    Combine text and vector results using Reciprocal Rank Fusion (RRF).
    
    Args:
        text_results: Results from text search
        vector_results: Results from vector search
        k: RRF constant (typically 60)
        top_k: Number of final results to return
        
    Returns merged and re-ranked results.
    """
    # Build rank maps
    text_ranks = {r["id"]: rank + 1 for rank, r in enumerate(text_results)}
    vector_ranks = {r["id"]: rank + 1 for rank, r in enumerate(vector_results)}
    
    # Collect all unique IDs and their sources
    all_ids = set(text_ranks.keys()) | set(vector_ranks.keys())
    id_to_source = {}
    for r in text_results:
        id_to_source[r["id"]] = r.get("source")
    for r in vector_results:
        if r["id"] not in id_to_source:
            id_to_source[r["id"]] = r.get("source")
    
    # Calculate RRF scores
    rrf_scores = {}
    for doc_id in all_ids:
        score = 0.0
        if doc_id in text_ranks:
            score += 1.0 / (k + text_ranks[doc_id])
        if doc_id in vector_ranks:
            score += 1.0 / (k + vector_ranks[doc_id])
        rrf_scores[doc_id] = score
    
    # Sort by RRF score and return top_k
    sorted_ids = sorted(rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True)
    
    results = []
    for doc_id in sorted_ids[:top_k]:
        results.append({
            "id": doc_id,
            "score": rrf_scores[doc_id],
            "source": id_to_source.get(doc_id)
        })
    
    return results


def search_projects(
    query_text: str,
    industry_id: int = None,
    skill_ids: list[int] = None,
    top_k: int = 20,
    client: Optional[Elasticsearch] = None
) -> list[dict]:
    """
    Search projects in Elasticsearch with optional filtering.
    Supports text, vector, or hybrid (RRF) search based on config.
    
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
    
    use_text = config.USE_TEXT_SEARCH
    use_vector = config.USE_VECTOR_SEARCH
    
    # Hybrid search with RRF
    if use_text and use_vector:
        text_results = _search_text(query_text, industry_id, skill_ids, top_k * 2, client)
        vector_results = _search_vector(query_text, industry_id, skill_ids, top_k * 2, client)
        return _combine_rrf(text_results, vector_results, k=60, top_k=top_k)
    
    # Text-only search
    elif use_text:
        return _search_text(query_text, industry_id, skill_ids, top_k, client)
    
    # Vector-only search
    elif use_vector:
        return _search_vector(query_text, industry_id, skill_ids, top_k, client)
    
    # Fallback: match_all with filters only
    else:
        query = {
            "size": top_k,
            "query": {
                "bool": {
                    "must": [{"match_all": {}}],
                    "filter": [],
                    "should": []
                }
            }
        }
        
        if industry_id is not None:
            query["query"]["bool"]["filter"].append({
                "term": {"metadata.industryID": industry_id}
            })
        
        if skill_ids:
            query["query"]["bool"]["should"].append({
                "terms": {
                    "metadata.skillIDs": skill_ids,
                    "boost": 2.0
                }
            })
            query["query"]["bool"]["minimum_should_match"] = 0
        
        response = client.search(index=config.ELASTIC_INDEX, body=query)
        
        results = []
        for hit in response["hits"]["hits"]:
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
