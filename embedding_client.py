import config
from openai import OpenAI, AzureOpenAI

def get_embedding(text: str) -> list[float]:
    """Generate embedding for query text."""
    if config.LLM_PROVIDER == "azure":
        client = AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION
        )
    else:
        client = OpenAI(api_key=config.OPENAI_API_KEY)
    
    response = client.embeddings.create(
        model=config.OPENAI_EMBEDDING_MODEL,
        input=text
    )
    return response.data[0].embedding