"""Configuration loader for evaluation harness."""

import os
from dotenv import load_dotenv

load_dotenv()

# LLM Configuration (Azure OpenAI)
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "azure")  # "azure" or "openai"
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o")  # deployment name for Azure

# Azure OpenAI settings
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")  # e.g. https://xxx.openai.azure.com/
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")

# OpenAI settings (fallback)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

# Elastic Configuration
ELASTIC_ENV = os.getenv("ELASTIC_ENV", "local")  # "local" or "cloud"
ELASTIC_LOCAL_URL = os.getenv("ELASTIC_LOCAL_URL", "http://localhost:9200")
ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "projects")

# Database Configuration
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "evaluation.db")

# Sampling Configuration
SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "500"))
MIN_PER_INDUSTRY = int(os.getenv("MIN_PER_INDUSTRY", "5"))

# Evaluation Configuration
TOP_K_RETRIEVAL = int(os.getenv("TOP_K_RETRIEVAL", "20"))
TOP_K_JUDGE = int(os.getenv("TOP_K_JUDGE", "10"))
