"""Typed runtime configuration for the capstone platform."""

from __future__ import annotations

import logging
from enum import StrEnum
from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    """Deployment environments supported by the platform."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class Settings(BaseSettings):
    """Validated application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CAPSTONE_",
        # The Compose .env also contains Airflow bootstrap secrets. They are not
        # application configuration and must not be exposed to this settings model.
        extra="ignore",
        case_sensitive=False,
    )

    environment: Environment = Environment.DEVELOPMENT
    log_level: str = Field(default="INFO", min_length=1, max_length=20)
    service_name: str = Field(default="modern-data-engineering-ai", min_length=3, max_length=100)
    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    kafka_raw_topic: str = Field(default="raw.events", min_length=1)
    kafka_validated_topic: str = Field(default="validated.events", min_length=1)
    kafka_quarantine_topic: str = Field(default="quarantine.events", min_length=1)
    kafka_consumer_group: str = Field(default="validation-consumer", min_length=1)
    delta_base_path: str = Field(default="file:///opt/capstone/data/lakehouse", min_length=1)
    opensearch_url: str = Field(default="http://localhost:9200", min_length=1)
    opensearch_index: str = Field(default="knowledge-chunks", min_length=1)
    embedding_model: str = Field(default="sentence-transformers/all-MiniLM-L6-v2", min_length=1)
    reranker_model: str = Field(default="cross-encoder/ms-marco-MiniLM-L-6-v2", min_length=1)
    llm_base_url: str = Field(default="http://localhost:11434", min_length=1)
    llm_api_key: SecretStr | None = None
    llm_model: str = Field(default="qwen2.5:0.5b", min_length=1)
    openlineage_url: str = Field(default="http://localhost:5000", min_length=1)
    openlineage_namespace: str = Field(default="modern-data-engineering-ai", min_length=1)

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """Normalize and validate a standard Python logging level."""
        normalized_value = value.upper()
        if normalized_value not in logging.getLevelNamesMapping():
            message = f"Unsupported logging level: {value}"
            raise ValueError(message)
        return normalized_value

    @field_validator("llm_base_url", mode="before")
    @classmethod
    def default_empty_llm_url(cls, value: object) -> str:
        """Use the local Ollama endpoint when a Compose variable is explicitly empty."""
        if value is None or not str(value).strip():
            return "http://localhost:11434"
        return str(value)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return one validated, process-wide settings instance."""
    return Settings()
