# src/config/settings.py

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Global configuration for project paths."""

    # environment
    ENVIRONMENT: str = Field(
        default="local", description="Environment name (local/dev/val/prod)"
    )
    
    APP_NAME: str = Field(
        default="league_analytics", description="Application name"
    )
    APP_VERSION: str = Field(default="0.0.1", description="Application version")
  
   

    # Paths
    RAW_DATA_PATH: Path = Field(
        default=Path("data/raw/Books_10k.jsonl"), description="Raw input dataset"
    )
    CLEAN_DATA_PATH: Path = Field(
        default=Path("data/processed/clean_reviews.json"), description="Cleaned dataset"
    )
  
    LOG_DIR: Path = Field(default=Path("logs"), description="Directory for logs")
    



@lru_cache()
def get_settings() -> Settings:
    return Settings()
