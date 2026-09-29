"""Configuration module for Sociobot."""

import os
import logging
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Telegram Bot
    TELEGRAM_BOT_TOKEN: str = Field("dummy_token", description="Bot token from @BotFather")
    ADMIN_CHAT_ID: int = Field(0, description="Telegram User ID of Admin")

    # Stream Extractor API
    YTSP_API_BASE_URL: str = Field("https://ytsp-api.pgwiz.cloud", description="Stream Extractor API base URL")
    YTSP_API_TIMEOUT: int = Field(30, description="API HTTP timeout in seconds")
    ENABLE_API_FALLBACK: bool = Field(True, description="Fallback to local yt-dlp if API fails")

    # Database (Neon PostgreSQL or SQLite)
    DATABASE_URL: Optional[str] = Field("sqlite:///sociobot.db", description="Database connection URI")
    DATABASE_PATH: Optional[str] = Field("sociobot.db", description="SQLite path if used")
    DB_POOL_MIN_SIZE: int = Field(2, description="Min asyncpg pool connections")
    DB_POOL_MAX_SIZE: int = Field(10, description="Max asyncpg pool connections")
    ENABLE_NEON_KEEPALIVE: bool = Field(False, description="Enable Neon ping loop")

    # Downloads & Temporary File Handling
    DOWNLOAD_DIR: str = Field("./downloads", description="Local temp download directory")
    MAX_CONCURRENT_DL: int = Field(3, description="Max concurrent downloads per instance")
    AUTO_CLEANUP_TEMP: bool = Field(True, description="Auto delete local temp files after upload")

    # Cache Settings
    ENABLE_MEMORY_CACHE: bool = Field(True, description="Enable RAM cache")
    MEMORY_CACHE_MAXSIZE: int = Field(2000, description="Max items in memory cache")
    MEMORY_CACHE_TTL_SECS: int = Field(3600, description="RAM cache TTL in seconds")
    DB_METADATA_TTL_DAYS: int = Field(7, description="Metadata cache TTL in days")
    DB_SEARCH_TTL_HOURS: int = Field(24, description="Search cache TTL in hours")

    # Cloud Hosting & Health Server
    ENABLE_HEALTH_SERVER: bool = Field(True, description="Run lightweight FastAPI health/metrics server")
    API_HOST: str = Field("0.0.0.0", description="API server host")
    API_PORT: int = Field(default_factory=lambda: int(os.environ.get("PORT", "8080")), description="API server port")


# Singleton instance
settings = Settings()

# Safe directory creation with fallback
try:
    Path(settings.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.warning(f"Could not create DOWNLOAD_DIR={settings.DOWNLOAD_DIR} ({e}), falling back to ./downloads")
    settings.DOWNLOAD_DIR = "./downloads"
    Path(settings.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
