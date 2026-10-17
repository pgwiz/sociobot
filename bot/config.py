"""Configuration module for Sociobot."""

import os
import logging
from pathlib import Path
from typing import Optional, List
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator

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
    SUPER_ADMIN_IDS: str = Field("", description="Comma-separated Telegram user IDs of Super Admins")

    @model_validator(mode="before")
    @classmethod
    def resolve_config(cls, values: dict):
        if isinstance(values, dict):
            # Sanitize and strip token
            tok = values.get("TELEGRAM_BOT_TOKEN")
            if tok and isinstance(tok, str):
                values["TELEGRAM_BOT_TOKEN"] = tok.strip().strip('"\'')
            # Fallback to MONGO_URL if DATABASE_URL is not set or defaults to sqlite and MONGO_URL is provided
            db_url = values.get("DATABASE_URL")
            mongo_url = values.get("MONGO_URL")
            if mongo_url and isinstance(mongo_url, str):
                values["MONGO_URL"] = mongo_url.strip().strip('"\'')
                if not db_url or db_url == "sqlite:///sociobot.db" or not str(db_url).strip():
                    values["DATABASE_URL"] = values["MONGO_URL"]
            if values.get("DATABASE_URL") and isinstance(values.get("DATABASE_URL"), str):
                values["DATABASE_URL"] = values["DATABASE_URL"].strip().strip('"\'')
            if values.get("MONGO_DB_NAME") and isinstance(values.get("MONGO_DB_NAME"), str):
                values["MONGO_DB_NAME"] = values["MONGO_DB_NAME"].strip().strip('"\'')
            if values.get("DB_SCHEMA") and isinstance(values.get("DB_SCHEMA"), str):
                values["DB_SCHEMA"] = values["DB_SCHEMA"].strip().strip('"\'')
        return values

    # Stream Extractor API
    YTSP_API_BASE_URL: str = Field("https://ytsp-api.pgwiz.cloud", description="Stream Extractor API base URL")
    YTSP_API_TIMEOUT: int = Field(30, description="API HTTP timeout in seconds")
    ENABLE_API_FALLBACK: bool = Field(True, description="Fallback to local yt-dlp if API fails")

    # Database (MongoDB Atlas, Neon PostgreSQL, or SQLite)
    DATABASE_URL: Optional[str] = Field("sqlite:///sociobot.db", description="Database connection URI (MongoDB, PostgreSQL, or SQLite)")
    MONGO_URL: Optional[str] = Field(None, description="MongoDB connection URI alias")
    MONGO_DB_NAME: Optional[str] = Field(None, description="MongoDB database name override")
    MONGO_TLS_INSECURE: bool = Field(False, description="Allow invalid TLS certificates for MongoDB (for troubleshooting/proxies)")
    DATABASE_PATH: Optional[str] = Field("sociobot.db", description="SQLite path if used")
    DB_SCHEMA: str = Field("sociobot", description="PostgreSQL custom schema name for table isolation")
    DB_POOL_MIN_SIZE: int = Field(2, description="Min database pool connections")
    DB_POOL_MAX_SIZE: int = Field(10, description="Max database pool connections")
    ENABLE_NEON_KEEPALIVE: bool = Field(False, description="Enable Neon ping loop")
    DEFAULT_MAX_CHANNELS: int = Field(5, description="Default channel quota per user")

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

    def get_super_admin_ids(self) -> List[int]:
        """Return list of numeric user IDs authorized as Super Admins."""
        ids = set()
        if self.ADMIN_CHAT_ID:
            ids.add(self.ADMIN_CHAT_ID)
        if self.SUPER_ADMIN_IDS:
            for item in self.SUPER_ADMIN_IDS.split(","):
                clean = item.strip()
                if clean.isdigit():
                    ids.add(int(clean))
        return list(ids)


# Singleton instance
settings = Settings()

# Safe directory creation with fallback
try:
    Path(settings.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.warning(f"Could not create DOWNLOAD_DIR={settings.DOWNLOAD_DIR} ({e}), falling back to ./downloads")
    settings.DOWNLOAD_DIR = "./downloads"
    Path(settings.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
