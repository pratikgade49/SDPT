"""
Application configuration module.
Handles environment variables and application settings.
"""

from pydantic_settings import BaseSettings
from pydantic import ConfigDict, field_validator
from typing import Optional

class Settings(BaseSettings):
    """Application settings from environment variables."""
    
    # Application Settings
    APP_NAME: str = "Smart Demand Planning Tool"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # Server Settings
    HOST: str = "localhost"
    PORT: int = 8000
    
    # Master Database Settings (for tenant registry)
    MASTER_DATABASE_URL: str = "postgresql://postgres:root@localhost:5433/smart_demand_master"
    
    # Database Connection Settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5433
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "root"
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    
    # Tenant Database Settings
    TENANT_DB_PREFIX: str = "tenant_"  # Prefix for tenant databases

    # JWT Settings
    JWT_SECRET_KEY: str = "supersecretkey"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_HOURS: int = 24
    
    # Environment
    ENVIRONMENT: str = "development"
    
    # Migration Settings
    MIGRATIONS_DIR: str = "migrations"
    
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra='ignore'  # Ignore extra fields from .env
    )

settings = Settings()