from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', case_sensitive=False)

    service_name: str = 'Rolley Service'
    service_version: str = '0.1.0'
    environment: str = 'development'

    api_prefix: str = '/api/v1'
    database_url: str = 'sqlite:///./rolley.db'

    cors_origins: str = '*'

    gemini_api_key: str | None = None
    gemini_model: str = 'gemini-2.5-flash'

    sports_api_key: str | None = None

    admin_refresh_key: str | None = None

    cron_enabled: bool = True
    cron_hour_utc: int = 8
    cron_minute_utc: int = 0

    default_pick_count: int = 10


@lru_cache
def get_settings() -> Settings:
    return Settings()
