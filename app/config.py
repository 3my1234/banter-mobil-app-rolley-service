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
    sports_provider: str = 'ESPN'  # ESPN or STUB
    soccer_competitions: str = 'eng.1,esp.1,ger.1,ita.1,fra.1'
    basketball_competitions: str = 'nba'
    same_day_only: bool = True

    admin_refresh_key: str | None = None

    cron_enabled: bool = True
    cron_hour_utc: int = 8
    cron_minute_utc: int = 0

    default_pick_count: int = 10
    primary_pick_count: int = 1

    xgboost_enabled: bool = True
    xgboost_model_path: str = './models/rolley_xgb_v1.json'
    xgboost_feature_names: str = (
        'h2h_home_win_rate,h2h_draw_rate,h2h_away_win_rate,home_form_index,away_form_index,'
        'urgency_score,volatility_index,injury_impact,fatigue_level,weather_impact,home_edge'
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
