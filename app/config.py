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
    soccer_competitions: str = (
        'eng.1,esp.1,ger.1,ita.1,fra.1,uefa.champions,uefa.europa,uefa.europa.conf,'
        'ned.1,por.1,bel.1,sco.1,sui.1,aut.1,swe.1,nor.1,den.1,usa.1,mex.1,bra.1,arg.1,'
        'aus.1,jpn.1,chn.1,ksa.1'
    )
    basketball_competitions: str = 'nba'
    same_day_only: bool = True
    soccer_event_timezone: str = 'UTC'
    basketball_event_timezone: str = 'America/New_York'
    sports_fallback_to_stub: bool = False
    api_football_enabled: bool = False
    api_football_key: str | None = None
    api_football_host: str = 'v3.football.api-sports.io'
    football_data_enabled: bool = False
    football_data_key: str | None = None

    admin_refresh_key: str | None = None

    cron_enabled: bool = True
    cron_hour_utc: int = 8
    cron_minute_utc: int = 0
    auto_settlement_enabled: bool = True
    auto_settlement_hour_utc: int = 2
    auto_settlement_minute_utc: int = 30
    auto_settlement_offset_days: int = 1

    default_pick_count: int = 10
    primary_pick_count: int = 1
    primary_min_completeness: float = 0.65
    prediction_min_confidence: float = 0.90
    prediction_max_picks_per_sport: int = 3
    prediction_exclude_started_matches: bool = True
    prediction_start_buffer_minutes: int = 0

    xgboost_enabled: bool = True
    xgboost_model_path: str = './models/rolley_xgb_v1.json'
    xgboost_feature_names: str = (
        'h2h_home_win_rate,h2h_draw_rate,h2h_away_win_rate,home_form_index,away_form_index,'
        'urgency_score,volatility_index,injury_impact,fatigue_level,weather_impact,home_edge'
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
