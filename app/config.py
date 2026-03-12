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
    odds_api_enabled: bool = True
    odds_api_key: str | None = None
    odds_api_base_url: str = 'https://api.odds-api.io/v3'
    odds_api_bookmakers: str = 'Bet365,Pinnacle,Unibet'
    odds_api_match_window_hours: int = 8
    odds_sanity_filter_enabled: bool = True
    odds_sanity_double_chance_max: float = 1.35
    odds_sanity_total_goals_over_05_max: float = 1.30
    odds_sanity_total_goals_over_15_max: float = 1.55
    odds_sanity_handicap_max: float = 1.50

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
    prediction_min_confidence_soccer: float | None = None
    prediction_min_confidence_basketball: float | None = None
    prediction_max_picks_per_sport: int = 3
    prediction_exclude_started_matches: bool = True
    prediction_start_buffer_minutes: int = 0
    soccer_primary_prefer_safe_markets: bool = True
    soccer_primary_allow_handicap_fallback: bool = True
    soccer_supported_handicap_lines: str = '1.5'
    daily_product_min_legs: int = 1
    daily_product_max_legs: int = 3
    daily_product_target_multiplier_min: float = 1.08
    daily_product_target_multiplier_max: float = 1.15
    daily_product_prefer_two_leg_bonus: float = 0.05
    daily_product_prefer_three_leg_bonus: float = 0.02
    soccer_daily_product_max_double_chance_legs: int = 1
    soccer_daily_product_max_same_market_legs: int = 2
    basketball_daily_product_prefer_mixed_sides: bool = True

    trusted_soccer_competitions: str = 'eng.1,esp.1,ger.1,ita.1,fra.1,uefa.champions,uefa.europa,uefa.europa.conf,tur.1'
    high_risk_soccer_competitions: str = 'vie.1,tur.2,tur.3'
    trusted_basketball_competitions: str = 'nba'
    league_risk_block_high_risk_primary: bool = True
    league_risk_confidence_penalty: float = 0.12
    league_risk_penalize_untrusted: bool = False

    xgboost_enabled: bool = True
    xgboost_model_path: str = './models/rolley_xgb_v1.json'
    xgboost_feature_names: str = (
        'h2h_home_win_rate,h2h_draw_rate,h2h_away_win_rate,home_form_index,away_form_index,'
        'urgency_score,volatility_index,injury_impact,fatigue_level,weather_impact,home_edge,'
        'h2h_sample_size,home_recent5_scored_rate,away_recent5_scored_rate,'
        'home_recent5_goal_diff,away_recent5_goal_diff,'
        'home_recent5_opponent_strength,away_recent5_opponent_strength'
    )

    movement_enabled: bool = False
    movement_network: str = 'testnet'
    movement_node_url: str = 'https://testnet.movementnetwork.xyz/v1'
    movement_explorer_base: str = 'https://explorer.movementnetwork.xyz'
    movement_rol_decimals: int = 8
    movement_private_key: str | None = None
    movement_account_address: str | None = None
    movement_token_module_address: str | None = None
    movement_settlement_module_address: str | None = None
    movement_rol_metadata_address: str | None = None
    movement_pick_metadata_base_url: str = 'https://sportbanter.online/rolley/picks'


@lru_cache
def get_settings() -> Settings:
    return Settings()
