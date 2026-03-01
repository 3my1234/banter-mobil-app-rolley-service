from datetime import date, datetime
from enum import Enum
from pydantic import BaseModel, Field


class Sport(str, Enum):
    SOCCER = 'SOCCER'
    BASKETBALL = 'BASKETBALL'


class MatchContext(BaseModel):
    urgency_score: float = Field(ge=0, le=10)
    volatility_index: float = Field(ge=0, le=10)
    injury_impact: float = Field(ge=0, le=10)
    fatigue_level: float = Field(ge=0, le=10)
    weather_impact: float = Field(default=0, ge=0, le=10)


class MatchCandidate(BaseModel):
    external_match_id: str
    sport: Sport
    league: str
    home_team: str
    away_team: str
    kick_off_utc: datetime
    h2h_home_win_rate: float = Field(ge=0, le=1)
    h2h_draw_rate: float = Field(ge=0, le=1)
    h2h_away_win_rate: float = Field(ge=0, le=1)
    home_form_index: float = Field(ge=0, le=1)
    away_form_index: float = Field(ge=0, le=1)


class ProbabilitySet(BaseModel):
    home_win: float = Field(ge=0, le=1)
    draw: float = Field(ge=0, le=1)
    away_win: float = Field(ge=0, le=1)
    over_05: float = Field(ge=0, le=1)
    over_15: float = Field(ge=0, le=1)
    double_chance_1x: float = Field(ge=0, le=1)
    double_chance_x2: float = Field(ge=0, le=1)
    handicap_home_plus_15: float = Field(ge=0, le=1)
    handicap_away_plus_15: float = Field(ge=0, le=1)
    basketball_home_plus_85: float = Field(default=0.5, ge=0, le=1)
    basketball_away_plus_85: float = Field(default=0.5, ge=0, le=1)


class RolleyPick(BaseModel):
    id: str
    date: date
    sport: Sport
    league: str
    home_team: str
    away_team: str
    market: str
    selection: str
    confidence: float = Field(ge=0, le=1)
    rationale: str
    model_version: str
    created_at: datetime


class DailyPicksResponse(BaseModel):
    date: date
    sport: Sport
    picks: list[RolleyPick]


class RefreshResponse(BaseModel):
    success: bool
    date: date
    generated: int
