from datetime import date, datetime
from enum import Enum
from pydantic import BaseModel, Field


class Sport(str, Enum):
    SOCCER = 'SOCCER'
    BASKETBALL = 'BASKETBALL'


class SettlementOutcome(str, Enum):
    PENDING = 'PENDING'
    WIN = 'WIN'
    LOSS = 'LOSS'
    VOID = 'VOID'


class StakeStatus(str, Enum):
    ACTIVE = 'ACTIVE'
    LOST = 'LOST'
    MATURED = 'MATURED'
    WITHDRAWN = 'WITHDRAWN'


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
    home_team_id: str | None = None
    away_team_id: str | None = None
    kick_off_utc: datetime
    h2h_home_win_rate: float = Field(ge=0, le=1)
    h2h_draw_rate: float = Field(ge=0, le=1)
    h2h_away_win_rate: float = Field(ge=0, le=1)
    home_form_index: float = Field(ge=0, le=1)
    away_form_index: float = Field(ge=0, le=1)
    home_table_position: int | None = None
    away_table_position: int | None = None
    home_points: int | None = None
    away_points: int | None = None
    home_injuries: int = 0
    away_injuries: int = 0
    data_completeness: float = Field(default=1.0, ge=0, le=1)
    confidence_penalty: float = Field(default=0.0, ge=0, le=1)
    data_sources: list[str] = Field(default_factory=list)


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


class PickSettlementPayload(BaseModel):
    outcome: SettlementOutcome
    notes: str | None = None
    settled_by: str | None = None


class RolleyPick(BaseModel):
    id: str
    external_match_id: str
    date: date
    sport: Sport
    league: str
    home_team: str
    away_team: str
    kick_off_utc: datetime
    market: str
    selection: str
    confidence: float = Field(ge=0, le=1)
    implied_odds: float = Field(ge=1)
    rationale: str
    model_version: str
    is_primary: bool = False
    settlement_outcome: SettlementOutcome = SettlementOutcome.PENDING
    settlement_notes: str | None = None
    settled_at: datetime | None = None
    created_at: datetime


class DailyPicksResponse(BaseModel):
    date: date
    sport: Sport
    primary_pick: RolleyPick | None = None
    alternatives: list[RolleyPick] = []
    picks: list[RolleyPick] = []


class RefreshResponse(BaseModel):
    success: bool
    date: date
    generated: int


class StakeCreateRequest(BaseModel):
    user_id: str = Field(min_length=2, max_length=120)
    sport: Sport
    amount_rol: float = Field(gt=0)
    lock_days: int = Field(ge=30, le=365)


class StakePositionView(BaseModel):
    id: str
    user_id: str
    sport: Sport
    principal_rol: float
    current_rol: float
    lock_days: int
    starts_on: date
    ends_on: date
    status: StakeStatus
    total_factor: float
    gross_profit_rol: float
    platform_fee_rol: float
    net_payout_rol: float
    matured_at: datetime | None = None
    withdrawn_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class StakeCreateResponse(BaseModel):
    success: bool
    stake: StakePositionView


class StakeListResponse(BaseModel):
    user_id: str
    stakes: list[StakePositionView]


class StakeWithdrawResponse(BaseModel):
    success: bool
    stake: StakePositionView
