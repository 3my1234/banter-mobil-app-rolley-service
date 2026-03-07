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
    competition_code: str | None = None
    league: str
    home_team: str
    away_team: str
    home_team_id: str | None = None
    away_team_id: str | None = None
    kick_off_utc: datetime
    h2h_home_win_rate: float = Field(ge=0, le=1)
    h2h_draw_rate: float = Field(ge=0, le=1)
    h2h_away_win_rate: float = Field(ge=0, le=1)
    h2h_sample_size: int = 0
    home_form_index: float = Field(ge=0, le=1)
    away_form_index: float = Field(ge=0, le=1)
    home_recent5_scored_rate: float = Field(default=0.5, ge=0, le=1)
    away_recent5_scored_rate: float = Field(default=0.5, ge=0, le=1)
    home_recent5_goal_diff: float = Field(default=0.0, ge=-10, le=10)
    away_recent5_goal_diff: float = Field(default=0.0, ge=-10, le=10)
    home_recent5_opponent_strength: float = Field(default=0.5, ge=0, le=1)
    away_recent5_opponent_strength: float = Field(default=0.5, ge=0, le=1)
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
    movement_pick_id: int | None = None
    movement_tx_hash: str | None = None
    movement_sync_status: str | None = None
    settlement_outcome: SettlementOutcome = SettlementOutcome.PENDING
    settlement_notes: str | None = None
    settled_by: str | None = None
    settled_at: datetime | None = None
    settlement_movement_tx_hash: str | None = None
    created_at: datetime


class DailyPicksResponse(BaseModel):
    date: date
    sport: Sport
    primary_pick: RolleyPick | None = None
    alternatives: list[RolleyPick] = Field(default_factory=list)
    picks: list[RolleyPick] = Field(default_factory=list)


class PickHistoryResponse(BaseModel):
    sport: Sport | None = None
    before_date: date | None = None
    pick_date: date | None = None
    picks: list[RolleyPick] = Field(default_factory=list)


class MovementWalletPickStatus(BaseModel):
    movement_pick_id: int
    wallet_address: str
    pick_status: str
    staked_raw: str
    staked_rol: float
    claimable_raw: str
    claimable_rol: float
    eligible_to_claim: bool


class MovementWalletStatusResponse(BaseModel):
    wallet_address: str
    statuses: list[MovementWalletPickStatus] = Field(default_factory=list)


class RefreshResponse(BaseModel):
    success: bool
    date: date
    generated: int


class AutoSettlementResponse(BaseModel):
    success: bool
    date: date
    total_candidates: int
    settled_now: int
    unresolved: int
    skipped_non_pending: int
    win: int
    loss: int
    void: int


class PerformanceStatsResponse(BaseModel):
    date_from: date
    date_to: date
    model_version: str | None = None
    total: int
    pending: int
    win: int
    loss: int
    void: int
    settled: int
    win_rate: float


class StakeCreateRequest(BaseModel):
    user_id: str = Field(min_length=2, max_length=120)
    sport: Sport
    amount_rol: float = Field(gt=0)
    lock_days: int = Field(ge=5, le=30)


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
