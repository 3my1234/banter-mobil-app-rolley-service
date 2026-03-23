from datetime import date, datetime
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PredictionCreator(Base):
    __tablename__ = 'rolley_creators'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    handle: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(120))
    bio: Mapped[str | None] = mapped_column(Text, default=None)
    status: Mapped[str] = mapped_column(String(20), default='ACTIVE', index=True)
    is_house: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    programs: Mapped[list['RolloverProgram']] = relationship(
        back_populates='creator',
        cascade='all, delete-orphan',
    )


class RolloverProgram(Base):
    __tablename__ = 'rolley_programs'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    creator_id: Mapped[str] = mapped_column(String(36), ForeignKey('rolley_creators.id', ondelete='CASCADE'), index=True)
    slug: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(160))
    description: Mapped[str | None] = mapped_column(Text, default=None)
    sport: Mapped[str] = mapped_column(String(20), index=True)
    stake_asset: Mapped[str] = mapped_column(String(16), default='USD')
    lock_days: Mapped[int] = mapped_column(Integer, default=5)
    creator_fee_rate: Mapped[float] = mapped_column(Float, default=0.2)
    banter_fee_share_rate: Mapped[float] = mapped_column(Float, default=0.1)
    status: Mapped[str] = mapped_column(String(20), default='ACTIVE', index=True)
    visibility: Mapped[str] = mapped_column(String(20), default='PUBLIC', index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    creator: Mapped['PredictionCreator'] = relationship(back_populates='programs')
    stakes: Mapped[list['StakePosition']] = relationship(back_populates='program')


class PickRecord(Base):
    __tablename__ = 'rolley_picks'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    external_match_id: Mapped[str] = mapped_column(String(120), index=True)
    pick_date: Mapped[date] = mapped_column(Date, index=True)
    sport: Mapped[str] = mapped_column(String(20), index=True)
    league: Mapped[str] = mapped_column(String(120))
    home_team: Mapped[str] = mapped_column(String(120))
    away_team: Mapped[str] = mapped_column(String(120))
    kick_off_utc: Mapped[datetime] = mapped_column(DateTime, index=True)
    market: Mapped[str] = mapped_column(String(64))
    selection: Mapped[str] = mapped_column(String(120))
    confidence: Mapped[float] = mapped_column(Float)
    implied_odds: Mapped[float] = mapped_column(Float, default=1.03)
    rationale: Mapped[str] = mapped_column(Text)
    model_version: Mapped[str] = mapped_column(String(64))
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    daily_product_id: Mapped[str | None] = mapped_column(String(36), ForeignKey('rolley_daily_products.id', ondelete='SET NULL'), index=True, default=None)
    movement_pick_id: Mapped[int | None] = mapped_column(Integer, default=None, index=True)
    movement_tx_hash: Mapped[str | None] = mapped_column(String(120), default=None)
    movement_sync_status: Mapped[str | None] = mapped_column(String(24), default=None, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    settlement: Mapped['PickSettlement | None'] = relationship(
        back_populates='pick',
        uselist=False,
        cascade='all, delete-orphan',
    )
    stake_days: Mapped[list['StakeDailyResult']] = relationship(back_populates='pick')
    daily_product: Mapped['DailyProduct | None'] = relationship(back_populates='picks')

    __table_args__ = (
        UniqueConstraint('pick_date', 'sport', 'external_match_id', name='uq_pick_date_sport_match'),
    )


class PickSettlement(Base):
    __tablename__ = 'rolley_pick_settlements'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    pick_id: Mapped[str] = mapped_column(String(36), ForeignKey('rolley_picks.id', ondelete='CASCADE'), unique=True)
    outcome: Mapped[str] = mapped_column(String(12), default='PENDING', index=True)  # PENDING/WIN/LOSS/VOID
    notes: Mapped[str | None] = mapped_column(Text, default=None)
    settled_by: Mapped[str | None] = mapped_column(String(120), default=None)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    movement_tx_hash: Mapped[str | None] = mapped_column(String(120), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    pick: Mapped[PickRecord] = relationship(back_populates='settlement')


class MatchHistory(Base):
    __tablename__ = 'rolley_match_history'

    id: Mapped[str] = mapped_column(String(120), primary_key=True)
    sport: Mapped[str] = mapped_column(String(20), index=True)
    league: Mapped[str] = mapped_column(String(120))
    home_team: Mapped[str] = mapped_column(String(120), index=True)
    away_team: Mapped[str] = mapped_column(String(120), index=True)
    kick_off_utc: Mapped[datetime] = mapped_column(DateTime, index=True)
    home_score: Mapped[int] = mapped_column(Integer)
    away_score: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default='FINAL')
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class DailyProduct(Base):
    __tablename__ = 'rolley_daily_products'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    product_date: Mapped[date] = mapped_column(Date, index=True)
    sport: Mapped[str] = mapped_column(String(20), index=True)
    kind: Mapped[str] = mapped_column(String(20), default='SINGLE')  # SINGLE/BASKET
    combined_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    combined_odds: Mapped[float] = mapped_column(Float, default=1.0)
    manual_factor_override: Mapped[float | None] = mapped_column(Float, default=None)
    settled_factor: Mapped[float | None] = mapped_column(Float, default=None)
    status: Mapped[str] = mapped_column(String(20), default='OPEN', index=True)  # OPEN/CLOSED/SETTLED
    outcome: Mapped[str] = mapped_column(String(12), default='PENDING', index=True)  # PENDING/WIN/LOSS/VOID
    rationale: Mapped[str] = mapped_column(Text, default='')
    settled_at: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    picks: Mapped[list['PickRecord']] = relationship(back_populates='daily_product')
    legs: Mapped[list['DailyProductLeg']] = relationship(
        back_populates='daily_product',
        cascade='all, delete-orphan',
        order_by='DailyProductLeg.leg_index',
    )
    stake_days: Mapped[list['StakeDailyResult']] = relationship(back_populates='daily_product')

    __table_args__ = (
        UniqueConstraint('product_date', 'sport', name='uq_daily_product_date_sport'),
    )


class DailyProductLeg(Base):
    __tablename__ = 'rolley_daily_product_legs'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    daily_product_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey('rolley_daily_products.id', ondelete='CASCADE'),
        index=True,
    )
    pick_id: Mapped[str] = mapped_column(String(36), ForeignKey('rolley_picks.id', ondelete='CASCADE'), index=True)
    leg_index: Mapped[int] = mapped_column(Integer, default=0)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    implied_odds: Mapped[float] = mapped_column(Float, default=1.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    daily_product: Mapped['DailyProduct'] = relationship(back_populates='legs')
    pick: Mapped['PickRecord'] = relationship()


class StakePosition(Base):
    __tablename__ = 'rolley_stake_positions'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(120), index=True)
    program_id: Mapped[str | None] = mapped_column(String(36), ForeignKey('rolley_programs.id', ondelete='SET NULL'), index=True, default=None)
    external_reference: Mapped[str | None] = mapped_column(String(120), unique=True, default=None, index=True)
    sport: Mapped[str] = mapped_column(String(20), index=True)
    stake_asset: Mapped[str] = mapped_column(String(16), default='ROL', index=True)
    asset_decimals: Mapped[int] = mapped_column(Integer, default=8)

    principal_raw: Mapped[str] = mapped_column(String(40))  # 1e8 decimals as integer string
    current_raw: Mapped[str] = mapped_column(String(40))

    lock_days: Mapped[int] = mapped_column(Integer)
    starts_on: Mapped[date] = mapped_column(Date, index=True)
    ends_on: Mapped[date] = mapped_column(Date, index=True)

    status: Mapped[str] = mapped_column(String(20), default='ACTIVE', index=True)  # ACTIVE/LOST/MATURED/WITHDRAWN
    total_factor: Mapped[float] = mapped_column(Float, default=1.0)
    creator_fee_rate: Mapped[float] = mapped_column(Float, default=0.10)
    banter_fee_share_rate: Mapped[float] = mapped_column(Float, default=1.0)

    gross_profit_raw: Mapped[str] = mapped_column(String(40), default='0')
    creator_fee_raw: Mapped[str] = mapped_column(String(40), default='0')
    creator_net_fee_raw: Mapped[str] = mapped_column(String(40), default='0')
    platform_fee_raw: Mapped[str] = mapped_column(String(40), default='0')
    net_payout_raw: Mapped[str] = mapped_column(String(40), default='0')

    matured_at: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    withdrawn_at: Mapped[datetime | None] = mapped_column(DateTime, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    daily_results: Mapped[list['StakeDailyResult']] = relationship(
        back_populates='stake',
        cascade='all, delete-orphan',
    )
    program: Mapped['RolloverProgram | None'] = relationship(back_populates='stakes')


class StakeDailyResult(Base):
    __tablename__ = 'rolley_stake_daily_results'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    stake_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey('rolley_stake_positions.id', ondelete='CASCADE'),
        index=True,
    )
    daily_product_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey('rolley_daily_products.id', ondelete='SET NULL'),
        index=True,
        default=None,
    )
    pick_id: Mapped[str] = mapped_column(String(36), ForeignKey('rolley_picks.id', ondelete='CASCADE'), index=True)
    pick_date: Mapped[date] = mapped_column(Date, index=True)
    outcome: Mapped[str] = mapped_column(String(12))
    factor: Mapped[float] = mapped_column(Float, default=1.0)
    starting_raw: Mapped[str] = mapped_column(String(40))
    ending_raw: Mapped[str] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    stake: Mapped[StakePosition] = relationship(back_populates='daily_results')
    daily_product: Mapped[DailyProduct | None] = relationship(back_populates='stake_days')
    pick: Mapped[PickRecord] = relationship(back_populates='stake_days')

    __table_args__ = (
        UniqueConstraint('stake_id', 'pick_date', name='uq_stake_pick_day'),
    )
