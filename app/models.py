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
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    settlement: Mapped['PickSettlement | None'] = relationship(
        back_populates='pick',
        uselist=False,
        cascade='all, delete-orphan',
    )
    stake_days: Mapped[list['StakeDailyResult']] = relationship(back_populates='pick')

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
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    pick: Mapped[PickRecord] = relationship(back_populates='settlement')


class StakePosition(Base):
    __tablename__ = 'rolley_stake_positions'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(120), index=True)
    sport: Mapped[str] = mapped_column(String(20), index=True)

    principal_raw: Mapped[str] = mapped_column(String(40))  # 1e8 decimals as integer string
    current_raw: Mapped[str] = mapped_column(String(40))

    lock_days: Mapped[int] = mapped_column(Integer)
    starts_on: Mapped[date] = mapped_column(Date, index=True)
    ends_on: Mapped[date] = mapped_column(Date, index=True)

    status: Mapped[str] = mapped_column(String(20), default='ACTIVE', index=True)  # ACTIVE/LOST/MATURED/WITHDRAWN
    total_factor: Mapped[float] = mapped_column(Float, default=1.0)

    gross_profit_raw: Mapped[str] = mapped_column(String(40), default='0')
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


class StakeDailyResult(Base):
    __tablename__ = 'rolley_stake_daily_results'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    stake_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey('rolley_stake_positions.id', ondelete='CASCADE'),
        index=True,
    )
    pick_id: Mapped[str] = mapped_column(String(36), ForeignKey('rolley_picks.id', ondelete='CASCADE'), index=True)
    pick_date: Mapped[date] = mapped_column(Date, index=True)
    outcome: Mapped[str] = mapped_column(String(12))
    factor: Mapped[float] = mapped_column(Float, default=1.0)
    starting_raw: Mapped[str] = mapped_column(String(40))
    ending_raw: Mapped[str] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    stake: Mapped[StakePosition] = relationship(back_populates='daily_results')
    pick: Mapped[PickRecord] = relationship(back_populates='stake_days')

    __table_args__ = (
        UniqueConstraint('stake_id', 'pick_date', name='uq_stake_pick_day'),
    )
