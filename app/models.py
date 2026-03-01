from datetime import datetime, date
from sqlalchemy import Date, DateTime, Float, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PickRecord(Base):
    __tablename__ = 'rolley_picks'

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    pick_date: Mapped[date] = mapped_column(Date, index=True)
    sport: Mapped[str] = mapped_column(String(20), index=True)
    league: Mapped[str] = mapped_column(String(120))
    home_team: Mapped[str] = mapped_column(String(120))
    away_team: Mapped[str] = mapped_column(String(120))
    market: Mapped[str] = mapped_column(String(64))
    selection: Mapped[str] = mapped_column(String(120))
    confidence: Mapped[float] = mapped_column(Float)
    rationale: Mapped[str] = mapped_column(Text)
    model_version: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
