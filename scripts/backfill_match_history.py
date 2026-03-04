from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select

from app.models import MatchHistory
from app.providers.sports_provider import SportsDataProvider
from app.schemas import Sport
from app.storage import SessionLocal, init_db


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill completed match history into rolley_match_history.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--sports",
        default="SOCCER,BASKETBALL",
        help="Comma-separated sports subset: SOCCER,BASKETBALL",
    )
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must be >= start-date")

    selected: list[Sport] = []
    for token in [item.strip().upper() for item in args.sports.split(",") if item.strip()]:
        selected.append(Sport(token))
    if not selected:
        selected = [Sport.SOCCER, Sport.BASKETBALL]

    init_db()
    db = SessionLocal()
    provider = SportsDataProvider()

    day = start_date
    days = 0
    try:
        while day <= end_date:
            dt = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            for sport in selected:
                provider.fetch_matches(sport=sport, target_date=dt, db=db)
            db.commit()
            days += 1
            if days % 7 == 0:
                print(f"Processed through {day.isoformat()} ({days} days)")
            day += timedelta(days=1)

        total = db.scalar(select(func.count()).select_from(MatchHistory)) or 0
        soccer = db.scalar(select(func.count()).select_from(MatchHistory).where(MatchHistory.sport == Sport.SOCCER.value)) or 0
        basketball = db.scalar(
            select(func.count()).select_from(MatchHistory).where(MatchHistory.sport == Sport.BASKETBALL.value)
        ) or 0
        print(f"Backfill complete: {days} days")
        print(f"History rows total={total}, soccer={soccer}, basketball={basketball}")
    finally:
        db.close()


if __name__ == "__main__":
    main()

