from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from ..models import PickRecord
from ..providers.gemini_client import GeminiContextClient
from ..providers.sports_provider import SportsDataProvider
from ..reasoning import ProbabilityEngine, RolleyDecisionEngine
from ..schemas import DailyPicksResponse, RefreshResponse, RolleyPick, Sport


class PicksService:
    def __init__(self) -> None:
        self._sports = SportsDataProvider()
        self._gemini = GeminiContextClient()
        self._probability = ProbabilityEngine()
        self._decision = RolleyDecisionEngine()

    async def refresh_daily_picks(self, db: Session, *, target_date: date) -> RefreshResponse:
        generated = 0
        for sport in [Sport.SOCCER, Sport.BASKETBALL]:
            matches = self._sports.fetch_matches(
                sport=sport,
                target_date=datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc),
            )

            db.execute(
                delete(PickRecord).where(
                    PickRecord.pick_date == target_date,
                    PickRecord.sport == sport.value,
                )
            )

            for match in matches:
                context = await self._gemini.extract_context(match)
                model_result = self._probability.predict(match, context)
                decision = self._decision.decide(
                    sport=sport,
                    probabilities=model_result.probabilities,
                    context=context,
                )
                record = PickRecord(
                    id=str(uuid4()),
                    pick_date=target_date,
                    sport=sport.value,
                    league=match.league,
                    home_team=match.home_team,
                    away_team=match.away_team,
                    market=decision.market,
                    selection=decision.selection,
                    confidence=decision.confidence,
                    rationale=decision.rationale,
                    model_version=model_result.model_version,
                )
                db.add(record)
                generated += 1

            db.commit()

        return RefreshResponse(success=True, date=target_date, generated=generated)

    def get_daily(self, db: Session, *, target_date: date, sport: Sport) -> DailyPicksResponse:
        rows = db.scalars(
            select(PickRecord)
            .where(PickRecord.pick_date == target_date, PickRecord.sport == sport.value)
            .order_by(PickRecord.confidence.desc(), PickRecord.created_at.asc())
        ).all()

        picks = [
            RolleyPick(
                id=row.id,
                date=row.pick_date,
                sport=Sport(row.sport),
                league=row.league,
                home_team=row.home_team,
                away_team=row.away_team,
                market=row.market,
                selection=row.selection,
                confidence=row.confidence,
                rationale=row.rationale,
                model_version=row.model_version,
                created_at=row.created_at,
            )
            for row in rows
        ]
        return DailyPicksResponse(date=target_date, sport=sport, picks=picks)

    def get_latest(self, db: Session, *, limit: int = 20) -> list[RolleyPick]:
        rows = db.scalars(select(PickRecord).order_by(PickRecord.created_at.desc()).limit(limit)).all()
        return [
            RolleyPick(
                id=row.id,
                date=row.pick_date,
                sport=Sport(row.sport),
                league=row.league,
                home_team=row.home_team,
                away_team=row.away_team,
                market=row.market,
                selection=row.selection,
                confidence=row.confidence,
                rationale=row.rationale,
                model_version=row.model_version,
                created_at=row.created_at,
            )
            for row in rows
        ]
