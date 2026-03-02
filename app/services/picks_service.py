from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_FLOOR
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, joinedload

from ..config import get_settings
from ..models import PickRecord, PickSettlement, StakeDailyResult, StakePosition
from ..providers.gemini_client import GeminiContextClient
from ..providers.sports_provider import SportsDataProvider
from ..reasoning import ProbabilityEngine, RolleyDecisionEngine
from ..schemas import (
    DailyPicksResponse,
    PickSettlementPayload,
    RefreshResponse,
    RolleyPick,
    SettlementOutcome,
    Sport,
    StakeCreateRequest,
    StakeCreateResponse,
    StakeListResponse,
    StakePositionView,
    StakeStatus,
    StakeWithdrawResponse,
)


ROL_DECIMALS = Decimal('100000000')
TEN_PERCENT = Decimal('0.10')


def rol_to_raw(amount_rol: float | Decimal) -> str:
    value = Decimal(str(amount_rol))
    raw = (value * ROL_DECIMALS).quantize(Decimal('1'), rounding=ROUND_FLOOR)
    return str(max(raw, Decimal('0')))


def raw_to_rol(raw: str | int | Decimal) -> float:
    value = Decimal(str(raw))
    return float(value / ROL_DECIMALS)


class PicksService:
    def __init__(self) -> None:
        self._settings = get_settings()
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
                delete(PickSettlement).where(PickSettlement.pick_id.in_(
                    select(PickRecord.id).where(PickRecord.pick_date == target_date, PickRecord.sport == sport.value)
                ))
            )
            db.execute(
                delete(PickRecord).where(
                    PickRecord.pick_date == target_date,
                    PickRecord.sport == sport.value,
                )
            )
            db.flush()

            staged: list[PickRecord] = []
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
                    external_match_id=match.external_match_id,
                    pick_date=target_date,
                    sport=sport.value,
                    league=match.league,
                    home_team=match.home_team,
                    away_team=match.away_team,
                    kick_off_utc=match.kick_off_utc,
                    market=decision.market,
                    selection=decision.selection,
                    confidence=decision.confidence,
                    implied_odds=decision.implied_odds,
                    rationale=decision.rationale,
                    model_version=model_result.model_version,
                )
                staged.append(record)
                db.add(record)

            if staged:
                staged.sort(key=lambda item: item.confidence, reverse=True)
                for idx, row in enumerate(staged):
                    row.is_primary = idx < self._settings.primary_pick_count
                    db.add(
                        PickSettlement(
                            id=str(uuid4()),
                            pick_id=row.id,
                            outcome=SettlementOutcome.PENDING.value,
                        )
                    )
                generated += len(staged)

            db.commit()

        return RefreshResponse(success=True, date=target_date, generated=generated)

    def get_daily(self, db: Session, *, target_date: date, sport: Sport) -> DailyPicksResponse:
        rows = db.scalars(
            select(PickRecord)
            .options(joinedload(PickRecord.settlement))
            .where(PickRecord.pick_date == target_date, PickRecord.sport == sport.value)
            .order_by(PickRecord.is_primary.desc(), PickRecord.confidence.desc(), PickRecord.created_at.asc())
        ).all()

        picks = [self._to_pick_view(row) for row in rows]
        primary = next((pick for pick in picks if pick.is_primary), None)
        alternatives = [pick for pick in picks if not pick.is_primary]
        return DailyPicksResponse(
            date=target_date,
            sport=sport,
            primary_pick=primary,
            alternatives=alternatives,
            picks=picks,
        )

    def get_latest(self, db: Session, *, limit: int = 20) -> list[RolleyPick]:
        rows = db.scalars(
            select(PickRecord)
            .options(joinedload(PickRecord.settlement))
            .order_by(PickRecord.created_at.desc())
            .limit(limit)
        ).all()
        return [self._to_pick_view(row) for row in rows]

    def settle_pick(self, db: Session, *, pick_id: str, payload: PickSettlementPayload) -> RolleyPick:
        pick = db.scalar(select(PickRecord).where(PickRecord.id == pick_id).options(joinedload(PickRecord.settlement)))
        if not pick:
            raise ValueError('Pick not found')

        settlement = pick.settlement
        if settlement is None:
            settlement = PickSettlement(id=str(uuid4()), pick_id=pick.id)
            db.add(settlement)

        settlement.outcome = payload.outcome.value
        settlement.notes = payload.notes
        settlement.settled_by = payload.settled_by
        settlement.settled_at = datetime.utcnow()
        db.add(settlement)
        db.flush()

        if pick.is_primary and payload.outcome != SettlementOutcome.PENDING:
            self._apply_settlement_to_stakes(db, pick=pick, outcome=payload.outcome)

        db.commit()
        db.refresh(pick)
        return self._to_pick_view(pick)

    def list_settlement_candidates(
        self,
        db: Session,
        *,
        target_date: date,
        sport: Sport | None = None,
    ) -> list[RolleyPick]:
        where = [PickRecord.pick_date == target_date]
        if sport:
            where.append(PickRecord.sport == sport.value)
        rows = db.scalars(
            select(PickRecord)
            .options(joinedload(PickRecord.settlement))
            .where(*where)
            .order_by(PickRecord.sport.asc(), PickRecord.is_primary.desc(), PickRecord.confidence.desc())
        ).all()
        return [self._to_pick_view(row) for row in rows]

    def create_stake(self, db: Session, payload: StakeCreateRequest) -> StakeCreateResponse:
        starts_on = date.today()
        ends_on = starts_on + timedelta(days=payload.lock_days)
        principal_raw = rol_to_raw(payload.amount_rol)
        position = StakePosition(
            id=str(uuid4()),
            user_id=payload.user_id,
            sport=payload.sport.value,
            principal_raw=principal_raw,
            current_raw=principal_raw,
            lock_days=payload.lock_days,
            starts_on=starts_on,
            ends_on=ends_on,
            status=StakeStatus.ACTIVE.value,
            total_factor=1.0,
        )
        db.add(position)
        db.commit()
        db.refresh(position)
        return StakeCreateResponse(success=True, stake=self._to_stake_view(position))

    def list_stakes(self, db: Session, *, user_id: str) -> StakeListResponse:
        rows = db.scalars(
            select(StakePosition).where(StakePosition.user_id == user_id).order_by(StakePosition.created_at.desc())
        ).all()
        return StakeListResponse(user_id=user_id, stakes=[self._to_stake_view(row) for row in rows])

    def withdraw_stake(self, db: Session, *, stake_id: str, user_id: str) -> StakeWithdrawResponse:
        position = db.scalar(
            select(StakePosition).where(StakePosition.id == stake_id, StakePosition.user_id == user_id)
        )
        if not position:
            raise ValueError('Stake not found')

        if position.status != StakeStatus.MATURED.value:
            raise ValueError('Stake must be matured before withdrawal')

        position.status = StakeStatus.WITHDRAWN.value
        position.withdrawn_at = datetime.utcnow()
        db.add(position)
        db.commit()
        db.refresh(position)
        return StakeWithdrawResponse(success=True, stake=self._to_stake_view(position))

    def _apply_settlement_to_stakes(self, db: Session, *, pick: PickRecord, outcome: SettlementOutcome) -> None:
        positions = db.scalars(
            select(StakePosition).where(
                StakePosition.status == StakeStatus.ACTIVE.value,
                StakePosition.sport == pick.sport,
                StakePosition.starts_on <= pick.pick_date,
                StakePosition.ends_on >= pick.pick_date,
            )
        ).all()

        for position in positions:
            exists = db.scalar(
                select(StakeDailyResult).where(
                    StakeDailyResult.stake_id == position.id,
                    StakeDailyResult.pick_date == pick.pick_date,
                )
            )
            if exists:
                continue

            starting = Decimal(position.current_raw)
            factor = Decimal('1')
            if outcome == SettlementOutcome.WIN:
                factor = Decimal(str(pick.implied_odds))
            elif outcome == SettlementOutcome.LOSS:
                factor = Decimal('0')
            elif outcome == SettlementOutcome.VOID:
                factor = Decimal('1')

            ending = (starting * factor).quantize(Decimal('1'), rounding=ROUND_FLOOR)
            position.current_raw = str(max(ending, Decimal('0')))
            position.total_factor = float(Decimal(str(position.total_factor)) * factor)

            if outcome == SettlementOutcome.LOSS:
                position.status = StakeStatus.LOST.value
                position.matured_at = datetime.utcnow()
                position.gross_profit_raw = '0'
                position.platform_fee_raw = '0'
                position.net_payout_raw = '0'
            elif pick.pick_date >= position.ends_on:
                self._mature_position(position)

            db.add(
                StakeDailyResult(
                    id=str(uuid4()),
                    stake_id=position.id,
                    pick_id=pick.id,
                    pick_date=pick.pick_date,
                    outcome=outcome.value,
                    factor=float(factor),
                    starting_raw=str(starting),
                    ending_raw=position.current_raw,
                )
            )
            db.add(position)

        db.flush()

    def _mature_position(self, position: StakePosition) -> None:
        if position.status != StakeStatus.ACTIVE.value:
            return
        current = Decimal(position.current_raw)
        principal = Decimal(position.principal_raw)
        profit = max(Decimal('0'), current - principal)
        fee = (profit * TEN_PERCENT).quantize(Decimal('1'), rounding=ROUND_FLOOR)
        net = max(Decimal('0'), current - fee)

        position.status = StakeStatus.MATURED.value
        position.matured_at = datetime.utcnow()
        position.gross_profit_raw = str(profit)
        position.platform_fee_raw = str(fee)
        position.net_payout_raw = str(net)

    def _to_pick_view(self, row: PickRecord) -> RolleyPick:
        settlement = row.settlement
        outcome = settlement.outcome if settlement else SettlementOutcome.PENDING.value
        return RolleyPick(
            id=row.id,
            external_match_id=row.external_match_id,
            date=row.pick_date,
            sport=Sport(row.sport),
            league=row.league,
            home_team=row.home_team,
            away_team=row.away_team,
            kick_off_utc=row.kick_off_utc,
            market=row.market,
            selection=row.selection,
            confidence=row.confidence,
            implied_odds=row.implied_odds,
            rationale=row.rationale,
            model_version=row.model_version,
            is_primary=row.is_primary,
            settlement_outcome=SettlementOutcome(outcome),
            settlement_notes=settlement.notes if settlement else None,
            settled_at=settlement.settled_at if settlement else None,
            created_at=row.created_at,
        )

    def _to_stake_view(self, row: StakePosition) -> StakePositionView:
        return StakePositionView(
            id=row.id,
            user_id=row.user_id,
            sport=Sport(row.sport),
            principal_rol=raw_to_rol(row.principal_raw),
            current_rol=raw_to_rol(row.current_raw),
            lock_days=row.lock_days,
            starts_on=row.starts_on,
            ends_on=row.ends_on,
            status=StakeStatus(row.status),
            total_factor=row.total_factor,
            gross_profit_rol=raw_to_rol(row.gross_profit_raw),
            platform_fee_rol=raw_to_rol(row.platform_fee_raw),
            net_payout_rol=raw_to_rol(row.net_payout_raw),
            matured_at=row.matured_at,
            withdrawn_at=row.withdrawn_at,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
