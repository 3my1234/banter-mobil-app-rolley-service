from __future__ import annotations

import re
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_FLOOR
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, joinedload

from ..config import get_settings
from ..models import MatchHistory, PickRecord, PickSettlement, StakeDailyResult, StakePosition
from ..providers.gemini_client import GeminiContextClient
from ..providers.sports_provider import SportsDataProvider
from ..reasoning import ProbabilityEngine, RolleyDecisionEngine
from ..schemas import (
    AutoSettlementResponse,
    DailyPicksResponse,
    PickSettlementPayload,
    PerformanceStatsResponse,
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
        now_utc = datetime.now(timezone.utc)
        for sport in [Sport.SOCCER, Sport.BASKETBALL]:
            matches = self._sports.fetch_matches(
                sport=sport,
                target_date=datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc),
                db=db,
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

            staged: list[tuple[PickRecord, float]] = []
            for match in matches:
                if self._should_skip_match_for_prediction(
                    target_date=target_date,
                    kick_off_utc=match.kick_off_utc,
                    now_utc=now_utc,
                ):
                    continue

                context = await self._gemini.extract_context(match)
                model_result = self._probability.predict(match, context)
                decision = self._decision.decide(
                    sport=sport,
                    probabilities=model_result.probabilities,
                    context=context,
                )
                confidence, implied_odds = self._apply_match_penalty(
                    decision_confidence=decision.confidence,
                    decision_implied_odds=decision.implied_odds,
                    penalty=match.confidence_penalty,
                )
                rationale = decision.rationale
                if match.confidence_penalty > 0:
                    rationale = (
                        f'{rationale} '
                        f'[Data completeness {match.data_completeness:.0%}; '
                        f'confidence penalty {match.confidence_penalty:.0%}; '
                        f'sources: {", ".join(match.data_sources)}]'
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
                    confidence=confidence,
                    implied_odds=implied_odds,
                    rationale=rationale,
                    model_version=model_result.model_version,
                )
                staged.append((record, match.data_completeness))
                db.add(record)

            if staged:
                staged.sort(key=lambda item: item[0].confidence, reverse=True)
                staged = self._filter_staged_predictions(staged=staged)

            if staged:
                primary_ids = self._select_primary_ids(staged=staged)
                for row, _completeness in staged:
                    row.is_primary = row.id in primary_ids
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

    def auto_settle_date(
        self,
        db: Session,
        *,
        target_date: date,
        settled_by: str = 'AUTO_SYSTEM',
    ) -> AutoSettlementResponse:
        # Refresh historical results for the target day before settlement evaluation.
        for sport in [Sport.SOCCER, Sport.BASKETBALL]:
            self._sports.fetch_matches(
                sport=sport,
                target_date=datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc),
                db=db,
            )
        db.flush()

        rows = db.scalars(
            select(PickRecord)
            .options(joinedload(PickRecord.settlement))
            .where(PickRecord.pick_date == target_date)
            .order_by(PickRecord.created_at.asc())
        ).all()

        settled_now = 0
        unresolved = 0
        skipped_non_pending = 0
        win_count = 0
        loss_count = 0
        void_count = 0

        for pick in rows:
            settlement = pick.settlement
            if settlement is None:
                settlement = PickSettlement(id=str(uuid4()), pick_id=pick.id, outcome=SettlementOutcome.PENDING.value)
                db.add(settlement)
                db.flush()

            if settlement.outcome != SettlementOutcome.PENDING.value:
                skipped_non_pending += 1
                continue

            history = db.get(MatchHistory, pick.external_match_id)
            if history is None:
                unresolved += 1
                continue

            outcome = self._evaluate_pick_outcome(pick=pick, history=history)
            settlement.outcome = outcome.value
            settlement.settled_by = settled_by
            settlement.notes = f'auto-settled final score {history.home_score}-{history.away_score}'
            settlement.settled_at = datetime.utcnow()
            db.add(settlement)
            settled_now += 1
            if outcome == SettlementOutcome.WIN:
                win_count += 1
            elif outcome == SettlementOutcome.LOSS:
                loss_count += 1
            else:
                void_count += 1

            if pick.is_primary and outcome != SettlementOutcome.PENDING:
                self._apply_settlement_to_stakes(db, pick=pick, outcome=outcome)

        db.commit()
        return AutoSettlementResponse(
            success=True,
            date=target_date,
            total_candidates=len(rows),
            settled_now=settled_now,
            unresolved=unresolved,
            skipped_non_pending=skipped_non_pending,
            win=win_count,
            loss=loss_count,
            void=void_count,
        )

    def get_performance_stats(
        self,
        db: Session,
        *,
        days: int = 30,
        model_version: str | None = None,
    ) -> PerformanceStatsResponse:
        safe_days = max(1, min(days, 3650))
        date_to = date.today()
        date_from = date_to - timedelta(days=safe_days - 1)
        rows = db.scalars(
            select(PickRecord)
            .options(joinedload(PickRecord.settlement))
            .where(PickRecord.pick_date >= date_from, PickRecord.pick_date <= date_to)
            .order_by(PickRecord.pick_date.desc(), PickRecord.created_at.desc())
        ).all()

        pending = 0
        win = 0
        loss = 0
        void = 0
        total = 0
        for row in rows:
            if model_version and row.model_version != model_version:
                continue
            total += 1
            outcome = row.settlement.outcome if row.settlement else SettlementOutcome.PENDING.value
            if outcome == SettlementOutcome.WIN.value:
                win += 1
            elif outcome == SettlementOutcome.LOSS.value:
                loss += 1
            elif outcome == SettlementOutcome.VOID.value:
                void += 1
            else:
                pending += 1

        settled = win + loss + void
        denominator = win + loss
        win_rate = round((win / denominator), 4) if denominator > 0 else 0.0

        return PerformanceStatsResponse(
            date_from=date_from,
            date_to=date_to,
            model_version=model_version,
            total=total,
            pending=pending,
            win=win,
            loss=loss,
            void=void,
            settled=settled,
            win_rate=win_rate,
        )

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

    def _apply_match_penalty(
        self,
        *,
        decision_confidence: float,
        decision_implied_odds: float,
        penalty: float,
    ) -> tuple[float, float]:
        safe_penalty = max(0.0, min(0.25, float(penalty)))
        adjusted_confidence = round(max(0.35, min(0.99, decision_confidence * (1 - safe_penalty))), 4)
        # keep odds conservative and in platform range
        adjusted_odds = 1.01 + max(0.0, min(0.08, (adjusted_confidence - 0.55) * 0.22))
        adjusted_odds = round(max(1.01, min(decision_implied_odds, adjusted_odds, 1.09)), 4)
        return adjusted_confidence, adjusted_odds

    def _should_skip_match_for_prediction(
        self,
        *,
        target_date: date,
        kick_off_utc: datetime,
        now_utc: datetime,
    ) -> bool:
        if not self._settings.prediction_exclude_started_matches:
            return False
        if target_date > now_utc.date():
            return False
        buffer_minutes = max(0, int(self._settings.prediction_start_buffer_minutes))
        cutoff = now_utc + timedelta(minutes=buffer_minutes)
        return kick_off_utc <= cutoff

    def _filter_staged_predictions(self, *, staged: list[tuple[PickRecord, float]]) -> list[tuple[PickRecord, float]]:
        min_confidence = max(0.0, min(0.99, float(self._settings.prediction_min_confidence)))
        max_picks = max(1, int(self._settings.prediction_max_picks_per_sport))

        filtered = [item for item in staged if item[0].confidence >= min_confidence]
        filtered.sort(key=lambda item: (item[0].confidence, item[1]), reverse=True)
        return filtered[:max_picks]

    def _select_primary_ids(self, *, staged: list[tuple[PickRecord, float]]) -> set[str]:
        min_completeness = max(0.0, min(1.0, float(self._settings.primary_min_completeness)))
        eligible = [item for item in staged if item[1] >= min_completeness]
        source = eligible if eligible else staged
        source.sort(key=lambda item: (item[0].confidence, item[1]), reverse=True)
        return {row.id for row, _completeness in source[: self._settings.primary_pick_count]}

    def _evaluate_pick_outcome(self, *, pick: PickRecord, history: MatchHistory) -> SettlementOutcome:
        market = (pick.market or '').upper()
        selection = (pick.selection or '').strip().upper()
        home_score = int(history.home_score)
        away_score = int(history.away_score)
        total_goals = home_score + away_score

        if market == 'TOTAL_GOALS':
            if 'OVER 0.5' in selection:
                return SettlementOutcome.WIN if total_goals > 0 else SettlementOutcome.LOSS
            if 'OVER 1.5' in selection:
                return SettlementOutcome.WIN if total_goals > 1 else SettlementOutcome.LOSS
            threshold = self._extract_threshold(selection=selection, token='OVER')
            if threshold is not None:
                return SettlementOutcome.WIN if total_goals > threshold else SettlementOutcome.LOSS
            return SettlementOutcome.VOID

        if market in {'HANDICAP', 'ALT_SPREAD'}:
            parsed = self._extract_side_and_line(selection=selection)
            if not parsed:
                return SettlementOutcome.VOID
            side, line = parsed
            if side == 'HOME':
                return SettlementOutcome.WIN if (home_score + line) > away_score else SettlementOutcome.LOSS
            return SettlementOutcome.WIN if (away_score + line) > home_score else SettlementOutcome.LOSS

        if market == 'DOUBLE_CHANCE':
            if selection == '1X':
                return SettlementOutcome.WIN if home_score >= away_score else SettlementOutcome.LOSS
            if selection == 'X2':
                return SettlementOutcome.WIN if away_score >= home_score else SettlementOutcome.LOSS
            return SettlementOutcome.VOID

        return SettlementOutcome.VOID

    def _extract_threshold(self, *, selection: str, token: str) -> float | None:
        pattern = rf'{token}\s+([0-9]+(?:\.[0-9]+)?)'
        match = re.search(pattern, selection)
        if not match:
            return None
        try:
            return float(match.group(1))
        except Exception:
            return None

    def _extract_side_and_line(self, *, selection: str) -> tuple[str, float] | None:
        match = re.search(r'(HOME|AWAY)\s*\+\s*([0-9]+(?:\.[0-9]+)?)', selection)
        if not match:
            return None
        try:
            return match.group(1), float(match.group(2))
        except Exception:
            return None

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
