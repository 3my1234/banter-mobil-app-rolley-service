from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_FLOOR
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, joinedload

from ..config import get_settings
from ..models import MatchHistory, PickRecord, PickSettlement, StakeDailyResult, StakePosition
from ..providers.gemini_client import GeminiContextClient
from ..providers.sports_provider import SportsDataProvider
from ..reasoning import Decision, ProbabilityEngine, RolleyDecisionEngine
from .movement_client import MovementClient
from ..schemas import (
    AutoSettlementResponse,
    DailyPicksResponse,
    MatchCandidate,
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
SAFE_SOCCER_PRIMARY_MARKETS = {'TOTAL_GOALS', 'DOUBLE_CHANCE'}
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeagueRiskProfile:
    competition_code: str | None
    is_high_risk: bool
    is_trusted: bool
    penalty: float


@dataclass
class StagedPrediction:
    record: PickRecord
    data_completeness: float
    sport: Sport
    competition_code: str | None
    risk: LeagueRiskProfile


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
        self._movement = MovementClient()

    async def refresh_daily_picks(self, db: Session, *, target_date: date) -> RefreshResponse:
        return await self._refresh_daily_picks(db, target_date=target_date, sport=None, force_rebuild=False)

    async def rebuild_daily_picks(
        self,
        db: Session,
        *,
        target_date: date,
        sport: Sport | None = None,
    ) -> RefreshResponse:
        return await self._refresh_daily_picks(db, target_date=target_date, sport=sport, force_rebuild=True)

    async def _refresh_daily_picks(
        self,
        db: Session,
        *,
        target_date: date,
        sport: Sport | None,
        force_rebuild: bool,
    ) -> RefreshResponse:
        generated = 0
        now_utc = datetime.now(timezone.utc)
        sports_to_process = [sport] if sport else [Sport.SOCCER, Sport.BASKETBALL]
        for current_sport in sports_to_process:
            existing_rows = db.scalars(
                select(PickRecord)
                .options(joinedload(PickRecord.settlement))
                .where(PickRecord.pick_date == target_date, PickRecord.sport == current_sport.value)
            ).all()
            preserved_match_ids = set()
            if not force_rebuild:
                preserved_match_ids = {
                    row.external_match_id for row in existing_rows
                    if self._movement.enabled and row.movement_pick_id is not None
                }

            matches = self._sports.fetch_matches(
                sport=current_sport,
                target_date=datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc),
                db=db,
            )

            delete_pick_ids = [row.id for row in existing_rows]
            if not force_rebuild:
                delete_pick_ids = [
                    row.id for row in existing_rows
                    if not (self._movement.enabled and row.movement_pick_id is not None)
                ]
            if delete_pick_ids:
                db.execute(delete(PickSettlement).where(PickSettlement.pick_id.in_(delete_pick_ids)))
                db.execute(delete(PickRecord).where(PickRecord.id.in_(delete_pick_ids)))
            db.flush()

            staged: list[StagedPrediction] = []
            for match in matches:
                if match.external_match_id in preserved_match_ids:
                    continue
                if self._should_skip_match_for_prediction(
                    target_date=target_date,
                    kick_off_utc=match.kick_off_utc,
                    now_utc=now_utc,
                ):
                    continue

                context = await self._gemini.extract_context(match)
                model_result = self._probability.predict(match, context)
                decision = self._decide_match(
                    sport=current_sport,
                    probabilities=model_result.probabilities,
                    context=context,
                    match=match,
                )
                league_risk = self._league_risk_profile(
                    sport=current_sport,
                    competition_code=match.competition_code,
                )
                total_penalty = min(0.35, match.confidence_penalty + league_risk.penalty)
                confidence, implied_odds = self._apply_match_penalty(
                    decision_confidence=decision.confidence,
                    decision_implied_odds=decision.implied_odds,
                    penalty=total_penalty,
                )
                rationale = decision.rationale
                if match.confidence_penalty > 0:
                    rationale = (
                        f'{rationale} '
                        f'[Data completeness {match.data_completeness:.0%}; '
                        f'confidence penalty {match.confidence_penalty:.0%}; '
                        f'sources: {", ".join(match.data_sources)}]'
                    )
                if league_risk.penalty > 0:
                    risk_reason = (
                        f'high-risk competition ({league_risk.competition_code or "unknown"})'
                        if league_risk.is_high_risk
                        else f'untrusted competition ({league_risk.competition_code or "unknown"})'
                    )
                    rationale = (
                        f'{rationale} '
                        f'[League risk: {risk_reason}; confidence penalty {league_risk.penalty:.0%}]'
                    )
                rationale = f'{rationale} {self._build_explain_fragment(match=match)}'
                record = PickRecord(
                    id=str(uuid4()),
                    external_match_id=match.external_match_id,
                    pick_date=target_date,
                    sport=current_sport.value,
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
                staged.append(
                    StagedPrediction(
                        record=record,
                        data_completeness=match.data_completeness,
                        sport=current_sport,
                        competition_code=match.competition_code,
                        risk=league_risk,
                    )
                )

            if staged:
                staged = self._filter_staged_predictions(staged=staged)

            if staged:
                primary_ids = self._select_primary_ids(staged=staged)
                for item in staged:
                    row = item.record
                    row.is_primary = row.id in primary_ids
                    db.add(row)
                    db.add(
                        PickSettlement(
                            id=str(uuid4()),
                            pick_id=row.id,
                            outcome=SettlementOutcome.PENDING.value,
                        )
                    )
                generated += len(staged)

            db.commit()

            if staged and self._movement.enabled:
                for item in staged:
                    await self._sync_pick_to_movement(db, pick_id=item.record.id)

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

    def get_history(
        self,
        db: Session,
        *,
        sport: Sport | None = None,
        before_date: date | None = None,
        pick_date: date | None = None,
        limit: int = 20,
    ) -> list[RolleyPick]:
        where = []
        if sport:
            where.append(PickRecord.sport == sport.value)
        if pick_date:
            where.append(PickRecord.pick_date == pick_date)
        if before_date:
            where.append(PickRecord.pick_date <= before_date)

        stmt = (
            select(PickRecord)
            .options(joinedload(PickRecord.settlement))
            .order_by(PickRecord.pick_date.desc(), PickRecord.created_at.desc())
            .limit(limit)
        )
        if where:
            stmt = stmt.where(*where)
        rows = db.scalars(stmt).all()
        return [self._to_pick_view(row) for row in rows]

    async def settle_pick(self, db: Session, *, pick_id: str, payload: PickSettlementPayload) -> RolleyPick:
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

        if (
            self._movement.enabled
            and pick.movement_pick_id
            and payload.outcome != SettlementOutcome.PENDING
            and not settlement.movement_tx_hash
        ):
            try:
                movement_result = await self._movement.settle_pick(
                    movement_pick_id=pick.movement_pick_id,
                    outcome=payload.outcome,
                    settled_at=settlement.settled_at,
                )
                settlement.movement_tx_hash = movement_result.tx_hash
                pick.movement_sync_status = movement_result.status
                db.add(settlement)
                db.add(pick)
                db.commit()
            except Exception as error:
                logger.exception('Movement settle_pick failed for local pick %s', pick.id)
                pick.movement_sync_status = 'SETTLE_FAILED'
                db.add(pick)
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

    async def auto_settle_date(
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

        if self._movement.enabled:
            settled_rows = [
                row for row in rows
                if row.movement_pick_id
                and row.settlement
                and row.settlement.outcome != SettlementOutcome.PENDING.value
                and not row.settlement.movement_tx_hash
            ]
            for row in settled_rows:
                try:
                    movement_result = await self._movement.settle_pick(
                        movement_pick_id=row.movement_pick_id,
                        outcome=SettlementOutcome(row.settlement.outcome),
                        settled_at=row.settlement.settled_at,
                    )
                    row.settlement.movement_tx_hash = movement_result.tx_hash
                    row.movement_sync_status = movement_result.status
                    db.add(row)
                    db.add(row.settlement)
                    db.commit()
                except Exception:
                    logger.exception('Movement auto-settle failed for local pick %s', row.id)
                    row.movement_sync_status = 'SETTLE_FAILED'
                    db.add(row)
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

    def _safe_odds_from_confidence(self, confidence: float) -> float:
        odds = 1.01 + max(0.0, min(0.08, (confidence - 0.55) * 0.22))
        return round(max(1.01, min(1.09, odds)), 4)

    def _decide_match(
        self,
        *,
        sport: Sport,
        probabilities,
        context,
        match: MatchCandidate,
    ) -> Decision:
        if sport == Sport.SOCCER:
            return self._decide_soccer_reasoned(probabilities=probabilities, context=context, match=match)
        if sport == Sport.BASKETBALL:
            return self._decide_basketball_reasoned(probabilities=probabilities, context=context, match=match)
        return self._decision.decide(sport=sport, probabilities=probabilities, context=context)

    def _decide_soccer_reasoned(self, *, probabilities, context, match: MatchCandidate) -> Decision:
        goal_floor = max(0.0, min(1.0, (match.home_recent5_scored_rate + match.away_recent5_scored_rate) / 2))
        h2h_balance = 1.0 - min(1.0, abs(match.h2h_home_win_rate - match.h2h_away_win_rate))
        strength_gap = match.home_recent5_opponent_strength - match.away_recent5_opponent_strength
        form_gap = match.home_form_index - match.away_form_index
        goal_diff_gap = (match.home_recent5_goal_diff - match.away_recent5_goal_diff) / 10.0
        volatility = max(0.0, min(1.0, context.volatility_index / 10.0))
        urgency = max(0.0, min(1.0, context.urgency_score / 10.0))
        weather = max(0.0, min(1.0, context.weather_impact / 10.0))
        injury_gap = max(-1.0, min(1.0, (match.away_injuries - match.home_injuries) / 10.0))

        def clamp(value: float, floor: float = 0.3, ceil: float = 0.99) -> float:
            return max(floor, min(ceil, value))

        candidates: list[tuple[float, Decision]] = []

        over05_conf = clamp(max(float(probabilities.over_05), goal_floor * 0.92), 0.45)
        over05_score = over05_conf + goal_floor * 0.10 + h2h_balance * 0.03 - weather * 0.02
        candidates.append(
            (
                over05_score,
                Decision(
                    market='TOTAL_GOALS',
                    selection='Over 0.5',
                    confidence=round(over05_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(over05_conf),
                    rationale=(
                        'Reasoned market selection favored the goals floor: both teams show scoring activity, '
                        'and recent H2H balance does not argue for a hard side pick.'
                    ),
                ),
            )
        )

        over15_conf = clamp(max(float(probabilities.over_15), goal_floor * 0.75), 0.35, 0.95)
        over15_score = over15_conf + goal_floor * 0.08 + h2h_balance * 0.05 + urgency * 0.02 - volatility * 0.03
        candidates.append(
            (
                over15_score,
                Decision(
                    market='TOTAL_GOALS',
                    selection='Over 1.5',
                    confidence=round(over15_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(over15_conf),
                    rationale=(
                        'Reasoned market selection favored a wider goals envelope: recency scoring, H2H balance, '
                        'and urgency signal enough scoring pressure for Over 1.5.'
                    ),
                ),
            )
        )

        one_x_conf = clamp(
            float(probabilities.double_chance_1x)
            + max(0.0, form_gap) * 0.10
            + max(0.0, goal_diff_gap) * 0.06
            + max(0.0, strength_gap) * 0.08
            + max(0.0, injury_gap) * 0.04,
            0.4,
        )
        one_x_score = one_x_conf + max(0.0, form_gap) * 0.10 + max(0.0, strength_gap) * 0.08 - goal_floor * 0.03
        candidates.append(
            (
                one_x_score,
                Decision(
                    market='DOUBLE_CHANCE',
                    selection='1X',
                    confidence=round(one_x_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(one_x_conf),
                    rationale=(
                        'Reasoned market selection favored 1X: the home side rates better on form, opponent-strength '
                        'context, and recent goal differential, while draw coverage preserves safety.'
                    ),
                ),
            )
        )

        x2_conf = clamp(
            float(probabilities.double_chance_x2)
            + max(0.0, -form_gap) * 0.10
            + max(0.0, -goal_diff_gap) * 0.06
            + max(0.0, -strength_gap) * 0.08
            + max(0.0, -injury_gap) * 0.04,
            0.4,
        )
        x2_score = x2_conf + max(0.0, -form_gap) * 0.10 + max(0.0, -strength_gap) * 0.08 - goal_floor * 0.03
        candidates.append(
            (
                x2_score,
                Decision(
                    market='DOUBLE_CHANCE',
                    selection='X2',
                    confidence=round(x2_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(x2_conf),
                    rationale=(
                        'Reasoned market selection favored X2: the away side carries the stronger recent profile or '
                        'faces the softer opposition context, with draw cover retained for safety.'
                    ),
                ),
            )
        )

        supported_handicap_lines = self._parse_handicap_lines(self._settings.soccer_supported_handicap_lines)
        if 1.5 in supported_handicap_lines:
            home_handicap_conf = clamp(
                float(probabilities.handicap_home_plus_15) + volatility * 0.04 - max(0.0, -form_gap) * 0.05,
                0.4,
            )
            home_handicap_score = home_handicap_conf + volatility * 0.06 - goal_floor * 0.02
            candidates.append(
                (
                    home_handicap_score,
                    Decision(
                        market='HANDICAP',
                        selection='Home +1.5',
                        confidence=round(home_handicap_conf, 4),
                        implied_odds=self._safe_odds_from_confidence(home_handicap_conf),
                        rationale=(
                            'Reasoned market selection used Home +1.5: volatility is high enough that side protection '
                            'is safer than a straight result market.'
                        ),
                    ),
                )
            )

            away_handicap_conf = clamp(
                float(probabilities.handicap_away_plus_15) + volatility * 0.04 - max(0.0, form_gap) * 0.05,
                0.4,
            )
            away_handicap_score = away_handicap_conf + volatility * 0.06 - goal_floor * 0.02
            candidates.append(
                (
                    away_handicap_score,
                    Decision(
                        market='HANDICAP',
                        selection='Away +1.5',
                        confidence=round(away_handicap_conf, 4),
                        implied_odds=self._safe_odds_from_confidence(away_handicap_conf),
                        rationale=(
                            'Reasoned market selection used Away +1.5: volatility and matchup shape favor protected '
                            'away coverage over a cleaner side market.'
                        ),
                    ),
                )
            )

        if 2.5 in supported_handicap_lines:
            home_plus_25_conf = clamp(float(probabilities.handicap_home_plus_15) + 0.05 + volatility * 0.03, 0.45)
            home_plus_25_score = home_plus_25_conf + volatility * 0.05 - goal_floor * 0.04
            candidates.append(
                (
                    home_plus_25_score,
                    Decision(
                        market='HANDICAP',
                        selection='Home +2.5',
                        confidence=round(home_plus_25_conf, 4),
                        implied_odds=self._safe_odds_from_confidence(home_plus_25_conf),
                        rationale=(
                            'Reasoned market selection widened the handicap to Home +2.5 because the platform allows '
                            'it and the volatility profile favors extra protection.'
                        ),
                    ),
                )
            )

            away_plus_25_conf = clamp(float(probabilities.handicap_away_plus_15) + 0.05 + volatility * 0.03, 0.45)
            away_plus_25_score = away_plus_25_conf + volatility * 0.05 - goal_floor * 0.04
            candidates.append(
                (
                    away_plus_25_score,
                    Decision(
                        market='HANDICAP',
                        selection='Away +2.5',
                        confidence=round(away_plus_25_conf, 4),
                        implied_odds=self._safe_odds_from_confidence(away_plus_25_conf),
                        rationale=(
                            'Reasoned market selection widened the handicap to Away +2.5 because the platform allows '
                            'it and the volatility profile favors extra protection.'
                        ),
                    ),
                )
            )

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _decide_basketball_reasoned(self, *, probabilities, context, match: MatchCandidate) -> Decision:
        offense_floor = max(0.0, min(1.0, (match.home_recent5_scored_rate + match.away_recent5_scored_rate) / 2))
        h2h_balance = 1.0 - min(1.0, abs(match.h2h_home_win_rate - match.h2h_away_win_rate))
        strength_gap = match.home_recent5_opponent_strength - match.away_recent5_opponent_strength
        form_gap = match.home_form_index - match.away_form_index
        margin_gap = (match.home_recent5_goal_diff - match.away_recent5_goal_diff) / 10.0
        volatility = max(0.0, min(1.0, context.volatility_index / 10.0))
        urgency = max(0.0, min(1.0, context.urgency_score / 10.0))
        fatigue_gap = max(-1.0, min(1.0, context.fatigue_level / 10.0))
        injury_gap = max(-1.0, min(1.0, (match.away_injuries - match.home_injuries) / 10.0))

        def clamp(value: float, floor: float = 0.35, ceil: float = 0.99) -> float:
            return max(floor, min(ceil, value))

        def score_for_side(side_bias: float, cover_bonus: float, line_penalty: float) -> float:
            return side_bias + cover_bonus + urgency * 0.03 + offense_floor * 0.02 + h2h_balance * 0.02 - line_penalty

        candidates: list[tuple[float, Decision]] = []

        home_side_bias = max(0.0, form_gap) * 0.12 + max(0.0, margin_gap) * 0.10 + max(0.0, strength_gap) * 0.08 + max(0.0, injury_gap) * 0.05
        away_side_bias = max(0.0, -form_gap) * 0.12 + max(0.0, -margin_gap) * 0.10 + max(0.0, -strength_gap) * 0.08 + max(0.0, -injury_gap) * 0.05
        volatility_cover_bonus = volatility * 0.08 + max(0.0, fatigue_gap) * 0.02

        home_plus_85_conf = clamp(
            float(probabilities.basketball_home_plus_85) + home_side_bias * 0.55 + urgency * 0.02 - volatility * 0.02,
            0.45,
        )
        candidates.append(
            (
                score_for_side(home_side_bias, volatility_cover_bonus * 0.4, 0.02),
                Decision(
                    market='ALT_SPREAD',
                    selection='Home +8.5',
                    confidence=round(home_plus_85_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(home_plus_85_conf),
                    rationale=(
                        'Reasoned market selection favored Home +8.5: home form, recent scoring margin, and opponent-strength '
                        'context support the home side while the cushion protects against late-game swings.'
                    ),
                ),
            )
        )

        away_plus_85_conf = clamp(
            float(probabilities.basketball_away_plus_85) + away_side_bias * 0.55 + urgency * 0.02 - volatility * 0.02,
            0.45,
        )
        candidates.append(
            (
                score_for_side(away_side_bias, volatility_cover_bonus * 0.4, 0.02),
                Decision(
                    market='ALT_SPREAD',
                    selection='Away +8.5',
                    confidence=round(away_plus_85_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(away_plus_85_conf),
                    rationale=(
                        'Reasoned market selection favored Away +8.5: the away profile compares better on form, recent margin, '
                        'or opponent-quality context, and the spread keeps the bet inside a safer range.'
                    ),
                ),
            )
        )

        home_plus_105_conf = clamp(home_plus_85_conf + 0.04 + volatility * 0.04, 0.5)
        candidates.append(
            (
                score_for_side(home_side_bias, volatility_cover_bonus, 0.0),
                Decision(
                    market='ALT_SPREAD',
                    selection='Home +10.5',
                    confidence=round(home_plus_105_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(home_plus_105_conf),
                    rationale=(
                        'Reasoned market selection widened to Home +10.5 because volatility and fatigue risk make extra protection '
                        'more defensible than a tighter spread.'
                    ),
                ),
            )
        )

        away_plus_105_conf = clamp(away_plus_85_conf + 0.04 + volatility * 0.04, 0.5)
        candidates.append(
            (
                score_for_side(away_side_bias, volatility_cover_bonus, 0.0),
                Decision(
                    market='ALT_SPREAD',
                    selection='Away +10.5',
                    confidence=round(away_plus_105_conf, 4),
                    implied_odds=self._safe_odds_from_confidence(away_plus_105_conf),
                    rationale=(
                        'Reasoned market selection widened to Away +10.5 because matchup volatility and rotation uncertainty favor '
                        'additional away-side cover over a tighter line.'
                    ),
                ),
            )
        )

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _parse_handicap_lines(self, raw: str) -> set[float]:
        result: set[float] = set()
        for item in raw.split(','):
            token = item.strip()
            if not token:
                continue
            try:
                result.add(float(token))
            except Exception:
                continue
        return result

    def _apply_soccer_market_guardrail(
        self,
        *,
        sport: Sport,
        decision: Decision,
        probabilities,
        match: MatchCandidate,
    ) -> Decision:
        if sport != Sport.SOCCER:
            return decision
        if not self._settings.soccer_primary_prefer_safe_markets:
            return decision
        if decision.market.upper() != 'HANDICAP':
            return decision

        goal_floor = max(0.0, min(1.0, (match.home_recent5_scored_rate + match.away_recent5_scored_rate) / 2))
        h2h_balance = 1.0 - min(1.0, abs(match.h2h_home_win_rate - match.h2h_away_win_rate))
        strength_gap = match.home_recent5_opponent_strength - match.away_recent5_opponent_strength

        over05 = max(float(probabilities.over_05), goal_floor * 0.92)
        over15 = max(float(probabilities.over_15), goal_floor * 0.75)
        one_x = float(probabilities.double_chance_1x)
        x2 = float(probabilities.double_chance_x2)

        # If one side has clearly stronger recent-opponent profile, tilt the double chance.
        if strength_gap > 0.08:
            one_x = min(0.99, one_x + 0.04)
        elif strength_gap < -0.08:
            x2 = min(0.99, x2 + 0.04)

        # Balanced H2H + both teams scoring often => totals are safer than side coverage.
        if h2h_balance > 0.72 and goal_floor > 0.65:
            over15 = min(0.95, over15 + 0.03)

        safe_candidates = [
            (
                'TOTAL_GOALS',
                'Over 0.5',
                max(0.35, min(0.99, over05)),
                'Soccer safe-market guardrail replaced handicap with goals floor.',
            ),
            (
                'TOTAL_GOALS',
                'Over 1.5',
                max(0.30, min(0.95, over15)),
                'Soccer safe-market guardrail replaced handicap with goals envelope.',
            ),
            (
                'DOUBLE_CHANCE',
                '1X',
                max(0.35, min(0.99, one_x)),
                'Soccer safe-market guardrail replaced handicap with double chance coverage.',
            ),
            (
                'DOUBLE_CHANCE',
                'X2',
                max(0.35, min(0.99, x2)),
                'Soccer safe-market guardrail replaced handicap with double chance coverage.',
            ),
        ]

        market, selection, confidence, rationale = max(safe_candidates, key=lambda item: item[2])
        return Decision(
            market=market,
            selection=selection,
            confidence=round(confidence, 4),
            implied_odds=self._safe_odds_from_confidence(confidence),
            rationale=rationale,
        )

    def _build_explain_fragment(self, *, match: MatchCandidate) -> str:
        return (
            '[Explain: '
            f'h2h_n={match.h2h_sample_size}; '
            f'h2h={match.h2h_home_win_rate:.2f}/{match.h2h_draw_rate:.2f}/{match.h2h_away_win_rate:.2f}; '
            f'recent5_scored={match.home_recent5_scored_rate:.2f}/{match.away_recent5_scored_rate:.2f}; '
            f'recent5_gd={match.home_recent5_goal_diff:.2f}/{match.away_recent5_goal_diff:.2f}; '
            f'opp_strength={match.home_recent5_opponent_strength:.2f}/{match.away_recent5_opponent_strength:.2f}'
            ']'
        )

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

    def _filter_staged_predictions(self, *, staged: list[StagedPrediction]) -> list[StagedPrediction]:
        min_confidence = max(0.0, min(0.99, float(self._settings.prediction_min_confidence)))
        max_picks = max(1, int(self._settings.prediction_max_picks_per_sport))

        filtered = [item for item in staged if item.record.confidence >= min_confidence]
        filtered.sort(key=lambda item: (item.record.confidence, item.data_completeness), reverse=True)
        return filtered[:max_picks]

    def _select_primary_ids(self, *, staged: list[StagedPrediction]) -> set[str]:
        min_completeness = max(0.0, min(1.0, float(self._settings.primary_min_completeness)))
        eligible = [item for item in staged if item.data_completeness >= min_completeness]
        source = eligible if eligible else staged
        if not source:
            return set()

        if self._settings.league_risk_block_high_risk_primary:
            non_high_risk = [item for item in source if not item.risk.is_high_risk]
            if non_high_risk:
                source = non_high_risk

        is_soccer = source[0].sport == Sport.SOCCER
        if is_soccer and self._settings.soccer_primary_prefer_safe_markets:
            safe = [item for item in source if item.record.market.upper() in SAFE_SOCCER_PRIMARY_MARKETS]
            if safe:
                source = safe
            elif not self._settings.soccer_primary_allow_handicap_fallback:
                return set()

        source.sort(key=lambda item: (item.record.confidence, item.data_completeness), reverse=True)
        return {item.record.id for item in source[: self._settings.primary_pick_count]}

    def _league_risk_profile(self, *, sport: Sport, competition_code: str | None) -> LeagueRiskProfile:
        code = (competition_code or '').strip().lower() or None
        penalty = max(0.0, min(0.25, float(self._settings.league_risk_confidence_penalty)))

        high_risk_set: set[str] = set()
        trusted_set: set[str] = set()
        if sport == Sport.SOCCER:
            high_risk_set = self._parse_competition_set(self._settings.high_risk_soccer_competitions)
            trusted_set = self._parse_competition_set(self._settings.trusted_soccer_competitions)
        elif sport == Sport.BASKETBALL:
            trusted_set = self._parse_competition_set(self._settings.trusted_basketball_competitions)

        is_high_risk = bool(code and code in high_risk_set)
        is_trusted = bool(code and code in trusted_set) if trusted_set else True

        applied_penalty = 0.0
        if is_high_risk:
            applied_penalty = penalty
        elif self._settings.league_risk_penalize_untrusted and not is_trusted:
            applied_penalty = penalty

        return LeagueRiskProfile(
            competition_code=code,
            is_high_risk=is_high_risk,
            is_trusted=is_trusted,
            penalty=applied_penalty,
        )

    def _parse_competition_set(self, raw: str) -> set[str]:
        return {item.strip().lower() for item in raw.split(',') if item.strip()}

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
            movement_pick_id=row.movement_pick_id,
            movement_tx_hash=row.movement_tx_hash,
            movement_sync_status=row.movement_sync_status,
            settlement_outcome=SettlementOutcome(outcome),
            settlement_notes=settlement.notes if settlement else None,
            settled_by=settlement.settled_by if settlement else None,
            settled_at=settlement.settled_at if settlement else None,
            settlement_movement_tx_hash=settlement.movement_tx_hash if settlement else None,
            created_at=row.created_at,
        )

    async def _sync_pick_to_movement(self, db: Session, *, pick_id: str) -> None:
        pick = db.scalar(select(PickRecord).where(PickRecord.id == pick_id).options(joinedload(PickRecord.settlement)))
        if not pick or pick.movement_pick_id is not None:
            return
        try:
            result = await self._movement.ensure_pick(pick)
            pick.movement_pick_id = result.pick_id
            if result.tx_hash:
                pick.movement_tx_hash = result.tx_hash
            pick.movement_sync_status = result.status
        except Exception:
            logger.exception('Movement create_pick failed for local pick %s', pick.id)
            pick.movement_sync_status = 'CREATE_FAILED'
        db.add(pick)
        db.commit()

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
