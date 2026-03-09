from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import and_, create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.models import Base, PickRecord, PickSettlement
from app.schemas import SettlementOutcome
from app.services.picks_service import PicksService


@dataclass
class EvalStats:
    model_version: str
    total: int
    win: int
    loss: int
    void: int

    @property
    def settled(self) -> int:
        return self.win + self.loss + self.void

    @property
    def win_rate(self) -> float:
        denominator = self.win + self.loss
        return (self.win / denominator) if denominator > 0 else 0.0


def _resolve_sqlite_file(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        raise ValueError(f"Only sqlite database_url is supported in this script, got: {database_url}")
    raw = database_url.removeprefix("sqlite:///")
    return Path(raw).resolve()


def _set_env(key: str, value: str) -> None:
    os.environ[key] = value


def _compute_model_stats(
    db: Session,
    *,
    date_from: date,
    date_to: date,
    model_version: str,
    primary_only: bool = True,
) -> EvalStats:
    where = [
        PickRecord.pick_date >= date_from,
        PickRecord.pick_date <= date_to,
        PickRecord.model_version == model_version,
    ]
    if primary_only:
        where.append(PickRecord.is_primary.is_(True))

    rows = db.execute(
        select(PickSettlement.outcome, func.count())
        .join(PickRecord, PickRecord.id == PickSettlement.pick_id)
        .where(*where)
        .group_by(PickSettlement.outcome)
    ).all()
    counts = {outcome: int(count) for outcome, count in rows}
    return EvalStats(
        model_version=model_version,
        total=sum(counts.values()),
        win=counts.get(SettlementOutcome.WIN.value, 0),
        loss=counts.get(SettlementOutcome.LOSS.value, 0),
        void=counts.get(SettlementOutcome.VOID.value, 0),
    )


def _detect_champion_version(db: Session, *, date_from: date, date_to: date) -> str | None:
    row = db.execute(
        select(PickRecord.model_version, func.count().label("c"))
        .join(PickSettlement, PickSettlement.pick_id == PickRecord.id)
        .where(
            PickRecord.pick_date >= date_from,
            PickRecord.pick_date <= date_to,
            PickRecord.is_primary.is_(True),
            PickSettlement.outcome.in_(
                [
                    SettlementOutcome.WIN.value,
                    SettlementOutcome.LOSS.value,
                    SettlementOutcome.VOID.value,
                ]
            ),
        )
        .group_by(PickRecord.model_version)
        .order_by(func.count().desc())
        .limit(1)
    ).first()
    return str(row[0]) if row else None


def _update_env_file(env_path: Path, key: str, value: str) -> None:
    lines = env_path.read_text(encoding="utf-8").splitlines()
    replaced = False
    for idx, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[idx] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _run_candidate_backtest(
    *,
    temp_db_path: Path,
    candidate_model_path: Path,
    date_from: date,
    date_to: date,
) -> str:
    _set_env("DATABASE_URL", f"sqlite:///{temp_db_path.as_posix()}")
    _set_env("XGBOOST_MODEL_PATH", candidate_model_path.as_posix())
    _set_env("PREDICTION_EXCLUDE_STARTED_MATCHES", "false")
    _set_env("CRON_ENABLED", "false")

    get_settings.cache_clear()
    service = PicksService()

    engine = create_engine(
        f"sqlite:///{temp_db_path.as_posix()}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    day = date_from
    candidate_version = ""
    while day <= date_to:
        with SessionLocal() as db:
            await service.refresh_daily_picks(db, target_date=day)
            await service.auto_settle_date(db, target_date=day, settled_by="BACKTEST")
            if not candidate_version:
                row = db.scalar(
                    select(PickRecord.model_version)
                    .where(PickRecord.pick_date == day)
                    .order_by(PickRecord.created_at.desc())
                )
                if row:
                    candidate_version = str(row)
        day += timedelta(days=1)

    if not candidate_version:
        candidate_version = "unknown"
    return candidate_version


def main() -> None:
    parser = argparse.ArgumentParser(description="Champion vs challenger model promotion gate")
    parser.add_argument(
        "--candidate-model",
        "--candidate-model-path",
        dest="candidate_model_path",
        required=True,
        help="Path to candidate model json",
    )
    parser.add_argument("--days", type=int, default=14, help="Evaluation window in days")
    parser.add_argument("--champion-version", default="", help="Optional explicit champion model_version")
    parser.add_argument("--min-settled", type=int, default=30, help="Minimum settled picks required for promotion")
    parser.add_argument(
        "--min-improvement",
        type=float,
        default=0.02,
        help="Required win-rate improvement over champion (e.g. 0.02 = +2pp)",
    )
    parser.add_argument("--promote", action="store_true", help="Update .env XGBOOST_MODEL_PATH when gate passes")
    parser.add_argument("--env-file", default=".env", help="Env file to update on promotion")
    args = parser.parse_args()

    candidate_path = Path(args.candidate_model_path).resolve()
    if not candidate_path.exists():
        raise FileNotFoundError(f"Candidate model not found: {candidate_path}")

    settings = get_settings()
    source_db_path = _resolve_sqlite_file(settings.database_url)
    if not source_db_path.exists():
        raise FileNotFoundError(f"DB file not found: {source_db_path}")

    today = date.today()
    date_to = today - timedelta(days=1)
    date_from = date_to - timedelta(days=max(1, args.days) - 1)

    engine = create_engine(
        f"sqlite:///{source_db_path.as_posix()}",
        connect_args={"check_same_thread": False},
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    with SessionLocal() as db:
        champion_version = args.champion_version.strip() or _detect_champion_version(
            db,
            date_from=date_from,
            date_to=date_to,
        )
        if not champion_version:
            raise RuntimeError("No champion model version found in settled primary picks for the evaluation window.")
        champion_stats = _compute_model_stats(
            db,
            date_from=date_from,
            date_to=date_to,
            model_version=champion_version,
            primary_only=True,
        )

    with tempfile.TemporaryDirectory(prefix="rolley-candidate-") as td:
        temp_db = Path(td) / "candidate_eval.db"
        shutil.copy2(source_db_path, temp_db)
        candidate_version = asyncio.run(
            _run_candidate_backtest(
                temp_db_path=temp_db,
                candidate_model_path=candidate_path,
                date_from=date_from,
                date_to=date_to,
            )
        )
        eval_engine = create_engine(
            f"sqlite:///{temp_db.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        EvalSession = sessionmaker(bind=eval_engine, autoflush=False, autocommit=False)
        with EvalSession() as db:
            candidate_stats = _compute_model_stats(
                db,
                date_from=date_from,
                date_to=date_to,
                model_version=candidate_version,
                primary_only=True,
            )

    improvement = candidate_stats.win_rate - champion_stats.win_rate
    gate_passed = (
        candidate_stats.settled >= max(1, args.min_settled)
        and champion_stats.settled >= max(1, args.min_settled)
        and improvement >= args.min_improvement
    )

    report = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "champion": {
            "model_version": champion_stats.model_version,
            "settled": champion_stats.settled,
            "win": champion_stats.win,
            "loss": champion_stats.loss,
            "void": champion_stats.void,
            "win_rate": round(champion_stats.win_rate, 4),
        },
        "candidate": {
            "model_version": candidate_stats.model_version,
            "settled": candidate_stats.settled,
            "win": candidate_stats.win,
            "loss": candidate_stats.loss,
            "void": candidate_stats.void,
            "win_rate": round(candidate_stats.win_rate, 4),
        },
        "improvement": round(improvement, 4),
        "min_improvement": args.min_improvement,
        "min_settled": args.min_settled,
        "gate_passed": gate_passed,
        "candidate_model_path": candidate_path.as_posix(),
    }
    print(json.dumps(report, indent=2))

    if args.promote and gate_passed:
        env_path = Path(args.env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Env file not found: {env_path}")
        backup = env_path.with_suffix(env_path.suffix + f".bak.{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
        shutil.copy2(env_path, backup)
        _update_env_file(env_path, "XGBOOST_MODEL_PATH", candidate_path.as_posix())
        print(f"Promoted candidate. Updated {env_path} (backup: {backup})")
    elif args.promote and not gate_passed:
        print("Promotion skipped: gate did not pass.")


if __name__ == "__main__":
    main()
