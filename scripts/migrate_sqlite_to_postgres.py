from __future__ import annotations

import os
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import Session

from app.models import (
    Base,
    DailyProduct,
    DailyProductLeg,
    MatchHistory,
    PickRecord,
    PickSettlement,
    PredictionCreator,
    RolloverProgram,
    StakeDailyResult,
    StakePosition,
)


MODEL_ORDER = [
    PredictionCreator,
    RolloverProgram,
    MatchHistory,
    DailyProduct,
    PickRecord,
    PickSettlement,
    DailyProductLeg,
    StakePosition,
    StakeDailyResult,
]


def row_to_dict(row):
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


def main():
    source_url = os.environ.get('SOURCE_SQLITE_URL', 'sqlite:////app/data/rolley.db').strip()
    target_url = os.environ.get('TARGET_DATABASE_URL', '').strip()

    if not target_url:
        raise SystemExit('TARGET_DATABASE_URL is required')

    if target_url.startswith('postgres://'):
        target_url = target_url.replace('postgres://', 'postgresql://', 1)

    source_engine = create_engine(
        source_url,
        connect_args={'check_same_thread': False} if source_url.startswith('sqlite') else {},
    )
    target_engine = create_engine(target_url, pool_pre_ping=True)

    Base.metadata.create_all(bind=target_engine)

    with Session(source_engine) as source_session, Session(target_engine) as target_session:
        for model in reversed(MODEL_ORDER):
            target_session.execute(delete(model))
        target_session.commit()

        for model in MODEL_ORDER:
            rows = source_session.execute(select(model)).scalars().all()
            if not rows:
                continue
            target_session.bulk_insert_mappings(model, [row_to_dict(row) for row in rows])
            target_session.commit()
            print(f'Migrated {len(rows)} rows into {model.__tablename__}')

    print('Rolley SQLite to Postgres migration complete.')


if __name__ == '__main__':
    main()
