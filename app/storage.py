from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from .config import get_settings
from .models import Base


settings = get_settings()

engine = create_engine(
    settings.database_url,
    connect_args={'check_same_thread': False} if settings.database_url.startswith('sqlite') else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _run_lightweight_migrations()


def _run_lightweight_migrations() -> None:
    inspector = inspect(engine)
    with engine.begin() as connection:
        if 'rolley_picks' in inspector.get_table_names():
            columns = {column['name'] for column in inspector.get_columns('rolley_picks')}
            if 'movement_pick_id' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN movement_pick_id INTEGER'))
            if 'movement_tx_hash' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN movement_tx_hash VARCHAR(120)'))
            if 'movement_sync_status' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN movement_sync_status VARCHAR(24)'))
        if 'rolley_pick_settlements' in inspector.get_table_names():
            columns = {column['name'] for column in inspector.get_columns('rolley_pick_settlements')}
            if 'movement_tx_hash' not in columns:
                connection.execute(text('ALTER TABLE rolley_pick_settlements ADD COLUMN movement_tx_hash VARCHAR(120)'))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
