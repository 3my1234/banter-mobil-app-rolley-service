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
            if 'daily_product_id' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN daily_product_id VARCHAR(36)'))
            if 'movement_pick_id' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN movement_pick_id INTEGER'))
            if 'movement_tx_hash' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN movement_tx_hash VARCHAR(120)'))
            if 'movement_sync_status' not in columns:
                connection.execute(text('ALTER TABLE rolley_picks ADD COLUMN movement_sync_status VARCHAR(24)'))
        if 'rolley_stake_daily_results' in inspector.get_table_names():
            columns = {column['name'] for column in inspector.get_columns('rolley_stake_daily_results')}
            if 'daily_product_id' not in columns:
                connection.execute(text('ALTER TABLE rolley_stake_daily_results ADD COLUMN daily_product_id VARCHAR(36)'))
        if 'rolley_stake_positions' in inspector.get_table_names():
            columns = {column['name'] for column in inspector.get_columns('rolley_stake_positions')}
            if 'external_reference' not in columns:
                connection.execute(text('ALTER TABLE rolley_stake_positions ADD COLUMN external_reference VARCHAR(120)'))
                connection.execute(text('CREATE UNIQUE INDEX IF NOT EXISTS ix_rolley_stake_positions_external_reference ON rolley_stake_positions (external_reference)'))
            if 'stake_asset' not in columns:
                connection.execute(text("ALTER TABLE rolley_stake_positions ADD COLUMN stake_asset VARCHAR(16) DEFAULT 'ROL'"))
            if 'asset_decimals' not in columns:
                connection.execute(text('ALTER TABLE rolley_stake_positions ADD COLUMN asset_decimals INTEGER DEFAULT 8'))
        if 'rolley_daily_products' in inspector.get_table_names():
            columns = {column['name'] for column in inspector.get_columns('rolley_daily_products')}
            if 'manual_factor_override' not in columns:
                connection.execute(text('ALTER TABLE rolley_daily_products ADD COLUMN manual_factor_override FLOAT'))
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
