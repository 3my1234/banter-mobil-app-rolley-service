from __future__ import annotations

from datetime import date, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .config import get_settings
from .schemas import (
    AutoSettlementResponse,
    DailyProductsResponse,
    MovementWalletStatusResponse,
    PerformanceStatsResponse,
    PickHistoryResponse,
    PickSettlementPayload,
    RefreshResponse,
    RolloverSummaryResponse,
    Sport,
    StakeCreateRequest,
)
from .services.picks_service import PicksService
from .storage import get_db, init_db

settings = get_settings()
service = PicksService()
scheduler = AsyncIOScheduler(timezone='UTC')

app = FastAPI(title=settings.service_name, version=settings.service_version)

origins = [origin.strip() for origin in settings.cors_origins.split(',') if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins else ['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True,
)


@app.on_event('startup')
async def startup_event() -> None:
    init_db()
    if settings.cron_enabled:
        scheduler.add_job(
            _run_daily_refresh,
            trigger='cron',
            hour=settings.cron_hour_utc,
            minute=settings.cron_minute_utc,
            id='daily-picks-refresh',
            replace_existing=True,
        )
    if settings.auto_settlement_enabled:
        scheduler.add_job(
            _run_auto_settlement,
            trigger='cron',
            hour=settings.auto_settlement_hour_utc,
            minute=settings.auto_settlement_minute_utc,
            id='daily-picks-auto-settlement',
            replace_existing=True,
        )
    if (settings.cron_enabled or settings.auto_settlement_enabled) and not scheduler.running:
        scheduler.start()


@app.on_event('shutdown')
async def shutdown_event() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)


async def _run_daily_refresh() -> None:
    from .storage import SessionLocal

    db = SessionLocal()
    try:
        await service.refresh_daily_picks(db, target_date=date.today())
    finally:
        db.close()


async def _run_auto_settlement() -> None:
    from .storage import SessionLocal

    db = SessionLocal()
    try:
        target_date = date.today() - timedelta(days=max(settings.auto_settlement_offset_days, 1))
        await service.auto_settle_date(db, target_date=target_date, settled_by='AUTO_CRON')
    finally:
        db.close()


@app.get('/health')
def health() -> dict:
    return {'status': 'ok', 'service': settings.service_name, 'version': settings.service_version}


@app.get(f'{settings.api_prefix}/picks/daily')
def get_daily_picks(
    sport: Sport = Query(...),
    pick_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    target_date = pick_date or date.today()
    return service.get_daily(db, target_date=target_date, sport=sport)


@app.get(f'{settings.api_prefix}/picks/latest')
def get_latest_picks(
    limit: int = Query(default=settings.default_pick_count, ge=1, le=100),
    db: Session = Depends(get_db),
):
    return {'picks': service.get_latest(db, limit=limit)}


@app.get(f'{settings.api_prefix}/picks/history', response_model=PickHistoryResponse)
def get_pick_history(
    sport: Sport | None = Query(default=None),
    before_date: date | None = Query(default=None),
    pick_date: date | None = Query(default=None),
    limit: int = Query(default=settings.default_pick_count, ge=1, le=100),
    db: Session = Depends(get_db),
):
    return PickHistoryResponse(
        sport=sport,
        before_date=before_date,
        pick_date=pick_date,
        picks=service.get_history(db, sport=sport, before_date=before_date, pick_date=pick_date, limit=limit),
    )


@app.get(f'{settings.api_prefix}/products/daily', response_model=DailyProductsResponse)
def get_daily_products(
    sport: Sport = Query(...),
    pick_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    target_date = pick_date or date.today()
    return service.get_daily_products(db, target_date=target_date, sport=sport)


@app.post(f'{settings.api_prefix}/picks/refresh', response_model=RefreshResponse)
async def refresh_picks(
    refresh_date: date | None = Query(default=None),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')

    target_date = refresh_date or date.today()
    return await service.refresh_daily_picks(db, target_date=target_date)


@app.post(f'{settings.api_prefix}/admin/picks/rebuild', response_model=RefreshResponse)
async def rebuild_picks(
    pick_date: date | None = Query(default=None),
    sport: Sport | None = Query(default=None),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')

    target_date = pick_date or date.today()
    return await service.rebuild_daily_picks(db, target_date=target_date, sport=sport)


@app.get(f'{settings.api_prefix}/admin/picks')
def get_admin_picks(
    pick_date: date | None = Query(default=None),
    sport: Sport | None = Query(default=None),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')
    target_date = pick_date or date.today()
    return {'date': target_date, 'sport': sport, 'picks': service.list_settlement_candidates(db, target_date=target_date, sport=sport)}


@app.get(f'{settings.api_prefix}/admin/picks/history', response_model=PickHistoryResponse)
def get_admin_pick_history(
    sport: Sport | None = Query(default=None),
    before_date: date | None = Query(default=None),
    pick_date: date | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')
    return PickHistoryResponse(
        sport=sport,
        before_date=before_date,
        pick_date=pick_date,
        picks=service.get_history(db, sport=sport, before_date=before_date, pick_date=pick_date, limit=limit),
    )


@app.post(f'{settings.api_prefix}/admin/picks/{{pick_id}}/settle')
async def settle_pick(
    pick_id: str,
    payload: PickSettlementPayload = Body(...),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')
    try:
        pick = await service.settle_pick(db, pick_id=pick_id, payload=payload)
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return {'success': True, 'pick': pick}


@app.get(f'{settings.api_prefix}/movement/status', response_model=MovementWalletStatusResponse)
async def get_movement_wallet_status(
    wallet_address: str = Query(..., min_length=4),
    pick_ids: str = Query(..., min_length=1),
):
    ids = [int(part) for part in pick_ids.split(',') if part.strip().isdigit()]
    if not ids:
        raise HTTPException(status_code=400, detail='No valid pick ids provided')
    return await service.get_wallet_movement_statuses(wallet_address=wallet_address, movement_pick_ids=ids)


@app.post(f'{settings.api_prefix}/admin/picks/auto-settle', response_model=AutoSettlementResponse)
async def auto_settle_picks(
    pick_date: date | None = Query(default=None),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')
    target_date = pick_date or (date.today() - timedelta(days=max(settings.auto_settlement_offset_days, 1)))
    return await service.auto_settle_date(db, target_date=target_date, settled_by='ADMIN_AUTO')


@app.get(f'{settings.api_prefix}/admin/rollover/summary', response_model=RolloverSummaryResponse)
def get_rollover_summary(
    as_of_date: date | None = Query(default=None),
    x_admin_key: str | None = Header(default=None, alias='X-Admin-Key'),
    db: Session = Depends(get_db),
):
    if settings.admin_refresh_key and x_admin_key != settings.admin_refresh_key:
        raise HTTPException(status_code=401, detail='Unauthorized refresh key')
    return service.get_rollover_summary(db, as_of_date=as_of_date or date.today())


@app.get(f'{settings.api_prefix}/stats/performance', response_model=PerformanceStatsResponse)
def get_performance_stats(
    days: int = Query(default=30, ge=1, le=3650),
    model_version: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return service.get_performance_stats(db, days=days, model_version=model_version)


@app.post(f'{settings.api_prefix}/stakes/create')
def create_stake(
    payload: StakeCreateRequest = Body(...),
    db: Session = Depends(get_db),
):
    return service.create_stake(db, payload)


@app.get(f'{settings.api_prefix}/stakes')
def list_stakes(
    user_id: str = Query(...),
    db: Session = Depends(get_db),
):
    return service.list_stakes(db, user_id=user_id)


@app.post(f'{settings.api_prefix}/stakes/{{stake_id}}/withdraw')
def withdraw_stake(
    stake_id: str,
    user_id: str = Query(...),
    db: Session = Depends(get_db),
):
    try:
        return service.withdraw_stake(db, stake_id=stake_id, user_id=user_id)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
