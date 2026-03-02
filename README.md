# Rolley Service (Standalone)

Standalone AI picks microservice for Banter's Rolley Bot page.

## Stack
- FastAPI + APScheduler
- SQLAlchemy (SQLite by default)
- Gemini context extraction (optional key)
- XGBoost-ready probability engine (heuristic fallback included)

## API
- `GET /health`
- `GET /api/v1/picks/daily?sport=SOCCER&pick_date=YYYY-MM-DD`
- `GET /api/v1/picks/latest?limit=10`
- `POST /api/v1/picks/refresh?refresh_date=YYYY-MM-DD` (optional `X-Admin-Key`)
- `GET /api/v1/admin/picks?pick_date=YYYY-MM-DD&sport=SOCCER` (`X-Admin-Key`)
- `POST /api/v1/admin/picks/{pick_id}/settle` (`X-Admin-Key`, outcome `WIN|LOSS|VOID|PENDING`)
- `POST /api/v1/stakes/create`
- `GET /api/v1/stakes?user_id=<id>`
- `POST /api/v1/stakes/{stake_id}/withdraw?user_id=<id>`

## Run local
```bash
cd rolley-service
python -m venv .venv
# windows
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --port 8090
```

## Banter frontend integration
Set in `banter-v3/.env`:
```bash
EXPO_PUBLIC_ROLLEY_SERVICE_URL=http://<ip-or-domain>:8090
```

## Production note
Replace providers with real feeds:
- ESPN provider is enabled by default in `app/providers/sports_provider.py`.
- Strict same-day filtering is controlled by `SAME_DAY_ONLY=true`.
- Gemini key in env enriches urgency/volatility features.
- Probability engine in `app/reasoning.py` loads trained XGBoost from `XGBOOST_MODEL_PATH`.

## Train XGBoost
```bash
cd rolley-service
python scripts/train_xgboost.py
# or with your labeled csv:
# python scripts/train_xgboost.py --dataset data/historical_training.csv --output models/rolley_xgb_v1.json --version xgb-v1
```

## Cron
Daily refresh runs at `CRON_HOUR_UTC:CRON_MINUTE_UTC` and stores picks.
