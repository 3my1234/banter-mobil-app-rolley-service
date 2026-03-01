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
- Sports API provider in `app/providers/sports_provider.py`
- Gemini key in env
- Probability engine in `app/reasoning.py` with trained XGBoost model

## Cron
Daily refresh runs at `CRON_HOUR_UTC:CRON_MINUTE_UTC` and stores picks.
