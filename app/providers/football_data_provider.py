from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from ..config import get_settings


class FootballDataProvider:
    """football-data.org fallback for standings + lightweight h2h."""

    _COMPETITION_MAP = {
        'eng.1': 'PL',
        'esp.1': 'PD',
        'ger.1': 'BL1',
        'ita.1': 'SA',
        'fra.1': 'FL1',
        'ned.1': 'DED',
        'por.1': 'PPL',
        'sco.1': 'PPL',  # fallback alias; competition availability depends on plan
        'uefa.champions': 'CL',
        'uefa.europa': 'EL',
        'uefa.europa.conf': 'ECL',
    }

    def __init__(self) -> None:
        settings = get_settings()
        self._enabled = bool(settings.football_data_enabled and settings.football_data_key)
        self._api_key = settings.football_data_key or ''
        self._base = 'https://api.football-data.org/v4'
        self._standings_cache: dict[tuple[str, int], tuple[datetime, dict[str, dict[str, Any]]]] = {}

    @property
    def enabled(self) -> bool:
        return self._enabled

    def enrich_soccer_match(
        self,
        *,
        competition_code: str,
        target_date: datetime,
        home_team: str,
        away_team: str,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            'home_table_position': None,
            'away_table_position': None,
            'home_points': None,
            'away_points': None,
            'home_form_index': None,
            'away_form_index': None,
            'sources': [],
            'has_standings': False,
        }
        if not self._enabled:
            return result

        comp = self._COMPETITION_MAP.get(competition_code)
        if not comp:
            return result

        season = target_date.year if target_date.month >= 7 else target_date.year - 1
        standings = self._get_standings(comp_code=comp, season=season)
        home_entry = self._lookup(standings, home_team)
        away_entry = self._lookup(standings, away_team)
        if home_entry and away_entry:
            result['home_table_position'] = home_entry.get('position')
            result['away_table_position'] = away_entry.get('position')
            result['home_points'] = home_entry.get('points')
            result['away_points'] = away_entry.get('points')
            result['home_form_index'] = home_entry.get('form_index')
            result['away_form_index'] = away_entry.get('form_index')
            result['has_standings'] = True
            result['sources'].append('football-data:standings')
        return result

    def _get_standings(self, *, comp_code: str, season: int) -> dict[str, dict[str, Any]]:
        now = datetime.now(timezone.utc)
        cache_key = (comp_code, season)
        cached = self._standings_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

        parsed: dict[str, dict[str, Any]] = {}
        try:
            headers = {'X-Auth-Token': self._api_key}
            with httpx.Client(timeout=15) as client:
                response = client.get(
                    f'{self._base}/competitions/{comp_code}/standings',
                    headers=headers,
                    params={'season': season},
                )
                response.raise_for_status()
                payload = response.json()
            standings = payload.get('standings') or []
            table = []
            if standings:
                table = standings[0].get('table') or []
            for row in table:
                team = row.get('team') or {}
                name = str(team.get('name') or team.get('shortName') or '').strip()
                if not name:
                    continue
                won = int(row.get('won') or 0)
                draw = int(row.get('draw') or 0)
                played = int(row.get('playedGames') or max(1, won + draw + int(row.get('lost') or 0)))
                form_index = (won + draw * 0.5) / max(1, played)
                parsed[self._normalize(name)] = {
                    'position': int(row.get('position') or 0),
                    'points': int(row.get('points') or 0),
                    'form_index': max(0.05, min(0.95, form_index)),
                }
        except Exception:
            parsed = {}

        self._standings_cache[cache_key] = (now + timedelta(hours=12), parsed)
        return parsed

    def _lookup(self, standings: dict[str, dict[str, Any]], team_name: str) -> dict[str, Any] | None:
        key = self._normalize(team_name)
        if key in standings:
            return standings[key]
        for item_key, item in standings.items():
            if key in item_key or item_key in key:
                return item
        return None

    def _normalize(self, value: str) -> str:
        return ' '.join(value.lower().replace('&', ' and ').split())
