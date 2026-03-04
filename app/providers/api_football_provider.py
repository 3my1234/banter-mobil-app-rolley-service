from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from ..config import get_settings


class APIFootballProvider:
    """API-Football enrichment provider for standings/injuries/H2H."""

    _LEAGUE_MAP = {
        'eng.1': 39,
        'esp.1': 140,
        'ger.1': 78,
        'ita.1': 135,
        'fra.1': 61,
        'ned.1': 88,
        'por.1': 94,
        'sco.1': 179,
        'bel.1': 144,
        'sui.1': 207,
        'aut.1': 218,
        'swe.1': 113,
        'nor.1': 103,
        'den.1': 119,
        'usa.1': 253,
        'mex.1': 262,
        'bra.1': 71,
        'arg.1': 128,
        'aus.1': 188,
        'jpn.1': 98,
        'chn.1': 169,
        'ksa.1': 307,
        'uefa.champions': 2,
        'uefa.europa': 3,
        'uefa.europa.conf': 848,
    }

    def __init__(self) -> None:
        settings = get_settings()
        self._enabled = bool(settings.api_football_enabled and settings.api_football_key)
        self._api_key = settings.api_football_key or ''
        self._host = settings.api_football_host
        self._base_url = f'https://{self._host}'
        self._standings_cache: dict[tuple[int, int], tuple[datetime, dict[str, dict[str, Any]]]] = {}
        self._injury_cache: dict[tuple[int, int, int], tuple[datetime, int]] = {}
        self._h2h_cache: dict[tuple[int, int], tuple[datetime, tuple[float, float, float]]] = {}

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
            'home_injuries': None,
            'away_injuries': None,
            'h2h': None,
            'sources': [],
            'has_standings': False,
            'has_injuries': False,
            'has_h2h': False,
        }
        if not self._enabled:
            return result

        league_id = self._LEAGUE_MAP.get(competition_code)
        if not league_id:
            return result

        season = self._season_for_date(target_date)
        standings = self._get_standings(league_id=league_id, season=season)
        home_entry = self._lookup_team(standings, home_team)
        away_entry = self._lookup_team(standings, away_team)
        if home_entry and away_entry:
            result['home_table_position'] = home_entry.get('rank')
            result['away_table_position'] = away_entry.get('rank')
            result['home_points'] = home_entry.get('points')
            result['away_points'] = away_entry.get('points')
            result['home_form_index'] = home_entry.get('form_index')
            result['away_form_index'] = away_entry.get('form_index')
            result['has_standings'] = True
            result['sources'].append('api-football:standings')

        home_team_id = home_entry.get('team_id') if home_entry else None
        away_team_id = away_entry.get('team_id') if away_entry else None

        if home_team_id and away_team_id:
            home_inj = self._get_injuries(league_id=league_id, season=season, team_id=int(home_team_id))
            away_inj = self._get_injuries(league_id=league_id, season=season, team_id=int(away_team_id))
            result['home_injuries'] = home_inj
            result['away_injuries'] = away_inj
            result['has_injuries'] = True
            result['sources'].append('api-football:injuries')

            h2h = self._get_h2h(home_team_id=int(home_team_id), away_team_id=int(away_team_id))
            if h2h:
                result['h2h'] = h2h
                result['has_h2h'] = True
                result['sources'].append('api-football:h2h')

        return result

    def _request(self, path: str, params: dict[str, Any]) -> Any:
        headers = {
            'x-apisports-key': self._api_key,
            'x-apisports-host': self._host,
        }
        with httpx.Client(timeout=15) as client:
            response = client.get(f'{self._base_url}{path}', headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
        return payload.get('response') or []

    def _get_standings(self, *, league_id: int, season: int) -> dict[str, dict[str, Any]]:
        now = datetime.now(timezone.utc)
        cache_key = (league_id, season)
        cached = self._standings_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

        parsed: dict[str, dict[str, Any]] = {}
        try:
            payload = self._request('/standings', {'league': league_id, 'season': season})
            league_data = payload[0].get('league', {}) if payload else {}
            standing_groups = league_data.get('standings') or []
            rows = standing_groups[0] if standing_groups and isinstance(standing_groups[0], list) else []
            for row in rows:
                team = row.get('team') or {}
                team_id = team.get('id')
                team_name = str(team.get('name') or '').strip()
                if not team_id or not team_name:
                    continue
                all_stats = row.get('all') or {}
                wins = int((all_stats.get('win') or 0))
                draws = int((all_stats.get('draw') or 0))
                played = int((all_stats.get('played') or max(1, wins + draws + int(all_stats.get('lose') or 0))))
                form_index = (wins + draws * 0.5) / max(1, played)
                parsed[self._normalize(team_name)] = {
                    'team_id': int(team_id),
                    'rank': int(row.get('rank') or 0),
                    'points': int(row.get('points') or 0),
                    'form_index': max(0.05, min(0.95, form_index)),
                }
        except Exception:
            parsed = {}

        self._standings_cache[cache_key] = (now + timedelta(hours=12), parsed)
        return parsed

    def _get_injuries(self, *, league_id: int, season: int, team_id: int) -> int:
        now = datetime.now(timezone.utc)
        cache_key = (league_id, season, team_id)
        cached = self._injury_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

        count = 0
        try:
            payload = self._request('/injuries', {'league': league_id, 'season': season, 'team': team_id})
            count = len(payload)
        except Exception:
            count = 0
        count = min(20, max(0, count))
        self._injury_cache[cache_key] = (now + timedelta(hours=1), count)
        return count

    def _get_h2h(self, *, home_team_id: int, away_team_id: int) -> tuple[float, float, float] | None:
        now = datetime.now(timezone.utc)
        cache_key = (home_team_id, away_team_id)
        cached = self._h2h_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

        h2h: tuple[float, float, float] | None = None
        try:
            payload = self._request('/fixtures/headtohead', {'h2h': f'{home_team_id}-{away_team_id}', 'last': 8})
            home_wins = 0
            away_wins = 0
            draws = 0
            for match in payload:
                teams = match.get('teams') or {}
                goals = match.get('goals') or {}
                home = teams.get('home') or {}
                away = teams.get('away') or {}
                home_goals = goals.get('home')
                away_goals = goals.get('away')
                if home_goals is None or away_goals is None:
                    continue
                if int(home_goals) == int(away_goals):
                    draws += 1
                else:
                    winner = home.get('id') if int(home_goals) > int(away_goals) else away.get('id')
                    if int(winner or 0) == home_team_id:
                        home_wins += 1
                    else:
                        away_wins += 1
            total = home_wins + away_wins + draws
            if total >= 2:
                h2h = (
                    max(0.08, min(0.9, home_wins / total)),
                    max(0.05, min(0.45, draws / total)),
                    max(0.05, min(0.9, away_wins / total)),
                )
        except Exception:
            h2h = None

        if h2h:
            self._h2h_cache[cache_key] = (now + timedelta(hours=6), h2h)
        return h2h

    def _lookup_team(self, standings: dict[str, dict[str, Any]], team_name: str) -> dict[str, Any] | None:
        key = self._normalize(team_name)
        if key in standings:
            return standings[key]

        # relaxed matching fallback
        for name_key, data in standings.items():
            if key in name_key or name_key in key:
                return data
        return None

    def _season_for_date(self, target_date: datetime) -> int:
        return target_date.year if target_date.month >= 7 else target_date.year - 1

    def _normalize(self, value: str) -> str:
        return ' '.join(value.lower().replace('&', ' and ').split())
