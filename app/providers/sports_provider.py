from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Any

import httpx

from ..config import get_settings
from ..schemas import MatchCandidate, Sport


class SportsDataProvider:
    """Real fixture ingestion with deterministic fallback.

    Provider modes:
    - ESPN (default, no api key)
    - STUB
    """

    def __init__(self) -> None:
        self._rng = Random(42)
        self._settings = get_settings()
        self._provider = (self._settings.sports_provider or 'ESPN').strip().upper()

    def fetch_matches(self, *, sport: Sport, target_date: datetime) -> list[MatchCandidate]:
        if self._provider == 'STUB':
            return self._stub_matches(sport=sport, target_date=target_date)

        if self._provider == 'ESPN':
            matches = self._fetch_espn_matches(sport=sport, target_date=target_date)
            if matches:
                return matches

        return self._stub_matches(sport=sport, target_date=target_date)

    def _fetch_espn_matches(self, *, sport: Sport, target_date: datetime) -> list[MatchCandidate]:
        date_token = target_date.strftime('%Y%m%d')
        competitions = self._get_competitions(sport)
        if not competitions:
            return []

        matches: list[MatchCandidate] = []
        for competition in competitions:
            url = self._espn_url(sport=sport, competition=competition, date_token=date_token)
            try:
                with httpx.Client(timeout=15) as client:
                    response = client.get(url)
                    response.raise_for_status()
                payload = response.json()
            except Exception:
                continue

            events = payload.get('events') or []
            for item in events:
                parsed = self._parse_espn_event(item=item, sport=sport, target_date=target_date)
                if parsed:
                    matches.append(parsed)

        # Deduplicate by external match id.
        unique: dict[str, MatchCandidate] = {}
        for match in matches:
            unique[match.external_match_id] = match
        ordered = sorted(unique.values(), key=lambda m: m.kick_off_utc)
        return ordered

    def _get_competitions(self, sport: Sport) -> list[str]:
        if sport == Sport.SOCCER:
            raw = self._settings.soccer_competitions
        else:
            raw = self._settings.basketball_competitions
        return [part.strip() for part in raw.split(',') if part.strip()]

    def _espn_url(self, *, sport: Sport, competition: str, date_token: str) -> str:
        if sport == Sport.SOCCER:
            return (
                f'https://site.api.espn.com/apis/site/v2/sports/soccer/{competition}/scoreboard'
                f'?dates={date_token}&limit=300'
            )
        return (
            f'https://site.api.espn.com/apis/site/v2/sports/basketball/{competition}/scoreboard'
            f'?dates={date_token}&limit=300'
        )

    def _parse_espn_event(self, *, item: dict[str, Any], sport: Sport, target_date: datetime) -> MatchCandidate | None:
        event_id = str(item.get('id') or '').strip()
        kickoff_raw = item.get('date')
        competitions = item.get('competitions') or []
        competition = competitions[0] if competitions else {}
        competitors = competition.get('competitors') or []
        if not event_id or not kickoff_raw or len(competitors) < 2:
            return None

        kickoff = self._parse_datetime(kickoff_raw)
        if kickoff is None:
            return None

        # Strict same-day filtering in UTC.
        if self._settings.same_day_only and kickoff.date() != target_date.date():
            return None

        home = next((c for c in competitors if str(c.get('homeAway', '')).lower() == 'home'), competitors[0])
        away = next((c for c in competitors if str(c.get('homeAway', '')).lower() == 'away'), competitors[1])

        home_team = (
            home.get('team', {}).get('displayName')
            or home.get('team', {}).get('shortDisplayName')
            or home.get('team', {}).get('name')
            or 'Home'
        )
        away_team = (
            away.get('team', {}).get('displayName')
            or away.get('team', {}).get('shortDisplayName')
            or away.get('team', {}).get('name')
            or 'Away'
        )
        league = (
            competition.get('league', {}).get('name')
            or item.get('league', {}).get('name')
            or item.get('shortName')
            or 'Unknown'
        )

        home_pct = self._record_win_pct(home)
        away_pct = self._record_win_pct(away)

        # H2H approximated from relative season strength when no dedicated H2H feed exists.
        home_h2h = max(0.15, min(0.8, 0.5 + (home_pct - away_pct) * 0.6))
        away_h2h = max(0.1, min(0.75, 1 - home_h2h - (0.18 if sport == Sport.SOCCER else 0.01)))
        draw = max(0.05, 1 - home_h2h - away_h2h) if sport == Sport.SOCCER else 0.01

        return MatchCandidate(
            external_match_id=f'{sport.value}-{event_id}',
            sport=sport,
            league=league,
            home_team=str(home_team),
            away_team=str(away_team),
            kick_off_utc=kickoff,
            h2h_home_win_rate=round(home_h2h, 4),
            h2h_draw_rate=round(draw, 4),
            h2h_away_win_rate=round(away_h2h, 4),
            home_form_index=round(home_pct, 4),
            away_form_index=round(away_pct, 4),
        )

    def _parse_datetime(self, value: str) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    def _record_win_pct(self, competitor: dict[str, Any]) -> float:
        records = competitor.get('records') or []
        summary = ''
        for record in records:
            r_type = str(record.get('type') or record.get('name') or '').lower()
            if r_type in {'total', 'overall'}:
                summary = str(record.get('summary') or '')
                break
        if not summary and records:
            summary = str(records[0].get('summary') or '')

        numbers = [int(part) for part in re.findall(r'\d+', summary)]
        if len(numbers) >= 2:
            wins, losses = numbers[0], numbers[1]
            draws = numbers[2] if len(numbers) >= 3 else 0
            total = wins + losses + draws
            if total > 0:
                return max(0.05, min(0.95, wins / total))
        return 0.5

    def _stub_matches(self, *, sport: Sport, target_date: datetime) -> list[MatchCandidate]:
        base = target_date.replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        if sport == Sport.SOCCER:
            fixtures = [
                ('EPL', 'Arsenal', 'Bournemouth'),
                ('EPL', 'Liverpool', 'Wolves'),
                ('LaLiga', 'Real Madrid', 'Valencia'),
            ]
        else:
            fixtures = [
                ('NBA', 'Boston Celtics', 'Chicago Bulls'),
                ('NBA', 'Denver Nuggets', 'Houston Rockets'),
                ('NBA', 'Milwaukee Bucks', 'Miami Heat'),
            ]

        matches: list[MatchCandidate] = []
        for i, (league, home, away) in enumerate(fixtures):
            home_rate = 0.52 + self._rng.random() * 0.3
            draw_rate = 0.14 + self._rng.random() * 0.18 if sport == Sport.SOCCER else 0.01
            away_rate = max(0.05, 1 - home_rate - draw_rate)
            matches.append(
                MatchCandidate(
                    external_match_id=f'{sport.value}-{target_date.date()}-{i + 1}',
                    sport=sport,
                    league=league,
                    home_team=home,
                    away_team=away,
                    kick_off_utc=base + timedelta(hours=i * 2),
                    h2h_home_win_rate=min(0.95, home_rate),
                    h2h_draw_rate=min(0.5, draw_rate),
                    h2h_away_win_rate=min(0.95, away_rate),
                    home_form_index=0.48 + self._rng.random() * 0.45,
                    away_form_index=0.34 + self._rng.random() * 0.42,
                )
            )
        return matches
