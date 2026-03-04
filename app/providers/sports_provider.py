from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Any

import httpx
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from ..config import get_settings
from ..models import MatchHistory
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
        self._injury_cache: dict[tuple[str, str, str], int] = {}
        self._standings_cache: dict[tuple[str, str], dict[str, dict[str, float | int | str]]] = {}

    def fetch_matches(self, *, sport: Sport, target_date: datetime, db: Session | None = None) -> list[MatchCandidate]:
        if self._provider == 'STUB':
            return self._stub_matches(sport=sport, target_date=target_date)

        if self._provider == 'ESPN':
            matches = self._fetch_espn_matches(sport=sport, target_date=target_date, db=db)
            if matches:
                return matches

        if self._settings.sports_fallback_to_stub:
            return self._stub_matches(sport=sport, target_date=target_date)
        return []

    def _fetch_espn_matches(self, *, sport: Sport, target_date: datetime, db: Session | None = None) -> list[MatchCandidate]:
        date_token = target_date.strftime('%Y%m%d')
        competitions = self._get_competitions(sport)
        if not competitions:
            return []

        matches: list[MatchCandidate] = []
        for competition in competitions:
            standings = self._get_standings(sport=sport, competition=competition)
            url = self._espn_url(sport=sport, competition=competition, date_token=date_token)
            try:
                with httpx.Client(timeout=15) as client:
                    response = client.get(url)
                    response.raise_for_status()
                payload = response.json()
            except Exception:
                continue

            events = payload.get('events') or []
            if db is not None:
                self._upsert_completed_results(db=db, sport=sport, events=events)
            for item in events:
                parsed = self._parse_espn_event(
                    item=item,
                    sport=sport,
                    competition=competition,
                    target_date=target_date,
                    standings=standings,
                    db=db,
                )
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

    def _parse_espn_event(
        self,
        *,
        item: dict[str, Any],
        sport: Sport,
        competition: str,
        target_date: datetime,
        standings: dict[str, dict[str, float | int | str]],
        db: Session | None,
    ) -> MatchCandidate | None:
        event_id = str(item.get('id') or '').strip()
        kickoff_raw = item.get('date')
        competitions = item.get('competitions') or []
        event_competition = competitions[0] if competitions else {}
        competitors = event_competition.get('competitors') or []
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
        home_team_payload = home.get('team') or {}
        away_team_payload = away.get('team') or {}
        home_team_id = str(home_team_payload.get('id') or '').strip() or None
        away_team_id = str(away_team_payload.get('id') or '').strip() or None

        home_team = (
            home_team_payload.get('displayName')
            or home_team_payload.get('shortDisplayName')
            or home_team_payload.get('name')
            or 'Home'
        )
        away_team = (
            away_team_payload.get('displayName')
            or away_team_payload.get('shortDisplayName')
            or away_team_payload.get('name')
            or 'Away'
        )
        league = (
            event_competition.get('league', {}).get('name')
            or item.get('league', {}).get('name')
            or item.get('shortName')
            or 'Unknown'
        )

        home_pct = self._record_win_pct(home)
        away_pct = self._record_win_pct(away)
        home_table = self._lookup_team_table(standings=standings, team_name=str(home_team), team_id=home_team_id)
        away_table = self._lookup_team_table(standings=standings, team_name=str(away_team), team_id=away_team_id)

        home_form = float(home_table.get('form_index') or home_pct)
        away_form = float(away_table.get('form_index') or away_pct)

        home_injuries = self._get_team_injuries(sport=sport, competition_code=competition, team_id=home_team_id)
        away_injuries = self._get_team_injuries(sport=sport, competition_code=competition, team_id=away_team_id)
        injury_penalty_home = min(0.25, home_injuries * 0.015)
        injury_penalty_away = min(0.25, away_injuries * 0.015)
        home_form = max(0.05, min(0.95, home_form - injury_penalty_home + injury_penalty_away * 0.35))
        away_form = max(0.05, min(0.95, away_form - injury_penalty_away + injury_penalty_home * 0.35))

        h2h = self._compute_h2h_from_history(db=db, sport=sport, home_team=str(home_team), away_team=str(away_team))
        if h2h is None:
            h2h = self._strength_based_h2h(home_form=home_form, away_form=away_form, sport=sport)
        home_h2h, draw, away_h2h = h2h

        return MatchCandidate(
            external_match_id=f'{sport.value}-{event_id}',
            sport=sport,
            league=league,
            home_team=str(home_team),
            away_team=str(away_team),
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            kick_off_utc=kickoff,
            h2h_home_win_rate=round(home_h2h, 4),
            h2h_draw_rate=round(draw, 4),
            h2h_away_win_rate=round(away_h2h, 4),
            home_form_index=round(home_form, 4),
            away_form_index=round(away_form, 4),
            home_table_position=int(home_table.get('position')) if home_table.get('position') else None,
            away_table_position=int(away_table.get('position')) if away_table.get('position') else None,
            home_points=int(home_table.get('points')) if home_table.get('points') is not None else None,
            away_points=int(away_table.get('points')) if away_table.get('points') is not None else None,
            home_injuries=home_injuries,
            away_injuries=away_injuries,
        )

    def _upsert_completed_results(self, *, db: Session, sport: Sport, events: list[dict[str, Any]]) -> None:
        for item in events:
            event_id = str(item.get('id') or '').strip()
            kickoff = self._parse_datetime(str(item.get('date') or ''))
            competitions = item.get('competitions') or []
            comp = competitions[0] if competitions else {}
            competitors = comp.get('competitors') or []
            status = ((item.get('status') or {}).get('type') or {})
            completed = bool(status.get('completed'))
            if not completed or not event_id or kickoff is None or len(competitors) < 2:
                continue

            home = next((c for c in competitors if str(c.get('homeAway', '')).lower() == 'home'), competitors[0])
            away = next((c for c in competitors if str(c.get('homeAway', '')).lower() == 'away'), competitors[1])

            home_team = (
                (home.get('team') or {}).get('displayName')
                or (home.get('team') or {}).get('shortDisplayName')
                or (home.get('team') or {}).get('name')
            )
            away_team = (
                (away.get('team') or {}).get('displayName')
                or (away.get('team') or {}).get('shortDisplayName')
                or (away.get('team') or {}).get('name')
            )
            if not home_team or not away_team:
                continue

            try:
                home_score = int(float(home.get('score') or 0))
                away_score = int(float(away.get('score') or 0))
            except Exception:
                continue

            history_id = f'{sport.value}-{event_id}'
            row = db.get(MatchHistory, history_id)
            if row is None:
                row = MatchHistory(id=history_id)

            row.sport = sport.value
            row.league = (
                comp.get('league', {}).get('name')
                or (item.get('league') or {}).get('name')
                or item.get('shortName')
                or 'Unknown'
            )
            row.home_team = str(home_team)
            row.away_team = str(away_team)
            row.kick_off_utc = kickoff
            row.home_score = home_score
            row.away_score = away_score
            row.status = str(status.get('name') or status.get('detail') or 'FINAL')
            db.add(row)

    def _compute_h2h_from_history(
        self,
        *,
        db: Session | None,
        sport: Sport,
        home_team: str,
        away_team: str,
        max_matches: int = 8,
    ) -> tuple[float, float, float] | None:
        if db is None:
            return None

        rows = db.scalars(
            select(MatchHistory)
            .where(
                MatchHistory.sport == sport.value,
                or_(
                    (MatchHistory.home_team == home_team) & (MatchHistory.away_team == away_team),
                    (MatchHistory.home_team == away_team) & (MatchHistory.away_team == home_team),
                ),
            )
            .order_by(MatchHistory.kick_off_utc.desc())
            .limit(max_matches)
        ).all()

        if len(rows) < 2:
            return None

        home_wins = 0
        away_wins = 0
        draws = 0
        for row in rows:
            if row.home_score == row.away_score:
                draws += 1
                continue

            winner = row.home_team if row.home_score > row.away_score else row.away_team
            if winner == home_team:
                home_wins += 1
            else:
                away_wins += 1

        total = max(1, home_wins + away_wins + draws)
        if sport == Sport.BASKETBALL:
            home_rate = home_wins / total
            away_rate = away_wins / total
            draw_rate = 0.01
            remap = max(home_rate + away_rate, 0.01)
            home_rate = home_rate / remap
            away_rate = away_rate / remap
            return (
                max(0.05, min(0.95, home_rate)),
                draw_rate,
                max(0.04, min(0.95, away_rate)),
            )

        return (
            max(0.08, min(0.9, home_wins / total)),
            max(0.05, min(0.45, draws / total)),
            max(0.05, min(0.9, away_wins / total)),
        )

    def _strength_based_h2h(self, *, home_form: float, away_form: float, sport: Sport) -> tuple[float, float, float]:
        home_h2h = max(0.15, min(0.82, 0.5 + (home_form - away_form) * 0.62))
        away_h2h = max(0.1, min(0.78, 1 - home_h2h - (0.18 if sport == Sport.SOCCER else 0.01)))
        draw = max(0.05, 1 - home_h2h - away_h2h) if sport == Sport.SOCCER else 0.01
        return home_h2h, draw, away_h2h

    def _get_standings(self, *, sport: Sport, competition: str) -> dict[str, dict[str, float | int | str]]:
        cache_key = (sport.value, competition)
        cached = self._standings_cache.get(cache_key)
        if cached is not None:
            return cached

        if sport == Sport.SOCCER:
            url = f'https://site.api.espn.com/apis/site/v2/sports/soccer/{competition}/standings'
        else:
            url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/{competition}/standings'

        try:
            with httpx.Client(timeout=15) as client:
                response = client.get(url)
                response.raise_for_status()
            payload = response.json()
            parsed = self._parse_standings(payload=payload)
            self._standings_cache[cache_key] = parsed
            return parsed
        except Exception:
            self._standings_cache[cache_key] = {}
            return {}

    def _parse_standings(self, *, payload: dict[str, Any]) -> dict[str, dict[str, float | int | str]]:
        entries: list[dict[str, Any]] = []

        direct = ((payload.get('standings') or {}).get('entries')) or []
        if isinstance(direct, list):
            entries.extend([entry for entry in direct if isinstance(entry, dict)])

        for child in payload.get('children') or []:
            if not isinstance(child, dict):
                continue
            child_entries = ((child.get('standings') or {}).get('entries')) or []
            if isinstance(child_entries, list):
                entries.extend([entry for entry in child_entries if isinstance(entry, dict)])

        standings: dict[str, dict[str, float | int | str]] = {}
        for entry in entries:
            team = entry.get('team') or {}
            team_name = (
                team.get('displayName')
                or team.get('shortDisplayName')
                or team.get('name')
                or team.get('abbreviation')
            )
            if not team_name:
                continue

            team_id = str(team.get('id') or '').strip()
            stats = self._stats_index(entry.get('stats') or [])
            wins = self._to_int(stats.get('wins'))
            draws = self._to_int(stats.get('ties'))
            losses = self._to_int(stats.get('losses'))
            games_played = self._to_int(stats.get('gamesPlayed'))
            if games_played <= 0:
                games_played = max(1, wins + draws + losses)

            form_index = self._to_float(stats.get('winPercent'))
            if form_index <= 0 and games_played > 0:
                form_index = (wins + draws * 0.5) / games_played

            standing = {
                'team_id': team_id,
                'position': self._to_int(stats.get('rank') or stats.get('playoffSeed') or stats.get('position')),
                'points': self._to_int(stats.get('points')),
                'form_index': max(0.05, min(0.95, form_index or 0.5)),
            }
            standings[self._normalize_team_name(str(team_name))] = standing
            if team_id:
                standings[f'id:{team_id}'] = standing
        return standings

    def _stats_index(self, stats: list[dict[str, Any]]) -> dict[str, float | int]:
        indexed: dict[str, float | int] = {}
        for stat in stats:
            if not isinstance(stat, dict):
                continue
            key = str(stat.get('name') or stat.get('abbreviation') or '').strip()
            if not key:
                continue
            value = stat.get('value')
            if isinstance(value, (int, float)):
                indexed[key] = value
            else:
                try:
                    indexed[key] = float(str(value))
                except Exception:
                    continue
        return indexed

    def _lookup_team_table(
        self,
        *,
        standings: dict[str, dict[str, float | int | str]],
        team_name: str,
        team_id: str | None,
    ) -> dict[str, float | int | str]:
        if team_id:
            by_id = standings.get(f'id:{team_id}')
            if by_id:
                return by_id
        return standings.get(self._normalize_team_name(team_name), {})

    def _get_team_injuries(self, *, sport: Sport, competition_code: str, team_id: str | None) -> int:
        if not team_id:
            return 0

        cache_key = (sport.value, competition_code, team_id)
        if cache_key in self._injury_cache:
            return self._injury_cache[cache_key]

        if sport == Sport.SOCCER:
            url = f'https://site.api.espn.com/apis/site/v2/sports/soccer/{competition_code}/teams/{team_id}'
        else:
            url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/{competition_code}/teams/{team_id}'

        count = 0
        try:
            with httpx.Client(timeout=12) as client:
                response = client.get(url)
                response.raise_for_status()
            payload = response.json()
            count += self._count_injuries(payload.get('injuries') or [])
            for athlete in payload.get('athletes') or []:
                if isinstance(athlete, dict):
                    count += self._count_injuries(athlete.get('injuries') or [])
        except Exception:
            count = 0

        count = min(12, max(0, count))
        self._injury_cache[cache_key] = count
        return count

    def _count_injuries(self, injuries: list[dict[str, Any]]) -> int:
        tracked = 0
        for item in injuries:
            if not isinstance(item, dict):
                continue
            status = str(item.get('status') or item.get('type') or item.get('shortComment') or '').lower()
            if any(flag in status for flag in ('out', 'doubt', 'question', 'injur', 'suspend', 'day-to-day')):
                tracked += 1
        return tracked

    def _normalize_team_name(self, name: str) -> str:
        normalized = re.sub(r'[^a-z0-9]+', ' ', name.lower()).strip()
        return re.sub(r'\s+', ' ', normalized)

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

    def _to_int(self, value: Any) -> int:
        try:
            return int(float(value))
        except Exception:
            return 0

    def _to_float(self, value: Any) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

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
                    home_injuries=0,
                    away_injuries=0,
                )
            )
        return matches
