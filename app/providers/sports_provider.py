from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from ..config import get_settings
from ..models import MatchHistory
from .api_football_provider import APIFootballProvider
from .football_data_provider import FootballDataProvider
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
        self._injury_cache: dict[tuple[str, str, str], tuple[int, bool]] = {}
        self._standings_cache: dict[tuple[str, str], dict[str, dict[str, float | int | str]]] = {}
        self._recent_form_cache: dict[tuple[str, str, str], dict[str, float]] = {}
        self._opponent_strength_cache: dict[tuple[str, str, str], float] = {}
        self._api_football = APIFootballProvider()
        self._football_data = FootballDataProvider()

    def fetch_matches(self, *, sport: Sport, target_date: datetime, db: Session | None = None) -> list[MatchCandidate]:
        if self._provider == 'STUB':
            return self._stub_matches(sport=sport, target_date=target_date)

        if self._provider == 'ESPN':
            matches, _ = self._fetch_espn_matches_with_diagnostics(sport=sport, target_date=target_date, db=db)
            if matches:
                return matches

        if self._settings.sports_fallback_to_stub:
            return self._stub_matches(sport=sport, target_date=target_date)
        return []

    def fetch_matches_diagnostics(
        self,
        *,
        sport: Sport,
        target_date: datetime,
        db: Session | None = None,
    ) -> tuple[list[MatchCandidate], list[dict[str, Any]]]:
        if self._provider == 'STUB':
            matches = self._stub_matches(sport=sport, target_date=target_date)
            return matches, [
                {
                    'provider': 'STUB',
                    'competition': 'stub',
                    'event_count': len(matches),
                    'parsed_count': len(matches),
                    'error': None,
                }
            ]

        if self._provider == 'ESPN':
            matches, diagnostics = self._fetch_espn_matches_with_diagnostics(
                sport=sport,
                target_date=target_date,
                db=db,
            )
            if matches or not self._settings.sports_fallback_to_stub:
                return matches, diagnostics

        matches = self._stub_matches(sport=sport, target_date=target_date)
        return matches, [
            {
                'provider': 'STUB',
                'competition': 'stub',
                'event_count': len(matches),
                'parsed_count': len(matches),
                'error': 'fallback_to_stub',
            }
        ]

    def _fetch_espn_matches(self, *, sport: Sport, target_date: datetime, db: Session | None = None) -> list[MatchCandidate]:
        matches, _ = self._fetch_espn_matches_with_diagnostics(sport=sport, target_date=target_date, db=db)
        return matches

    def _fetch_espn_matches_with_diagnostics(
        self,
        *,
        sport: Sport,
        target_date: datetime,
        db: Session | None = None,
    ) -> tuple[list[MatchCandidate], list[dict[str, Any]]]:
        date_token = target_date.strftime('%Y%m%d')
        competitions = self._get_competitions(sport)
        if not competitions:
            return [], []

        matches: list[MatchCandidate] = []
        diagnostics: list[dict[str, Any]] = []
        for competition in competitions:
            standings = self._get_standings(sport=sport, competition=competition)
            url = self._espn_url(sport=sport, competition=competition, date_token=date_token)
            events: list[dict[str, Any]] = []
            error_message: str | None = None
            try:
                with httpx.Client(timeout=15) as client:
                    response = client.get(url)
                    response.raise_for_status()
                payload = response.json()
                events = payload.get('events') or []
            except Exception as error:
                error_message = str(error)
                diagnostics.append(
                    {
                        'provider': 'ESPN',
                        'competition': competition,
                        'event_count': 0,
                        'parsed_count': 0,
                        'error': error_message,
                    }
                )
                continue

            if db is not None:
                self._upsert_completed_results(db=db, sport=sport, events=events)
            parsed_count = 0
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
                    parsed_count += 1

            diagnostics.append(
                {
                    'provider': 'ESPN',
                    'competition': competition,
                    'event_count': len(events),
                    'parsed_count': parsed_count,
                    'error': error_message,
                }
            )

        # Deduplicate by external match id.
        unique: dict[str, MatchCandidate] = {}
        for match in matches:
            unique[match.external_match_id] = match
        ordered = sorted(unique.values(), key=lambda m: m.kick_off_utc)
        return ordered, diagnostics

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

        if self._settings.same_day_only:
            event_tz = self._event_timezone_for_sport(sport)
            kickoff_day = kickoff.astimezone(event_tz).date()
            # target_date carries the requested date token (YYYYMMDD). Keep that
            # calendar day stable instead of converting midnight UTC to local TZ.
            target_day = target_date.date()
            if kickoff_day != target_day:
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

        home_injuries, home_injury_available = self._get_team_injuries(
            sport=sport,
            competition_code=competition,
            team_id=home_team_id,
        )
        away_injuries, away_injury_available = self._get_team_injuries(
            sport=sport,
            competition_code=competition,
            team_id=away_team_id,
        )

        data_sources = ['espn:scoreboard']
        standings_complete = self._has_standings(home_table=home_table, away_table=away_table)
        injuries_complete = home_injury_available and away_injury_available
        enriched: dict[str, Any] | None = None

        if sport == Sport.SOCCER and (not standings_complete or not injuries_complete):
            enriched = self._enrich_soccer_fallback(
                competition=competition,
                target_date=target_date,
                home_team=str(home_team),
                away_team=str(away_team),
            )
            data_sources.extend(enriched.get('sources') or [])
            if not standings_complete:
                home_table = self._merge_table_data(current=home_table, fallback=enriched.get('home_table') or {})
                away_table = self._merge_table_data(current=away_table, fallback=enriched.get('away_table') or {})
                standings_complete = self._has_standings(home_table=home_table, away_table=away_table)
                if standings_complete:
                    home_form = float(home_table.get('form_index') or home_form)
                    away_form = float(away_table.get('form_index') or away_form)

            if not injuries_complete:
                if enriched.get('home_injuries') is not None:
                    home_injuries = int(enriched['home_injuries'])
                if enriched.get('away_injuries') is not None:
                    away_injuries = int(enriched['away_injuries'])
                injuries_complete = bool(enriched.get('has_injuries'))

        injury_penalty_home = min(0.25, home_injuries * 0.015)
        injury_penalty_away = min(0.25, away_injuries * 0.015)
        home_form = max(0.05, min(0.95, home_form - injury_penalty_home + injury_penalty_away * 0.35))
        away_form = max(0.05, min(0.95, away_form - injury_penalty_away + injury_penalty_home * 0.35))

        has_h2h_data = False
        h2h_sample_size = 0
        h2h = self._compute_h2h_from_history(
            db=db,
            sport=sport,
            home_team=str(home_team),
            away_team=str(away_team),
            max_matches=20,
        )
        if h2h is not None:
            data_sources.append('local:h2h')
            has_h2h_data = True
            h2h_sample_size = int(h2h[3])
        if h2h is None and sport == Sport.SOCCER:
            fallback_h2h = None
            if enriched is not None and isinstance(enriched.get('h2h'), (list, tuple)) and len(enriched['h2h']) == 3:
                item = enriched['h2h']
                fallback_h2h = (float(item[0]), float(item[1]), float(item[2]), 0)
            if fallback_h2h is None:
                fallback_h2h = self._get_soccer_h2h_fallback(
                    competition=competition,
                    target_date=target_date,
                    home_team=str(home_team),
                    away_team=str(away_team),
                )
            if fallback_h2h is not None:
                h2h = fallback_h2h
                data_sources.append('fallback:h2h')
                has_h2h_data = True
        if h2h is None:
            base = self._strength_based_h2h(home_form=home_form, away_form=away_form, sport=sport)
            h2h = (base[0], base[1], base[2], 0)
            data_sources.append('heuristic:h2h')
        home_h2h, draw, away_h2h, h2h_count = h2h
        h2h_sample_size = max(h2h_sample_size, int(h2h_count))

        recent_home = self._compute_recent_team_form(
            db=db,
            sport=sport,
            team_name=str(home_team),
            reference_time=kickoff,
            lookback=5,
        )
        recent_away = self._compute_recent_team_form(
            db=db,
            sport=sport,
            team_name=str(away_team),
            reference_time=kickoff,
            lookback=5,
        )
        if recent_home.get('sample_size', 0) >= 3 and recent_away.get('sample_size', 0) >= 3:
            data_sources.append('local:recent5')

        completeness = self._completeness_score(
            sport=sport,
            standings_complete=standings_complete,
            injuries_complete=injuries_complete,
            has_h2h=has_h2h_data,
        )
        confidence_penalty = self._confidence_penalty_from_completeness(completeness)

        return MatchCandidate(
            external_match_id=f'{sport.value}-{event_id}',
            sport=sport,
            competition_code=competition,
            league=league,
            home_team=str(home_team),
            away_team=str(away_team),
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            kick_off_utc=kickoff,
            h2h_home_win_rate=round(home_h2h, 4),
            h2h_draw_rate=round(draw, 4),
            h2h_away_win_rate=round(away_h2h, 4),
            h2h_sample_size=h2h_sample_size,
            home_form_index=round(home_form, 4),
            away_form_index=round(away_form, 4),
            home_recent5_scored_rate=round(float(recent_home.get('scored_rate', 0.5)), 4),
            away_recent5_scored_rate=round(float(recent_away.get('scored_rate', 0.5)), 4),
            home_recent5_goal_diff=round(float(recent_home.get('goal_diff_avg', 0.0)), 4),
            away_recent5_goal_diff=round(float(recent_away.get('goal_diff_avg', 0.0)), 4),
            home_recent5_opponent_strength=round(float(recent_home.get('opponent_strength', 0.5)), 4),
            away_recent5_opponent_strength=round(float(recent_away.get('opponent_strength', 0.5)), 4),
            home_table_position=int(home_table.get('position')) if home_table.get('position') else None,
            away_table_position=int(away_table.get('position')) if away_table.get('position') else None,
            home_points=int(home_table.get('points')) if home_table.get('points') is not None else None,
            away_points=int(away_table.get('points')) if away_table.get('points') is not None else None,
            home_injuries=home_injuries,
            away_injuries=away_injuries,
            data_completeness=completeness,
            confidence_penalty=confidence_penalty,
            data_sources=sorted(set(data_sources)),
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
        max_matches: int = 10,
    ) -> tuple[float, float, float, int] | None:
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

        home_wins = 0.0
        away_wins = 0.0
        draws = 0.0
        for idx, row in enumerate(rows):
            # Recency weighting: newest result has highest weight.
            weight = 1.0 / (1.0 + idx * 0.35)
            if row.home_score == row.away_score:
                draws += weight
                continue

            winner = row.home_team if row.home_score > row.away_score else row.away_team
            if winner == home_team:
                home_wins += weight
            else:
                away_wins += weight

        total = max(0.01, home_wins + away_wins + draws)
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
                len(rows),
            )

        return (
            max(0.08, min(0.9, home_wins / total)),
            max(0.05, min(0.45, draws / total)),
            max(0.05, min(0.9, away_wins / total)),
            len(rows),
        )

    def _compute_recent_team_form(
        self,
        *,
        db: Session | None,
        sport: Sport,
        team_name: str,
        reference_time: datetime,
        lookback: int = 5,
    ) -> dict[str, float]:
        if db is None:
            return {
                'sample_size': 0,
                'scored_rate': 0.5,
                'goal_diff_avg': 0.0,
                'opponent_strength': 0.5,
            }

        cache_key = (sport.value, team_name.lower(), reference_time.date().isoformat())
        cached = self._recent_form_cache.get(cache_key)
        if cached is not None:
            return cached

        rows = db.scalars(
            select(MatchHistory)
            .where(
                MatchHistory.sport == sport.value,
                MatchHistory.kick_off_utc < reference_time,
                or_(MatchHistory.home_team == team_name, MatchHistory.away_team == team_name),
            )
            .order_by(MatchHistory.kick_off_utc.desc())
            .limit(max(1, lookback))
        ).all()

        sample_size = len(rows)
        if sample_size == 0:
            result = {
                'sample_size': 0,
                'scored_rate': 0.5,
                'goal_diff_avg': 0.0,
                'opponent_strength': 0.5,
            }
            self._recent_form_cache[cache_key] = result
            return result

        scored = 0
        goal_diff_total = 0.0
        opponent_strength_total = 0.0
        home_games = 0
        away_games = 0
        home_scored = 0
        away_scored = 0
        for row in rows:
            is_home = row.home_team == team_name
            goals_for = row.home_score if is_home else row.away_score
            goals_against = row.away_score if is_home else row.home_score
            opponent = row.away_team if is_home else row.home_team
            if goals_for > 0:
                scored += 1
                if is_home:
                    home_scored += 1
                else:
                    away_scored += 1
            if is_home:
                home_games += 1
            else:
                away_games += 1
            goal_diff_total += (goals_for - goals_against)
            opponent_strength_total += self._opponent_form_before_match(
                db=db,
                sport=sport,
                team_name=opponent,
                cutoff=row.kick_off_utc,
            )

        result = {
            'sample_size': float(sample_size),
            'scored_rate': max(0.0, min(1.0, scored / sample_size)),
            'goal_diff_avg': max(-10.0, min(10.0, goal_diff_total / sample_size)),
            'opponent_strength': max(0.0, min(1.0, opponent_strength_total / sample_size)),
            'home_scored_rate': (home_scored / home_games) if home_games > 0 else 0.5,
            'away_scored_rate': (away_scored / away_games) if away_games > 0 else 0.5,
        }
        self._recent_form_cache[cache_key] = result
        return result

    def _opponent_form_before_match(
        self,
        *,
        db: Session,
        sport: Sport,
        team_name: str,
        cutoff: datetime,
        lookback: int = 10,
    ) -> float:
        cache_key = (sport.value, team_name.lower(), cutoff.date().isoformat())
        cached = self._opponent_strength_cache.get(cache_key)
        if cached is not None:
            return cached

        rows = db.scalars(
            select(MatchHistory)
            .where(
                MatchHistory.sport == sport.value,
                MatchHistory.kick_off_utc < cutoff,
                or_(MatchHistory.home_team == team_name, MatchHistory.away_team == team_name),
            )
            .order_by(MatchHistory.kick_off_utc.desc())
            .limit(max(1, lookback))
        ).all()
        if not rows:
            self._opponent_strength_cache[cache_key] = 0.5
            return 0.5

        points = 0.0
        possible = 0.0
        for row in rows:
            is_home = row.home_team == team_name
            goals_for = row.home_score if is_home else row.away_score
            goals_against = row.away_score if is_home else row.home_score
            if sport == Sport.SOCCER:
                possible += 3.0
                if goals_for > goals_against:
                    points += 3.0
                elif goals_for == goals_against:
                    points += 1.0
            else:
                possible += 1.0
                if goals_for > goals_against:
                    points += 1.0

        rating = max(0.0, min(1.0, points / possible)) if possible > 0 else 0.5
        self._opponent_strength_cache[cache_key] = rating
        return rating

    def _strength_based_h2h(self, *, home_form: float, away_form: float, sport: Sport) -> tuple[float, float, float]:
        home_h2h = max(0.15, min(0.82, 0.5 + (home_form - away_form) * 0.62))
        away_h2h = max(0.1, min(0.78, 1 - home_h2h - (0.18 if sport == Sport.SOCCER else 0.01)))
        draw = max(0.05, 1 - home_h2h - away_h2h) if sport == Sport.SOCCER else 0.01
        return home_h2h, draw, away_h2h

    def _has_standings(
        self,
        *,
        home_table: dict[str, float | int | str],
        away_table: dict[str, float | int | str],
    ) -> bool:
        return bool(
            (home_table.get('position') and away_table.get('position'))
            or (home_table.get('points') is not None and away_table.get('points') is not None)
        )

    def _merge_table_data(
        self,
        *,
        current: dict[str, float | int | str],
        fallback: dict[str, Any],
    ) -> dict[str, float | int | str]:
        merged = dict(current)
        for key in ('team_id', 'position', 'points', 'form_index'):
            if merged.get(key) in (None, '', 0):
                value = fallback.get(key)
                if value not in (None, ''):
                    merged[key] = value
        return merged

    def _enrich_soccer_fallback(
        self,
        *,
        competition: str,
        target_date: datetime,
        home_team: str,
        away_team: str,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {
            'home_table': {},
            'away_table': {},
            'home_injuries': None,
            'away_injuries': None,
            'has_injuries': False,
            'sources': [],
            'h2h': None,
        }

        if self._api_football.enabled:
            api_data = self._api_football.enrich_soccer_match(
                competition_code=competition,
                target_date=target_date,
                home_team=home_team,
                away_team=away_team,
            )
            merged['sources'].extend(api_data.get('sources') or [])
            merged['home_table'] = {
                'position': api_data.get('home_table_position'),
                'points': api_data.get('home_points'),
                'form_index': api_data.get('home_form_index'),
            }
            merged['away_table'] = {
                'position': api_data.get('away_table_position'),
                'points': api_data.get('away_points'),
                'form_index': api_data.get('away_form_index'),
            }
            merged['home_injuries'] = api_data.get('home_injuries')
            merged['away_injuries'] = api_data.get('away_injuries')
            merged['has_injuries'] = bool(api_data.get('has_injuries'))
            merged['h2h'] = api_data.get('h2h')

        if self._football_data.enabled:
            fd_data = self._football_data.enrich_soccer_match(
                competition_code=competition,
                target_date=target_date,
                home_team=home_team,
                away_team=away_team,
            )
            merged['sources'].extend(fd_data.get('sources') or [])
            merged['home_table'] = self._merge_table_data(
                current=merged.get('home_table') or {},
                fallback={
                    'position': fd_data.get('home_table_position'),
                    'points': fd_data.get('home_points'),
                    'form_index': fd_data.get('home_form_index'),
                },
            )
            merged['away_table'] = self._merge_table_data(
                current=merged.get('away_table') or {},
                fallback={
                    'position': fd_data.get('away_table_position'),
                    'points': fd_data.get('away_points'),
                    'form_index': fd_data.get('away_form_index'),
                },
            )

        return merged

    def _get_soccer_h2h_fallback(
        self,
        *,
        competition: str,
        target_date: datetime,
        home_team: str,
        away_team: str,
    ) -> tuple[float, float, float, int] | None:
        enriched = self._enrich_soccer_fallback(
            competition=competition,
            target_date=target_date,
            home_team=home_team,
            away_team=away_team,
        )
        h2h = enriched.get('h2h')
        if isinstance(h2h, (list, tuple)) and len(h2h) == 3:
            return float(h2h[0]), float(h2h[1]), float(h2h[2]), 0
        return None

    def _completeness_score(
        self,
        *,
        sport: Sport,
        standings_complete: bool,
        injuries_complete: bool,
        has_h2h: bool,
    ) -> float:
        if sport == Sport.BASKETBALL:
            score = 0.5
            score += 0.2 if standings_complete else 0
            score += 0.15 if injuries_complete else 0
            score += 0.15 if has_h2h else 0
            return round(max(0.3, min(1.0, score)), 4)

        score = 0.45
        score += 0.25 if standings_complete else 0
        score += 0.15 if injuries_complete else 0
        score += 0.15 if has_h2h else 0
        return round(max(0.25, min(1.0, score)), 4)

    def _confidence_penalty_from_completeness(self, completeness: float) -> float:
        if completeness >= 0.95:
            return 0.0
        if completeness >= 0.85:
            return 0.03
        if completeness >= 0.75:
            return 0.07
        if completeness >= 0.65:
            return 0.11
        return 0.15

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

    def _get_team_injuries(self, *, sport: Sport, competition_code: str, team_id: str | None) -> tuple[int, bool]:
        if not team_id:
            return 0, False

        cache_key = (sport.value, competition_code, team_id)
        if cache_key in self._injury_cache:
            return self._injury_cache[cache_key]

        if sport == Sport.SOCCER:
            url = f'https://site.api.espn.com/apis/site/v2/sports/soccer/{competition_code}/teams/{team_id}'
        else:
            url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/{competition_code}/teams/{team_id}'

        count = 0
        available = False
        try:
            with httpx.Client(timeout=12) as client:
                response = client.get(url)
                response.raise_for_status()
            payload = response.json()
            available = True
            count += self._count_injuries(payload.get('injuries') or [])
            for athlete in payload.get('athletes') or []:
                if isinstance(athlete, dict):
                    count += self._count_injuries(athlete.get('injuries') or [])
        except Exception:
            count = 0
            available = False

        count = min(12, max(0, count))
        self._injury_cache[cache_key] = (count, available)
        return count, available

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

    def _event_timezone_for_sport(self, sport: Sport) -> ZoneInfo:
        configured = (
            self._settings.soccer_event_timezone
            if sport == Sport.SOCCER
            else self._settings.basketball_event_timezone
        )
        try:
            return ZoneInfo(configured)
        except Exception:
            return ZoneInfo('UTC')

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
                    competition_code='stub',
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
