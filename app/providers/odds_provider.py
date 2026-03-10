from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

import httpx

from ..config import get_settings
from ..schemas import Sport


@dataclass(frozen=True)
class PickOddsQuote:
    price: float
    bookmaker: str | None
    event_id: str | None


class OddsApiProvider:
    def __init__(self) -> None:
        settings = get_settings()
        self._enabled = bool(
            settings.odds_api_enabled
            and settings.odds_sanity_filter_enabled
            and settings.odds_api_key
        )
        self._api_key = settings.odds_api_key or ''
        self._base_url = settings.odds_api_base_url.rstrip('/')
        self._bookmakers = [
            item.strip()
            for item in (settings.odds_api_bookmakers or '').split(',')
            if item.strip()
        ]
        self._window_hours = max(1, int(settings.odds_api_match_window_hours))

    @property
    def enabled(self) -> bool:
        return self._enabled

    def quote_for_pick(
        self,
        *,
        sport: Sport,
        home_team: str,
        away_team: str,
        kick_off_utc: datetime,
        market: str,
        selection: str,
    ) -> PickOddsQuote | None:
        if not self._enabled:
            return None

        event = self._find_event(
            sport=sport,
            home_team=home_team,
            away_team=away_team,
            kick_off_utc=kick_off_utc,
        )
        if not event:
            return None

        prices = self._extract_candidate_prices(
            bookmakers_payload=event.get('bookmakers') or {},
            market=market,
            selection=selection,
        )
        if not prices:
            return None

        market_price = float(median(prices))
        return PickOddsQuote(
            price=round(market_price, 4),
            bookmaker=self._bookmakers[0] if self._bookmakers else None,
            event_id=str(event.get('id') or ''),
        )

    def _find_event(
        self,
        *,
        sport: Sport,
        home_team: str,
        away_team: str,
        kick_off_utc: datetime,
    ) -> dict[str, Any] | None:
        query_sport = self._sport_name(sport)
        starts_at = (kick_off_utc - timedelta(hours=self._window_hours)).astimezone(timezone.utc)
        ends_at = (kick_off_utc + timedelta(hours=self._window_hours)).astimezone(timezone.utc)
        params: dict[str, Any] = {
            'apiKey': self._api_key,
            'sport': query_sport,
            'startAt': starts_at.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
            'endAt': ends_at.replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        }
        if self._bookmakers:
            params['bookmaker'] = self._bookmakers[0]

        try:
            with httpx.Client(timeout=15) as client:
                response = client.get(f'{self._base_url}/events', params=params)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            return None

        data = payload.get('data') if isinstance(payload, dict) else payload
        if not isinstance(data, list):
            return None

        home_key = self._normalize(home_team)
        away_key = self._normalize(away_team)

        best_match: tuple[float, dict[str, Any]] | None = None
        for item in data:
            event_home = self._normalize(str(item.get('home') or item.get('home_team') or ''))
            event_away = self._normalize(str(item.get('away') or item.get('away_team') or ''))
            if not event_home or not event_away:
                continue
            score = self._team_match_score(home_key, away_key, event_home, event_away)
            if score <= 0:
                continue
            if best_match is None or score > best_match[0]:
                best_match = (score, item)

        return best_match[1] if best_match else None

    def _extract_candidate_prices(
        self,
        *,
        bookmakers_payload: dict[str, Any],
        market: str,
        selection: str,
    ) -> list[float]:
        prices: list[float] = []
        for bookmaker_name, entries in bookmakers_payload.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_market = str(entry.get('name') or '').strip()
                if not self._market_matches(entry_market, market):
                    continue
                matched = self._price_from_market_entry(entry=entry, market=market, selection=selection)
                if matched is not None:
                    prices.append(matched)
        return prices

    def _price_from_market_entry(self, *, entry: dict[str, Any], market: str, selection: str) -> float | None:
        market_upper = market.upper()
        selection_upper = selection.upper().strip()
        if market_upper == 'TOTAL_GOALS':
            side, line = self._parse_total_goals_selection(selection_upper)
            if side is None or line is None:
                return None
            for key, value in entry.items():
                if self._normalize(str(key)) != self._normalize(side):
                    continue
                price = self._extract_price_for_line(value, target_line=line)
                if price is not None:
                    return price
            return None

        if market_upper == 'DOUBLE_CHANCE':
            for key, value in entry.items():
                if self._normalize(str(key)) != self._normalize(selection_upper):
                    continue
                price = self._as_price(value)
                if price is not None:
                    return price
            return None

        if market_upper in {'HANDICAP', 'ALT_SPREAD'}:
            side, line = self._parse_handicap_selection(selection_upper)
            if side is None or line is None:
                return None
            side_key = 'home' if side == 'HOME' else 'away'
            for key, value in entry.items():
                if self._normalize(str(key)) != side_key:
                    continue
                price = self._extract_price_for_line(value, target_line=line)
                if price is not None:
                    return price
            return None

        return None

    def _extract_price_for_line(self, payload: Any, *, target_line: float) -> float | None:
        line_key = f'{target_line:g}'
        if isinstance(payload, dict):
            direct = payload.get(line_key)
            if direct is not None:
                return self._as_price(direct)
            for key, value in payload.items():
                try:
                    if abs(float(str(key)) - target_line) < 0.001:
                        return self._as_price(value)
                except Exception:
                    continue
        return self._as_price(payload)

    def _as_price(self, payload: Any) -> float | None:
        if isinstance(payload, (int, float)):
            price = float(payload)
            return price if price >= 1.0 else None
        if isinstance(payload, str):
            try:
                price = float(payload)
            except ValueError:
                return None
            return price if price >= 1.0 else None
        return None

    def _market_matches(self, entry_market: str, market: str) -> bool:
        entry_key = self._normalize(entry_market)
        market_key = market.upper()
        if market_key == 'TOTAL_GOALS':
            return entry_key in {'totals', 'totalgoals', 'goaltotals'}
        if market_key == 'DOUBLE_CHANCE':
            return entry_key in {'doublechance', 'doublechance3way'}
        if market_key in {'HANDICAP', 'ALT_SPREAD'}:
            return entry_key in {'spread', 'handicap', 'asianhandicap'}
        return False

    def _parse_total_goals_selection(self, selection: str) -> tuple[str | None, float | None]:
        upper = selection.upper()
        if upper.startswith('OVER '):
            return 'Over', self._extract_last_number(upper)
        if upper.startswith('UNDER '):
            return 'Under', self._extract_last_number(upper)
        return None, None

    def _parse_handicap_selection(self, selection: str) -> tuple[str | None, float | None]:
        upper = selection.upper()
        if upper.startswith('HOME '):
            return 'HOME', abs(self._extract_last_number(upper) or 0.0)
        if upper.startswith('AWAY '):
            return 'AWAY', abs(self._extract_last_number(upper) or 0.0)
        return None, None

    def _extract_last_number(self, value: str) -> float | None:
        digits = ''.join(ch if (ch.isdigit() or ch in '.-') else ' ' for ch in value)
        parts = [item for item in digits.split() if item]
        if not parts:
            return None
        try:
            return float(parts[-1])
        except ValueError:
            return None

    def _team_match_score(self, home_key: str, away_key: str, event_home: str, event_away: str) -> float:
        direct = 1.0 if home_key == event_home and away_key == event_away else 0.0
        partial = (
            self._token_overlap(home_key, event_home)
            + self._token_overlap(away_key, event_away)
        ) / 2.0
        return max(direct, partial)

    def _token_overlap(self, expected: str, actual: str) -> float:
        expected_tokens = set(expected.split())
        actual_tokens = set(actual.split())
        if not expected_tokens or not actual_tokens:
            return 0.0
        overlap = len(expected_tokens & actual_tokens)
        return overlap / max(len(expected_tokens), len(actual_tokens))

    def _normalize(self, value: str) -> str:
        normalized = unicodedata.normalize('NFKD', value or '')
        ascii_value = normalized.encode('ascii', 'ignore').decode('ascii')
        cleaned = ''.join(ch.lower() if ch.isalnum() else ' ' for ch in ascii_value)
        return ' '.join(cleaned.split())

    def _sport_name(self, sport: Sport) -> str:
        if sport == Sport.BASKETBALL:
            return 'basketball'
        return 'football'
