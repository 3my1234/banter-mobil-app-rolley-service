from datetime import datetime, timedelta, timezone
from random import Random
from ..schemas import MatchCandidate, Sport


class SportsDataProvider:
    """Replace this with real provider integration (Sportradar/Api-Football/Balldontlie)."""

    def __init__(self) -> None:
        self._rng = Random(42)

    def fetch_matches(self, *, sport: Sport, target_date: datetime) -> list[MatchCandidate]:
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
                    external_match_id=f'{sport.value}-{target_date.date()}-{i+1}',
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
