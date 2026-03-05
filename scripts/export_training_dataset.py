from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from sqlalchemy import select

from app.models import MatchHistory
from app.schemas import Sport
from app.storage import SessionLocal, init_db


FEATURE_NAMES = [
    "h2h_home_win_rate",
    "h2h_draw_rate",
    "h2h_away_win_rate",
    "home_form_index",
    "away_form_index",
    "urgency_score",
    "volatility_index",
    "injury_impact",
    "fatigue_level",
    "weather_impact",
    "home_edge",
    "h2h_sample_size",
    "home_recent5_scored_rate",
    "away_recent5_scored_rate",
    "home_recent5_goal_diff",
    "away_recent5_goal_diff",
    "home_recent5_opponent_strength",
    "away_recent5_opponent_strength",
]


@dataclass
class TeamGame:
    kick_off_utc: datetime
    goals_for: int
    goals_against: int
    opponent_team: str
    is_home: bool


def normalize_team_name(name: str) -> str:
    normalized = re.sub(r'[^a-z0-9]+', ' ', name.lower()).strip()
    return re.sub(r'\s+', ' ', normalized)


def normalize_date_token(value: str) -> str:
    token = value.strip()
    if not token:
        return ''
    if 'T' in token:
        token = token.split('T', 1)[0]
    return token


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def recent_games(history: list[TeamGame], lookback: int) -> list[TeamGame]:
    if lookback <= 0:
        return history[:]
    return history[-lookback:]


def compute_form_index(games: Iterable[TeamGame], sport: Sport) -> float:
    games = list(games)
    if not games:
        return 0.5
    points = 0.0
    max_points = 0.0
    for game in games:
        if sport == Sport.SOCCER:
            max_points += 3
            if game.goals_for > game.goals_against:
                points += 3
            elif game.goals_for == game.goals_against:
                points += 1
        else:
            max_points += 1
            if game.goals_for > game.goals_against:
                points += 1
    if max_points <= 0:
        return 0.5
    return clamp(points / max_points, 0.05, 0.95)


def compute_volatility(games: Iterable[TeamGame], sport: Sport) -> float:
    games = list(games)
    if not games:
        return 0.5
    avg_abs_margin = sum(abs(g.goals_for - g.goals_against) for g in games) / len(games)
    normalizer = 5.0 if sport == Sport.SOCCER else 15.0
    return clamp(avg_abs_margin / normalizer, 0.05, 0.95)


def compute_fatigue(games: Iterable[TeamGame], reference: datetime) -> float:
    games = list(games)
    if not games:
        return 0.1
    window_start = reference - timedelta(days=7)
    recent_count = sum(1 for game in games if game.kick_off_utc >= window_start)
    return clamp(recent_count / 7.0, 0.05, 0.95)


def compute_recent5_profile(
    *,
    sport: Sport,
    games: list[TeamGame],
    team_history: dict[tuple[str, str], list[TeamGame]],
    lookback: int,
) -> dict[str, float]:
    if not games:
        return {
            "scored_rate": 0.5,
            "goal_diff": 0.0,
            "opponent_strength": 0.5,
        }

    sample = recent_games(games, lookback)
    scored = sum(1 for game in sample if game.goals_for > 0)
    goal_diff = sum(game.goals_for - game.goals_against for game in sample) / len(sample)

    strength_values: list[float] = []
    for game in sample:
        key = (sport.value, game.opponent_team)
        opp_games = recent_games(team_history.get(key, []), 10)
        strength_values.append(compute_form_index(opp_games, sport))

    opponent_strength = sum(strength_values) / len(strength_values) if strength_values else 0.5
    return {
        "scored_rate": clamp(scored / len(sample), 0.0, 1.0),
        "goal_diff": goal_diff,
        "opponent_strength": clamp(opponent_strength, 0.0, 1.0),
    }


def compute_h2h(
    h2h_rows: list[tuple[str, str, int, int]],
    current_home_team: str,
    sport: Sport,
) -> tuple[float, float, float, int]:
    if not h2h_rows:
        # fallback to neutral h2h when no prior pair history exists
        if sport == Sport.BASKETBALL:
            return 0.5, 0.01, 0.49, 0
        return 0.42, 0.18, 0.40, 0

    home_wins = 0.0
    away_wins = 0.0
    draws = 0.0
    ordered = list(reversed(h2h_rows))  # newest first
    for idx, (home_team, away_team, home_score, away_score) in enumerate(ordered):
        weight = 1.0 / (1.0 + idx * 0.35)
        if home_score == away_score:
            draws += weight
            continue
        winner = home_team if home_score > away_score else away_team
        if winner == current_home_team:
            home_wins += weight
        else:
            away_wins += weight

    total = max(0.01, home_wins + away_wins + draws)
    if sport == Sport.BASKETBALL:
        draw_rate = 0.01
        home_rate = clamp(home_wins / total, 0.05, 0.95)
        away_rate = clamp(away_wins / total, 0.04, 0.95)
        remap = max(home_rate + away_rate, 0.01)
        return home_rate / remap, draw_rate, away_rate / remap, len(h2h_rows)
    return (
        clamp(home_wins / total, 0.08, 0.9),
        clamp(draws / total, 0.05, 0.45),
        clamp(away_wins / total, 0.05, 0.9),
        len(h2h_rows),
    )


def target_class_for_match(sport: Sport, home_score: int, away_score: int) -> int:
    if sport == Sport.BASKETBALL:
        return 7 if home_score >= away_score else 8

    # Soccer: favor safer markets first, then directional outcomes.
    total_goals = home_score + away_score
    margin = home_score - away_score
    if total_goals >= 3:
        return 4  # over 1.5
    if total_goals >= 1:
        return 3  # over 0.5
    if margin >= 2:
        return 5  # home +1.5
    if margin <= -2:
        return 6  # away +1.5
    if margin > 0:
        return 0
    if margin == 0:
        return 1
    return 2


def map_market_selection_to_target(*, sport: Sport, market: str, selection: str) -> int | None:
    market_key = market.strip().upper()
    selection_key = selection.strip().upper()

    if sport == Sport.BASKETBALL:
        if market_key in {'ALT_SPREAD', 'HANDICAP'}:
            if selection_key.startswith('HOME +'):
                return 7
            if selection_key.startswith('AWAY +'):
                return 8
        return None

    if market_key == 'TOTAL_GOALS':
        if 'OVER 1.5' in selection_key:
            return 4
        if 'OVER 0.5' in selection_key:
            return 3
    if market_key == 'HANDICAP':
        if selection_key.startswith('HOME +'):
            return 5
        if selection_key.startswith('AWAY +'):
            return 6
    if market_key == 'DOUBLE_CHANCE':
        if selection_key == '1X':
            return 0
        if selection_key == 'X2':
            return 2
    if market_key == 'MATCH_RESULT':
        if selection_key == 'HOME':
            return 0
        if selection_key in {'DRAW', 'X'}:
            return 1
        if selection_key == 'AWAY':
            return 2
    return None


def load_analyst_labels(path: Path) -> dict[tuple[str, str, str, str], int]:
    if not path.exists():
        return {}

    labels: dict[tuple[str, str, str, str], int] = {}
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            sport_token = str(raw.get('sport') or '').strip().upper()
            if sport_token not in {Sport.SOCCER.value, Sport.BASKETBALL.value}:
                continue
            sport = Sport(sport_token)

            date_token = normalize_date_token(
                str(raw.get('match_date') or '').strip()
                or str(raw.get('pick_date') or '').strip()
                or str(raw.get('date') or '').strip()
            )
            home_team = str(raw.get('home_team') or '').strip()
            away_team = str(raw.get('away_team') or '').strip()
            if not date_token or not home_team or not away_team:
                continue

            target_class: int | None = None
            target_raw = str(raw.get('target_class') or '').strip()
            if target_raw:
                try:
                    target_class = int(target_raw)
                except Exception:
                    target_class = None

            if target_class is None:
                market = str(raw.get('market') or raw.get('analyst_market') or '').strip()
                selection = str(raw.get('selection') or raw.get('analyst_selection') or '').strip()
                if market and selection:
                    target_class = map_market_selection_to_target(
                        sport=sport,
                        market=market,
                        selection=selection,
                    )

            if target_class is None or target_class < 0:
                continue

            key = (
                date_token,
                sport.value,
                normalize_team_name(home_team),
                normalize_team_name(away_team),
            )
            labels[key] = target_class

    return labels


def main() -> None:
    parser = argparse.ArgumentParser(description="Export historical training CSV from rolley_match_history.")
    parser.add_argument("--output", default="data/historical_training.csv", help="Output CSV path")
    parser.add_argument("--lookback", type=int, default=12, help="Recent match window per team")
    parser.add_argument("--min-team-games", type=int, default=5, help="Minimum prior games per team")
    parser.add_argument("--sports", default="SOCCER,BASKETBALL", help="Comma-separated sports subset")
    parser.add_argument(
        "--labels-path",
        default="data/analyst_labels.csv",
        help="Optional analyst labels CSV path (target_class or market/selection overrides)",
    )
    args = parser.parse_args()

    selected_sports = {token.strip().upper() for token in args.sports.split(",") if token.strip()}
    if not selected_sports:
        selected_sports = {Sport.SOCCER.value, Sport.BASKETBALL.value}

    labels_path = Path(args.labels_path)
    analyst_labels = load_analyst_labels(labels_path)
    labels_applied = 0

    init_db()
    db = SessionLocal()
    try:
        rows = db.scalars(select(MatchHistory).order_by(MatchHistory.kick_off_utc.asc())).all()
    finally:
        db.close()

    team_history: dict[tuple[str, str], list[TeamGame]] = defaultdict(list)
    h2h_history: dict[tuple[str, str, str], list[tuple[str, str, int, int]]] = defaultdict(list)

    output_rows: list[dict[str, float | int]] = []

    for row in rows:
        if row.sport not in selected_sports:
            continue

        sport = Sport(row.sport)
        home_key = (row.sport, row.home_team)
        away_key = (row.sport, row.away_team)
        pair_key = (row.sport, min(row.home_team, row.away_team), max(row.home_team, row.away_team))

        home_prior = recent_games(team_history[home_key], args.lookback)
        away_prior = recent_games(team_history[away_key], args.lookback)
        pair_prior = h2h_history[pair_key][-args.lookback :] if args.lookback > 0 else h2h_history[pair_key]

        if len(home_prior) >= args.min_team_games and len(away_prior) >= args.min_team_games:
            home_form = compute_form_index(home_prior, sport)
            away_form = compute_form_index(away_prior, sport)
            home_vol = compute_volatility(home_prior, sport)
            away_vol = compute_volatility(away_prior, sport)
            fatigue = max(
                compute_fatigue(home_prior, row.kick_off_utc),
                compute_fatigue(away_prior, row.kick_off_utc),
            )
            volatility_index = clamp(((home_vol + away_vol) / 2) * 10, 0.5, 10.0)
            urgency_score = clamp((4.0 + abs(home_form - away_form) * 8.0), 0.5, 10.0)
            injury_impact = clamp((fatigue * 5.0 + ((home_vol + away_vol) / 2) * 2.0), 0.0, 10.0)
            weather_impact = clamp((2.0 + home_vol * 1.5) if sport == Sport.SOCCER else 1.0, 0.0, 10.0)

            h2h_home, h2h_draw, h2h_away, h2h_sample = compute_h2h(pair_prior, row.home_team, sport)
            home_edge = home_form - away_form
            target_class = target_class_for_match(sport, row.home_score, row.away_score)
            home_recent = compute_recent5_profile(
                sport=sport,
                games=home_prior,
                team_history=team_history,
                lookback=5,
            )
            away_recent = compute_recent5_profile(
                sport=sport,
                games=away_prior,
                team_history=team_history,
                lookback=5,
            )
            label_key = (
                row.kick_off_utc.date().isoformat(),
                sport.value,
                normalize_team_name(row.home_team),
                normalize_team_name(row.away_team),
            )
            if label_key in analyst_labels:
                target_class = analyst_labels[label_key]
                labels_applied += 1

            output_rows.append(
                {
                    "h2h_home_win_rate": round(h2h_home, 6),
                    "h2h_draw_rate": round(h2h_draw, 6),
                    "h2h_away_win_rate": round(h2h_away, 6),
                    "home_form_index": round(home_form, 6),
                    "away_form_index": round(away_form, 6),
                    "urgency_score": round(urgency_score / 10, 6),
                    "volatility_index": round(volatility_index / 10, 6),
                    "injury_impact": round(injury_impact / 10, 6),
                    "fatigue_level": round(fatigue, 6),
                    "weather_impact": round(weather_impact / 10, 6),
                    "home_edge": round(home_edge, 6),
                    "h2h_sample_size": round(min(1.0, h2h_sample / 20.0), 6),
                    "home_recent5_scored_rate": round(home_recent["scored_rate"], 6),
                    "away_recent5_scored_rate": round(away_recent["scored_rate"], 6),
                    "home_recent5_goal_diff": round(clamp((home_recent["goal_diff"] + 5.0) / 10.0, 0.0, 1.0), 6),
                    "away_recent5_goal_diff": round(clamp((away_recent["goal_diff"] + 5.0) / 10.0, 0.0, 1.0), 6),
                    "home_recent5_opponent_strength": round(home_recent["opponent_strength"], 6),
                    "away_recent5_opponent_strength": round(away_recent["opponent_strength"], 6),
                    "target_class": target_class,
                }
            )

        team_history[home_key].append(
            TeamGame(
                kick_off_utc=row.kick_off_utc,
                goals_for=row.home_score,
                goals_against=row.away_score,
                opponent_team=row.away_team,
                is_home=True,
            )
        )
        team_history[away_key].append(
            TeamGame(
                kick_off_utc=row.kick_off_utc,
                goals_for=row.away_score,
                goals_against=row.home_score,
                opponent_team=row.home_team,
                is_home=False,
            )
        )
        h2h_history[pair_key].append((row.home_team, row.away_team, row.home_score, row.away_score))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[*FEATURE_NAMES, "target_class"])
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Exported {len(output_rows)} rows to {output_path}")
    if analyst_labels:
        print(f"Applied {labels_applied} analyst label override(s) from {labels_path}")


if __name__ == "__main__":
    main()
