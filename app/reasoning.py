from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .schemas import MatchCandidate, MatchContext, ProbabilitySet, Sport


@dataclass
class ModelResult:
    probabilities: ProbabilitySet
    model_version: str


class ProbabilityEngine:
    """XGBoost-compatible probability engine.

    Replace `_heuristic_predict` with real model inference once trained model is ready.
    """

    def __init__(self) -> None:
        self._model = None
        self._model_version = 'heuristic-v1'

    def predict(self, match: MatchCandidate, context: MatchContext) -> ModelResult:
        return ModelResult(probabilities=self._heuristic_predict(match, context), model_version=self._model_version)

    def _heuristic_predict(self, match: MatchCandidate, context: MatchContext) -> ProbabilitySet:
        home_bias = max(0.05, min(0.9, (match.home_form_index + match.h2h_home_win_rate) / 2))
        volatility_penalty = max(0.0, min(0.35, context.volatility_index / 30))
        urgency_boost = max(0.0, min(0.2, context.urgency_score / 60))

        home_win = max(0.05, min(0.92, home_bias + urgency_boost - volatility_penalty))
        draw = 0.16 if match.sport == Sport.SOCCER else 0.01
        away_win = max(0.03, min(0.8, 1 - home_win - draw))

        over_05 = max(0.55, min(0.96, 0.72 + context.urgency_score / 40 - context.weather_impact / 30))
        over_15 = max(0.28, min(0.88, 0.5 + context.urgency_score / 50 - context.volatility_index / 60))

        return ProbabilitySet(
            home_win=home_win,
            draw=draw,
            away_win=away_win,
            over_05=over_05,
            over_15=over_15,
            double_chance_1x=max(home_win + draw, 0.5),
            double_chance_x2=max(away_win + draw, 0.35),
            handicap_home_plus_15=max(0.6, min(0.98, 0.73 + urgency_boost - volatility_penalty / 2)),
            handicap_away_plus_15=max(0.45, min(0.92, 0.58 + volatility_penalty / 1.5)),
            basketball_home_plus_85=max(0.58, min(0.97, 0.75 + urgency_boost - volatility_penalty / 2)),
            basketball_away_plus_85=max(0.44, min(0.91, 0.6 + volatility_penalty / 2.2)),
        )


@dataclass
class Decision:
    market: str
    selection: str
    confidence: float
    rationale: str


class RolleyDecisionEngine:
    """Safety-first decision module (inclusive-set logic)."""

    def decide(self, *, sport: Sport, probabilities: ProbabilitySet, context: MatchContext) -> Decision:
        if sport == Sport.SOCCER:
            return self._decide_soccer(probabilities, context)
        return self._decide_basketball(probabilities, context)

    def _decide_soccer(self, p: ProbabilitySet, c: MatchContext) -> Decision:
        if c.volatility_index >= 7.0 or c.urgency_score <= 4.0:
            return Decision(
                market='HANDICAP',
                selection='Home +1.5',
                confidence=round(p.handicap_home_plus_15, 4),
                rationale='High volatility or low urgency detected; inclusive handicap protects against upset outcomes.',
            )

        if p.home_win >= 0.72 and c.volatility_index <= 4.5:
            return Decision(
                market='DOUBLE_CHANCE',
                selection='1X',
                confidence=round(p.double_chance_1x, 4),
                rationale='Strong home edge with controlled volatility; 1X covers both win and draw set.',
            )

        if p.over_05 >= 0.78:
            return Decision(
                market='TOTAL_GOALS',
                selection='Over 0.5',
                confidence=round(p.over_05, 4),
                rationale='Goal floor is statistically robust; over 0.5 sits in safest outcome set.',
            )

        return Decision(
            market='TOTAL_GOALS',
            selection='Over 1.5',
            confidence=round(p.over_15, 4),
            rationale='Balanced scenario; over 1.5 selected as middle-risk inclusive option.',
        )

    def _decide_basketball(self, p: ProbabilitySet, c: MatchContext) -> Decision:
        if c.volatility_index >= 6.5:
            return Decision(
                market='ALT_SPREAD',
                selection='Home +10.5',
                confidence=round(max(p.basketball_home_plus_85 - 0.05, 0.52), 4),
                rationale='Volatility elevated; wider spread used to absorb late-game variance.',
            )

        if p.home_win >= 0.68:
            return Decision(
                market='ALT_SPREAD',
                selection='Home +8.5',
                confidence=round(p.basketball_home_plus_85, 4),
                rationale='Home advantage is clear; +8.5 keeps result in safe middle set.',
            )

        return Decision(
            market='ALT_SPREAD',
            selection='Away +8.5',
            confidence=round(p.basketball_away_plus_85, 4),
            rationale='Away value with safety margin chosen for downside protection.',
        )
