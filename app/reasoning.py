from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .config import get_settings
from .schemas import MatchCandidate, MatchContext, ProbabilitySet, Sport

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None

try:
    import xgboost as xgb
except Exception:  # pragma: no cover
    xgb = None


@dataclass
class ModelResult:
    probabilities: ProbabilitySet
    model_version: str


class ProbabilityEngine:
    """XGBoost-backed probability engine with deterministic fallback."""

    def __init__(self) -> None:
        settings = get_settings()
        self._model = None
        self._model_version = 'heuristic-v1'
        self._feature_names = [item.strip() for item in settings.xgboost_feature_names.split(',') if item.strip()]
        if settings.xgboost_enabled:
            self._load_model(settings.xgboost_model_path)

    def _load_model(self, path: str) -> None:
        if not xgb or not np:
            return

        model_path = Path(path)
        if not model_path.exists():
            return

        try:
            booster = xgb.Booster()
            booster.load_model(str(model_path))
            self._model = booster

            metadata_path = model_path.with_suffix('.meta.json')
            if metadata_path.exists():
                metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
                self._model_version = str(metadata.get('model_version') or model_path.stem)
                configured = metadata.get('feature_names')
                if isinstance(configured, list) and configured:
                    self._feature_names = [str(item) for item in configured]
            else:
                self._model_version = model_path.stem
        except Exception:
            self._model = None
            self._model_version = 'heuristic-v1'

    def predict(self, match: MatchCandidate, context: MatchContext) -> ModelResult:
        if self._model is not None and np is not None:
            try:
                return ModelResult(
                    probabilities=self._xgboost_predict(match, context),
                    model_version=self._model_version,
                )
            except Exception:
                pass
        return ModelResult(probabilities=self._heuristic_predict(match, context), model_version='heuristic-v1')

    def _feature_row(self, match: MatchCandidate, context: MatchContext) -> list[float]:
        home_edge = match.home_form_index - match.away_form_index
        values = {
            'h2h_home_win_rate': match.h2h_home_win_rate,
            'h2h_draw_rate': match.h2h_draw_rate,
            'h2h_away_win_rate': match.h2h_away_win_rate,
            'home_form_index': match.home_form_index,
            'away_form_index': match.away_form_index,
            'urgency_score': context.urgency_score / 10,
            'volatility_index': context.volatility_index / 10,
            'injury_impact': context.injury_impact / 10,
            'fatigue_level': context.fatigue_level / 10,
            'weather_impact': context.weather_impact / 10,
            'home_edge': home_edge,
        }
        return [float(values.get(name, 0.0)) for name in self._feature_names]

    def _xgboost_predict(self, match: MatchCandidate, context: MatchContext) -> ProbabilitySet:
        # Expected class order from training script:
        # 0=home_win,1=draw,2=away_win,3=over_05,4=over_15,5=home+1.5,6=away+1.5,7=home+8.5,8=away+8.5
        row = self._feature_row(match, context)
        dm = xgb.DMatrix(np.array([row], dtype=float), feature_names=self._feature_names)
        probs = self._model.predict(dm)
        if probs.ndim == 2:
            p = probs[0].tolist()
        else:
            p = probs.tolist()

        while len(p) < 9:
            p.append(0.5)

        home_win = max(0.03, min(0.95, float(p[0])))
        draw = 0.01 if match.sport == Sport.BASKETBALL else max(0.05, min(0.42, float(p[1])))
        away_win = max(0.03, min(0.92, float(p[2])))

        # Re-normalize 1X2 to avoid invalid sums.
        total = home_win + draw + away_win
        if total > 0:
            home_win, draw, away_win = home_win / total, draw / total, away_win / total

        over_05 = max(0.5, min(0.99, float(p[3])))
        over_15 = max(0.2, min(0.95, float(p[4])))
        handicap_home_plus_15 = max(0.45, min(0.99, float(p[5])))
        handicap_away_plus_15 = max(0.35, min(0.96, float(p[6])))
        basketball_home_plus_85 = max(0.45, min(0.99, float(p[7])))
        basketball_away_plus_85 = max(0.35, min(0.96, float(p[8])))

        return ProbabilitySet(
            home_win=home_win,
            draw=draw,
            away_win=away_win,
            over_05=over_05,
            over_15=over_15,
            double_chance_1x=max(home_win + draw, 0.5),
            double_chance_x2=max(away_win + draw, 0.35),
            handicap_home_plus_15=handicap_home_plus_15,
            handicap_away_plus_15=handicap_away_plus_15,
            basketball_home_plus_85=basketball_home_plus_85,
            basketball_away_plus_85=basketball_away_plus_85,
        )

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
    implied_odds: float
    rationale: str


class RolleyDecisionEngine:
    """Safety-first decision module (inclusive-set logic)."""

    def _to_safe_odds(self, confidence: float) -> float:
        # clamp daily stake multipliers to conservative range
        odds = 1.01 + max(0.0, min(0.08, (confidence - 0.55) * 0.22))
        return round(max(1.01, min(1.09, odds)), 4)

    def decide(self, *, sport: Sport, probabilities: ProbabilitySet, context: MatchContext) -> Decision:
        if sport == Sport.SOCCER:
            return self._decide_soccer(probabilities, context)
        return self._decide_basketball(probabilities, context)

    def _decide_soccer(self, p: ProbabilitySet, c: MatchContext) -> Decision:
        if c.volatility_index >= 7.0 or c.urgency_score <= 4.0:
            confidence = round(p.handicap_home_plus_15, 4)
            return Decision(
                market='HANDICAP',
                selection='Home +1.5',
                confidence=confidence,
                implied_odds=self._to_safe_odds(confidence),
                rationale='High volatility or low urgency detected; inclusive handicap protects against upset outcomes.',
            )

        if p.home_win >= 0.72 and c.volatility_index <= 4.5:
            confidence = round(p.double_chance_1x, 4)
            return Decision(
                market='DOUBLE_CHANCE',
                selection='1X',
                confidence=confidence,
                implied_odds=self._to_safe_odds(confidence),
                rationale='Strong home edge with controlled volatility; 1X covers both win and draw set.',
            )

        if p.over_05 >= 0.78:
            confidence = round(p.over_05, 4)
            return Decision(
                market='TOTAL_GOALS',
                selection='Over 0.5',
                confidence=confidence,
                implied_odds=self._to_safe_odds(confidence),
                rationale='Goal floor is statistically robust; over 0.5 sits in safest outcome set.',
            )

        confidence = round(p.over_15, 4)
        return Decision(
            market='TOTAL_GOALS',
            selection='Over 1.5',
            confidence=confidence,
            implied_odds=self._to_safe_odds(confidence),
            rationale='Balanced scenario; over 1.5 selected as middle-risk inclusive option.',
        )

    def _decide_basketball(self, p: ProbabilitySet, c: MatchContext) -> Decision:
        if c.volatility_index >= 6.5:
            confidence = round(max(p.basketball_home_plus_85 - 0.05, 0.52), 4)
            return Decision(
                market='ALT_SPREAD',
                selection='Home +10.5',
                confidence=confidence,
                implied_odds=self._to_safe_odds(confidence),
                rationale='Volatility elevated; wider spread used to absorb late-game variance.',
            )

        if p.home_win >= 0.68:
            confidence = round(p.basketball_home_plus_85, 4)
            return Decision(
                market='ALT_SPREAD',
                selection='Home +8.5',
                confidence=confidence,
                implied_odds=self._to_safe_odds(confidence),
                rationale='Home advantage is clear; +8.5 keeps result in safe middle set.',
            )

        confidence = round(p.basketball_away_plus_85, 4)
        return Decision(
            market='ALT_SPREAD',
            selection='Away +8.5',
            confidence=confidence,
            implied_odds=self._to_safe_odds(confidence),
            rationale='Away value with safety margin chosen for downside protection.',
        )
