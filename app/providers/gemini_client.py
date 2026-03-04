from __future__ import annotations

import json
import httpx
from ..config import get_settings
from ..schemas import MatchCandidate, MatchContext


class GeminiContextClient:
    """Gemini client for contextual feature extraction.

    If GEMINI_API_KEY is missing or request fails, it falls back to deterministic heuristics.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._api_key = settings.gemini_api_key
        self._model = settings.gemini_model

    async def extract_context(self, match: MatchCandidate) -> MatchContext:
        if not self._api_key:
            return self._fallback(match)

        prompt = self._build_prompt(match)
        url = (
            f'https://generativelanguage.googleapis.com/v1beta/models/{self._model}:generateContent'
            f'?key={self._api_key}'
        )
        payload = {
            'contents': [
                {
                    'role': 'user',
                    'parts': [{'text': prompt}],
                }
            ],
            'generationConfig': {
                'temperature': 0.2,
                'maxOutputTokens': 300,
                'responseMimeType': 'application/json',
            },
        }

        try:
            async with httpx.AsyncClient(timeout=25) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()
            text = (
                data.get('candidates', [{}])[0]
                .get('content', {})
                .get('parts', [{}])[0]
                .get('text', '{}')
            )
            parsed = json.loads(text)
            return MatchContext(
                urgency_score=float(parsed.get('urgency_score', 5)),
                volatility_index=float(parsed.get('volatility_index', 5)),
                injury_impact=float(parsed.get('injury_impact', 3)),
                fatigue_level=float(parsed.get('fatigue_level', 3)),
                weather_impact=float(parsed.get('weather_impact', 1)),
            )
        except Exception:
            return self._fallback(match)

    def _build_prompt(self, match: MatchCandidate) -> str:
        standings_summary = (
            f'home_pos={match.home_table_position or "na"},away_pos={match.away_table_position or "na"},'
            f'home_points={match.home_points or "na"},away_points={match.away_points or "na"}'
        )
        injury_summary = f'home_injuries={match.home_injuries},away_injuries={match.away_injuries}'
        return (
            "You are Rolley Context Engine for sports risk analysis.\n"
            "Return JSON only with keys: urgency_score, volatility_index, injury_impact, fatigue_level, weather_impact.\n"
            "Each value must be number 0-10.\n"
            f'Match: {match.home_team} vs {match.away_team} ({match.league}) on {match.kick_off_utc.isoformat()}.\n'
            f'Standings: {standings_summary}\n'
            f'Injuries: {injury_summary}\n'
            f'H2H: home={match.h2h_home_win_rate:.3f},draw={match.h2h_draw_rate:.3f},away={match.h2h_away_win_rate:.3f}\n'
            f'Form: home={match.home_form_index:.3f},away={match.away_form_index:.3f}'
        )

    def _fallback(self, match: MatchCandidate) -> MatchContext:
        form_gap = max(0.0, match.home_form_index - match.away_form_index)
        table_gap = 0.0
        if match.home_table_position and match.away_table_position:
            table_gap = max(0.0, (match.away_table_position - match.home_table_position) / 20)
        urgency = min(10.0, 4.6 + form_gap * 4.8 + table_gap * 2.2)
        volatility = max(1.2, 7.7 - form_gap * 5.4 - table_gap * 1.4)
        injury_impact = min(10.0, 1.8 + (match.home_injuries + match.away_injuries) * 0.9)
        fatigue = 3.2
        weather = 1.0 if match.sport.value == 'BASKETBALL' else 2.5
        return MatchContext(
            urgency_score=round(urgency, 2),
            volatility_index=round(volatility, 2),
            injury_impact=round(injury_impact, 2),
            fatigue_level=round(fatigue, 2),
            weather_impact=round(weather, 2),
        )
