from __future__ import annotations

import json
from datetime import datetime
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
        return (
            "You are Rolley Context Engine for sports risk analysis.\n"
            "Return JSON only with keys: urgency_score, volatility_index, injury_impact, fatigue_level, weather_impact.\n"
            "Each value must be number 0-10.\n"
            f'Match: {match.home_team} vs {match.away_team} ({match.league}) on {match.kick_off_utc.isoformat()}.'
        )

    def _fallback(self, match: MatchCandidate) -> MatchContext:
        form_gap = max(0.0, match.home_form_index - match.away_form_index)
        urgency = min(10.0, 4.8 + form_gap * 5.2)
        volatility = max(1.5, 7.8 - form_gap * 6.1)
        injury_impact = 2.8 + (datetime.utcnow().day % 3) * 0.6
        fatigue = 3.2
        weather = 1.0 if match.sport.value == 'BASKETBALL' else 2.5
        return MatchContext(
            urgency_score=round(urgency, 2),
            volatility_index=round(volatility, 2),
            injury_impact=round(injury_impact, 2),
            fatigue_level=round(fatigue, 2),
            weather_impact=round(weather, 2),
        )
