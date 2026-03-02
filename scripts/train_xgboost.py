from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from random import Random

import numpy as np
import xgboost as xgb


FEATURE_NAMES = [
    'h2h_home_win_rate',
    'h2h_draw_rate',
    'h2h_away_win_rate',
    'home_form_index',
    'away_form_index',
    'urgency_score',
    'volatility_index',
    'injury_impact',
    'fatigue_level',
    'weather_impact',
    'home_edge',
]

# 0=home_win,1=draw,2=away_win,3=over_05,4=over_15,5=home+1.5,6=away+1.5,7=home+8.5,8=away+8.5
TARGET_COL = 'target_class'
CLASS_COUNT = 9


def load_dataset(path: Path) -> tuple[np.ndarray, np.ndarray]:
    rows: list[list[float]] = []
    labels: list[int] = []
    with path.open('r', encoding='utf-8', newline='') as handle:
        reader = csv.DictReader(handle)
        for record in reader:
            try:
                row = [float(record[name]) for name in FEATURE_NAMES]
                label = int(record[TARGET_COL])
            except Exception:
                continue
            if not (0 <= label < CLASS_COUNT):
                continue
            rows.append(row)
            labels.append(label)
    if not rows:
        raise ValueError('Dataset has no valid rows')
    return np.array(rows, dtype=float), np.array(labels, dtype=int)


def build_bootstrap_dataset(samples: int = 6000) -> tuple[np.ndarray, np.ndarray]:
    rng = Random(42)
    rows: list[list[float]] = []
    labels: list[int] = []
    for _ in range(samples):
        home_form = rng.uniform(0.3, 0.9)
        away_form = rng.uniform(0.25, 0.82)
        h2h_home = min(0.9, max(0.1, home_form * rng.uniform(0.8, 1.1)))
        h2h_draw = rng.uniform(0.05, 0.3)
        h2h_away = max(0.03, 1 - h2h_home - h2h_draw)
        urgency = rng.uniform(0.2, 1.0)
        volatility = rng.uniform(0.1, 1.0)
        injury = rng.uniform(0.0, 0.7)
        fatigue = rng.uniform(0.0, 0.7)
        weather = rng.uniform(0.0, 0.6)
        home_edge = home_form - away_form

        row = [
            h2h_home,
            h2h_draw,
            h2h_away,
            home_form,
            away_form,
            urgency,
            volatility,
            injury,
            fatigue,
            weather,
            home_edge,
        ]
        rows.append(row)

        if volatility > 0.72:
            label = 5
        elif home_edge > 0.22 and volatility < 0.45:
            label = 0
        elif urgency > 0.72 and volatility < 0.55:
            label = 3
        elif home_edge < -0.15:
            label = 6
        else:
            label = 4
        labels.append(label)

    return np.array(rows, dtype=float), np.array(labels, dtype=int)


def train(x: np.ndarray, y: np.ndarray) -> xgb.Booster:
    dm = xgb.DMatrix(x, label=y, feature_names=FEATURE_NAMES)
    params = {
        'objective': 'multi:softprob',
        'num_class': CLASS_COUNT,
        'eval_metric': 'mlogloss',
        'eta': 0.05,
        'max_depth': 6,
        'subsample': 0.9,
        'colsample_bytree': 0.9,
        'seed': 42,
    }
    return xgb.train(params=params, dtrain=dm, num_boost_round=220)


def main() -> None:
    parser = argparse.ArgumentParser(description='Train Rolley XGBoost model')
    parser.add_argument('--dataset', type=str, default='', help='CSV file with feature columns + target_class')
    parser.add_argument('--output', type=str, default='models/rolley_xgb_v1.json', help='Output model path')
    parser.add_argument('--version', type=str, default='xgb-v1')
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.dataset:
        x, y = load_dataset(Path(args.dataset))
    else:
        x, y = build_bootstrap_dataset()

    model = train(x, y)
    model.save_model(str(output_path))

    metadata = {
        'model_version': args.version,
        'feature_names': FEATURE_NAMES,
        'target_mapping': {
            '0': 'home_win',
            '1': 'draw',
            '2': 'away_win',
            '3': 'over_05',
            '4': 'over_15',
            '5': 'handicap_home_plus_15',
            '6': 'handicap_away_plus_15',
            '7': 'basketball_home_plus_85',
            '8': 'basketball_away_plus_85',
        },
        'rows': int(x.shape[0]),
    }
    output_path.with_suffix('.meta.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    print(f'Trained model saved to {output_path}')
    print(f'Metadata saved to {output_path.with_suffix(".meta.json")}')


if __name__ == '__main__':
    main()
