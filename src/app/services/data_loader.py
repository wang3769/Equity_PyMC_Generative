from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

SCORE_COLS_CORE = ["ticker", "dt", "mu_1d", "sigma", "z_score", "p_pos", "label"]
OPTIONAL_COLS = [
    "score_0_100",      # if you add it
    "top_contribs",     # if you add it
]

def load_scores(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing scores file: {path}")

    df = pd.read_csv(path)
    # Ensure required cols exist
    missing = [c for c in SCORE_COLS_CORE if c not in df.columns]
    if missing:
        raise ValueError(f"today_scores.csv missing columns: {missing}")

    # Keep everything, but guarantee core columns first
    cols = SCORE_COLS_CORE + [c for c in OPTIONAL_COLS if c in df.columns] + \
           [c for c in df.columns if c not in SCORE_COLS_CORE and c not in OPTIONAL_COLS]
    df = df[cols].copy()

    # Sort best first (higher z is “better” in your convention)
    df = df.sort_values("z_score", ascending=False).reset_index(drop=True)
    return df

def load_report(path: Path) -> dict:
    if not path.exists():
        return {
            "asof": None,
            "universe_size": None,
            "train_rows": None,
            "eval_days": None,
            "ic_mean": None,
            "ic_std": None,
            "ic_t": None,
            "notes": "model_report.json not found yet.",
        }
    with open(path, "r") as f:
        return json.load(f)
