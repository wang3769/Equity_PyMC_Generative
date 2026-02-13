from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class Settings:
    repo_root: Path = Path(__file__).resolve().parents[2]
    data_dir: Path = repo_root / "data"
    docs_dir: Path = repo_root / "docs"

    today_scores_csv: Path = data_dir / "today_scores.csv"
    model_report_json: Path = data_dir / "model_report.json"
    model_card_md: Path = docs_dir / "model_card.md"

settings = Settings()
