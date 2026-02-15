from __future__ import annotations

import os
import io
import json
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from azure.storage.blob import BlobServiceClient


@dataclass(frozen=True)
class BlobPaths:
    scores_csv: str = "scores/latest/today_scores.csv"
    report_json: str = "scores/latest/model_report.json"
    model_card_md: str = "docs/model_card.md"


def _get_container_name() -> str:
    return os.getenv("BLOB_CONTAINER", "equity-artifacts")


def _get_bsc() -> BlobServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise EnvironmentError("Missing AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn)


def _download_bytes(blob_path: str) -> bytes:
    bsc = _get_bsc()
    container = _get_container_name()
    bc = bsc.get_blob_client(container=container, blob=blob_path)
    return bc.download_blob().readall()


def load_scores_df(blob_path: str = BlobPaths.scores_csv) -> pd.DataFrame:
    data = _download_bytes(blob_path)
    return pd.read_csv(io.BytesIO(data))


def load_report_dict(blob_path: str = BlobPaths.report_json) -> dict:
    data = _download_bytes(blob_path)
    return json.loads(data.decode("utf-8"))


def load_model_card_md(blob_path: str = BlobPaths.model_card_md) -> str:
    data = _download_bytes(blob_path)
    return data.decode("utf-8")