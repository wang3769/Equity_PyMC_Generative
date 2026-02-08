from __future__ import annotations
import hashlib
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

MODEL_NAME = "ProsusAI/finbert"

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

class FinBertScorer:
    def __init__(self, model_name: str = MODEL_NAME, device: str | None = None):
        self.model_name = model_name
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = device

        self.tok = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
        self.model.eval()

        # ProsusAI/finbert labels are typically: negative, neutral, positive
        # We'll read id2label to be safe.
        self.id2label = self.model.config.id2label

    @torch.no_grad()
    def score_texts(self, texts: list[str], max_length: int = 128):
        enc = self.tok(
            texts,
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt",
        ).to(self.device)

        logits = self.model(**enc).logits
        probs = torch.softmax(logits, dim=-1).detach().cpu()

        # Map label -> column index robustly
        label2id = {v.lower(): k for k, v in self.id2label.items()}
        i_neg = label2id.get("negative")
        i_neu = label2id.get("neutral")
        i_pos = label2id.get("positive")

        p_neg = probs[:, i_neg].numpy()
        p_neu = probs[:, i_neu].numpy()
        p_pos = probs[:, i_pos].numpy()

        score = p_pos - p_neg  # [-1, 1]
        return p_pos, p_neg, p_neu, score
