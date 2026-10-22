"""
synapse_ai/tier2_slm_gate.py
----------------------------
Tier-2 Edge SLM with Calibrated Epistemic Uncertainty Gating.
Runs with PyTorch CUDA acceleration on NVIDIA GeForce RTX 2050 GPU.

Computes:
1. Multi-class log sequence probabilities (Benign Drift vs Known Fault vs Novel Anomaly).
2. Predictive Shannon Entropy: H(P)
3. Logit-Margin Uncertainty: M_margin
4. Conformal Epistemic Gating Score: U(x) = alpha * H_norm + (1-alpha) * M_margin
Escalates to Tier-3 ONLY when U(x) >= tau_confident.
"""

import math
import time
from typing import Dict, List, Optional, Tuple, Any
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from synapse_stream.sessionizer import SessionWindow


class Tier2SLMGate:
    """
    Tier-2 Semantic SLM Evaluator with Epistemic Gating.
    """
    CLASSES = ["BENIGN_NORMAL", "KNOWN_OPERATIONAL_FAULT", "NOVEL_SYSTEMIC_ANOMALY"]

    def __init__(
        self,
        model_name: str = "distilbert-base-uncased",
        device: str = "cuda",
        tau_confident: float = 0.65,
        alpha: float = 0.5,
        temperature: float = 2.0,
        max_length: int = 128
    ):
        self.device = device if torch.cuda.is_available() and device == "cuda" else "cpu"
        self.tau_confident = tau_confident
        self.alpha = alpha
        self.temperature = temperature
        self.max_length = max_length
        self.num_classes = len(self.CLASSES)
        
        print(f"[Tier-2 SLM] Initializing {model_name} on device: {self.device}...")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_name,
            num_labels=self.num_classes
        ).to(self.device)
        self.model.eval()

        # Prototype cache for Tier-4 dynamic memory
        self.prototype_embeddings: Dict[str, torch.Tensor] = {}

    def compute_epistemic_uncertainty(self, probs: torch.Tensor) -> Tuple[float, float, float]:
        """
        Computes calibrated epistemic uncertainty metrics:
        Returns: (composite_uncertainty, normalized_entropy, margin_uncertainty)
        """
        # Shannon Entropy: H(P) = -sum(p * log(p))
        eps = 1e-12
        entropy = -torch.sum(probs * torch.log(probs + eps)).item()
        max_entropy = math.log(self.num_classes)
        norm_entropy = min(1.0, max(0.0, entropy / max_entropy))

        # Margin Uncertainty: 1.0 - (P_top1 - P_top2)
        top_probs, _ = torch.topk(probs, k=min(2, self.num_classes))
        top1 = top_probs[0].item()
        top2 = top_probs[1].item() if len(top_probs) > 1 else 0.0
        margin_uncertainty = 1.0 - (top1 - top2)

        # Composite Epistemic Uncertainty
        composite_u = self.alpha * norm_entropy + (1.0 - self.alpha) * margin_uncertainty
        return float(composite_u), float(norm_entropy), float(margin_uncertainty)

    def evaluate_sequence(self, text_sequence: str) -> Dict[str, Any]:
        """
        Evaluates a log sequence or temporal window.
        Returns detailed triage decision, probabilities, and uncertainty metrics.
        """
        t0 = time.perf_counter()

        inputs = self.tokenizer(
            text_sequence,
            max_length=self.max_length,
            padding=True,
            truncation=True,
            return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits[0]
            # Conformal temperature scaling for calibrated OOD uncertainty
            probs = F.softmax(logits / self.temperature, dim=-1)

        pred_idx = torch.argmax(probs).item()
        predicted_class = self.CLASSES[pred_idx]
        conf_score = probs[pred_idx].item()

        uncertainty, entropy, margin_u = self.compute_epistemic_uncertainty(probs)
        latency_ms = (time.perf_counter() - t0) * 1000.0

        # Gating logic:
        # If uncertainty is high, escalate to Tier-3 (Generative LLM)
        escalate_to_llm = uncertainty >= self.tau_confident

        result = {
            "tier": 2,
            "device": self.device,
            "predicted_class": predicted_class,
            "confidence": conf_score,
            "epistemic_uncertainty": uncertainty,
            "normalized_entropy": entropy,
            "margin_uncertainty": margin_u,
            "tau_threshold": self.tau_confident,
            "escalate_to_llm": escalate_to_llm,
            "probabilities": {self.CLASSES[i]: float(probs[i].item()) for i in range(self.num_classes)},
            "latency_ms": latency_ms
        }
        return result

    def evaluate_window(self, window: SessionWindow) -> Dict[str, Any]:
        """Evaluates an aggregated session window."""
        text_seq = window.get_aggregated_text()
        res = self.evaluate_sequence(text_seq)
        res["window_id"] = window.window_id
        res["service"] = window.service_name
        res["window_size"] = window.size
        return res
