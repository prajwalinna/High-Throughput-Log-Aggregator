"""
synapse_ai/tier4_distillation.py
--------------------------------
Tier-4 Closed-Loop Active Distillation & Dynamic Incident Deduplication.
Eliminates repetitive LLM invocations during ongoing failure storms by synthesizing
transient signatures into Tier-1 Bloom filter and embedding prototypes into Tier-2.
"""

import time
from typing import Dict, Any, Optional

from synapse_ai.tier1_micro_triage import Tier1MicroTriage, SignatureBloomFilter
from synapse_ai.tier2_slm_gate import Tier2SLMGate


class Tier4DistillationLoop:
    """
    Coordinates closed-loop feedback and active distillation.
    """
    def __init__(
        self,
        tier1: Tier1MicroTriage,
        tier2: Tier2SLMGate,
        default_ttl_seconds: float = 900.0
    ):
        self.tier1 = tier1
        self.tier2 = tier2
        self.default_ttl = default_ttl_seconds
        
        # Incident deduplication metrics
        self.stats = {
            "registered_diagnoses": 0,
            "suppressed_duplicate_logs": 0,
            "saved_llm_tokens": 0,
            "estimated_dollars_saved": 0.0
        }
        self.active_incidents: Dict[str, Dict[str, Any]] = {}

    def register_diagnosis(
        self,
        diagnosis: Dict[str, Any],
        current_masked_message: Optional[str] = None,
        ttl_seconds: Optional[float] = None
    ) -> str:
        """
        Receives Tier-3 RCA diagnosis and synthesizes transient suppression filters.
        """
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
        sig = diagnosis.get("transient_signature", "")
        inc_id = diagnosis.get("incident_id", "INC-UNKNOWN")
        srv = diagnosis.get("focal_service", "unknown")

        # 1. Inject into Tier-1 Bloom Filter
        # Register both masked logs and raw messages in the focal window
        dossier = diagnosis.get("dossier", {})
        focal_masked = dossier.get("recent_masked_logs", [])
        focal_raw = dossier.get("recent_focal_logs", [])
        
        for log_text in focal_masked + focal_raw:
            exact_sig = SignatureBloomFilter.compute_signature(srv, log_text)
            self.tier1.bloom.inject_transient_suppression(exact_sig, ttl_seconds=ttl)

        if current_masked_message:
            curr_sig = SignatureBloomFilter.compute_signature(srv, current_masked_message)
            self.tier1.bloom.inject_transient_suppression(curr_sig, ttl_seconds=ttl)

        # Also register the generic signature
        gen_sig = SignatureBloomFilter.compute_signature(srv, sig)
        self.tier1.bloom.inject_transient_suppression(gen_sig, ttl_seconds=ttl)

        # 2. Store active incident descriptor
        self.active_incidents[inc_id] = {
            "diagnosis": diagnosis,
            "expires_at": time.time() + ttl,
            "suppressed_count": 0
        }
        self.stats["registered_diagnoses"] += 1

        print(f"[Tier-4 Distillation] Injected transient suppression for {inc_id} ({sig}) with TTL={ttl}s.")
        return sig

    def record_suppression(self, incident_id: Optional[str] = None):
        """Records a duplicate log prevented from escalating to Tier-3."""
        self.stats["suppressed_duplicate_logs"] += 1
        # Approx. 1500 tokens per full RCA context dossier
        self.stats["saved_llm_tokens"] += 1500
        # Approx. $0.002 per 1k tokens on production LLMs
        self.stats["estimated_dollars_saved"] = (self.stats["saved_llm_tokens"] / 1000.0) * 0.002

        if incident_id and incident_id in self.active_incidents:
            self.active_incidents[incident_id]["suppressed_count"] += 1
