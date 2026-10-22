"""
synapse_ai/pipeline_orchestrator.py
-----------------------------------
Unified SynapseLog Pipeline Orchestrator.
Coordinates end-to-end log triage from high-throughput streaming through Tiers 1-4.
"""

import time
from typing import Dict, List, Optional, Tuple, Any

from synapse_stream.sessionizer import StreamSessionizer, SessionWindow
from synapse_ai.tier1_micro_triage import Tier1MicroTriage
from synapse_ai.tier2_slm_gate import Tier2SLMGate
from synapse_ai.tier3_llm_rca import GenerativeLLMReasoner
from synapse_ai.tier4_distillation import Tier4DistillationLoop


class SynapsePipeline:
    """
    End-to-End Hierarchical AI Log Triage Pipeline.
    """
    def __init__(
        self,
        config_path: Optional[str] = None,
        topology_path: Optional[str] = None,
        device: str = "cuda",
        tau_confident: float = 0.45
    ):
        print("=" * 60)
        print("  Initializing SynapseLog Production Pipeline Engine")
        print("=" * 60)
        
        # 1. Ingestion Sessionizer
        self.sessionizer = StreamSessionizer(window_duration=5.0, max_window_size=50)

        # 2. Tier-1 Micro-Triage Filter (<0.2ms)
        self.tier1 = Tier1MicroTriage(contamination=0.03)

        # 3. Tier-2 Quantized SLM with Epistemic Gating (CUDA on RTX 2050)
        import os
        model_target = "models/tier2_slm_calibrated" if os.path.exists("models/tier2_slm_calibrated") else "distilbert-base-uncased"
        self.tier2 = Tier2SLMGate(
            model_name=model_target,
            device=device,
            tau_confident=tau_confident
        )

        # 4. Tier-3 Generative LLM Causal RCA & Routing
        self.tier3 = GenerativeLLMReasoner(topology_path=topology_path)

        # 5. Tier-4 Closed-Loop Active Distillation
        self.tier4 = Tier4DistillationLoop(tier1=self.tier1, tier2=self.tier2)

        # Metrics Tracking
        self.metrics = {
            "total_ingested_logs": 0,
            "tier1_filtered_benign": 0,
            "tier1_instant_fatal": 0,
            "tier1_transient_suppressed": 0,
            "tier2_evaluated_sequences": 0,
            "tier2_resolved_locally": 0,
            "tier3_escalated_rca": 0,
            "total_latency_ms": 0.0,
            "latencies": []
        }

    def process_log(self, raw_log: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes end-to-end triage for a single arriving log event.
        Returns final structured triage verdict and telemetry.
        """
        t0 = time.perf_counter()
        self.metrics["total_ingested_logs"] += 1

        # Step 0: Ingestion & Dynamic Masking
        enriched_log, expired_window = self.sessionizer.ingest(raw_log)

        # Step 1: Tier-1 Micro-Triage Filter (<0.2ms)
        t1_status, t1_meta = self.tier1.evaluate_log(enriched_log)

        if t1_status == "KNOWN_BENIGN":
            self.metrics["tier1_filtered_benign"] += 1
            lat = (time.perf_counter() - t0) * 1000.0
            self._record_lat(lat)
            return {
                "verdict": "PASS_TO_ARCHIVE",
                "tier": 1,
                "reason": "Known benign signature matched in Bloom filter",
                "latency_ms": lat,
                "log": enriched_log
            }

        elif t1_status == "TRANSIENT_SUPPRESSED":
            self.metrics["tier1_transient_suppressed"] += 1
            self.tier4.record_suppression()
            lat = (time.perf_counter() - t0) * 1000.0
            self._record_lat(lat)
            return {
                "verdict": "SUPPRESSED_ONGOING_INCIDENT",
                "tier": 1,
                "reason": "Ongoing diagnosed failure suppressed by Tier-4 distillation filter",
                "latency_ms": lat,
                "log": enriched_log
            }

        elif t1_status == "KNOWN_FATAL":
            self.metrics["tier1_instant_fatal"] += 1
            lat = (time.perf_counter() - t0) * 1000.0
            self._record_lat(lat)
            return {
                "verdict": "INSTANT_P0_ALERT",
                "tier": 1,
                "reason": "Deterministic fatal signature detected. Rule alert fired.",
                "latency_ms": lat,
                "log": enriched_log
            }

        # Step 2: Tier-2 SLM Evaluation (Novel / Outlier pattern)
        self.metrics["tier2_evaluated_sequences"] += 1
        masked_text = enriched_log.get("masked_message", enriched_log.get("message", ""))
        t2_eval = self.tier2.evaluate_sequence(masked_text)

        if not t2_eval["escalate_to_llm"]:
            # SLM is confident! (Uncertainty < tau_confident)
            self.metrics["tier2_resolved_locally"] += 1
            lat = (time.perf_counter() - t0) * 1000.0
            self._record_lat(lat)
            return {
                "verdict": "RESOLVED_BY_SLM_LOCALLY",
                "tier": 2,
                "predicted_class": t2_eval["predicted_class"],
                "confidence": t2_eval["confidence"],
                "epistemic_uncertainty": t2_eval["epistemic_uncertainty"],
                "reason": f"SLM resolved with high certainty U(x)={t2_eval['epistemic_uncertainty']:.3f} < {self.tier2.tau_confident}",
                "latency_ms": lat,
                "log": enriched_log
            }

        # Step 3: Tier-3 Generative LLM Escalation (High Uncertainty U(x) >= tau_confident)
        self.metrics["tier3_escalated_rca"] += 1
        service = enriched_log.get("service", "unknown-service")
        current_window = self.sessionizer.windows.get(service)
        
        diagnosis = self.tier3.analyze_incident(service, current_window, t2_eval)

        # Step 4: Tier-4 Active Distillation Loop
        # Register diagnosis in Tier-4 to suppress future duplicate logs during this outage
        self.tier4.register_diagnosis(diagnosis, current_masked_message=masked_text)

        lat = (time.perf_counter() - t0) * 1000.0
        self._record_lat(lat)

        return {
            "verdict": "ESCALATED_LLM_RCA",
            "tier": 3,
            "incident_id": diagnosis["incident_id"],
            "root_cause_summary": diagnosis["root_cause_summary"],
            "blast_radius": diagnosis["blast_radius"],
            "routing_target": diagnosis["routing_target"],
            "oncall_pager": diagnosis["oncall_pager"],
            "mitigation_steps": diagnosis["mitigation_steps"],
            "transient_signature": diagnosis["transient_signature"],
            "latency_ms": lat,
            "log": enriched_log
        }

    def _record_lat(self, lat_ms: float):
        self.metrics["total_latency_ms"] += lat_ms
        self.metrics["latencies"].append(lat_ms)

    def get_summary_report(self) -> Dict[str, Any]:
        """Returns executive metrics summary for paper evaluation tables."""
        total = max(1, self.metrics["total_ingested_logs"])
        t1_total = (self.metrics["tier1_filtered_benign"] + 
                    self.metrics["tier1_instant_fatal"] + 
                    self.metrics["tier1_transient_suppressed"])
        
        lats = sorted(self.metrics["latencies"]) if self.metrics["latencies"] else [0.0]
        p50 = lats[int(len(lats) * 0.50)]
        p95 = lats[int(len(lats) * 0.95)]
        p99 = lats[min(len(lats) - 1, int(len(lats) * 0.99))]
        
        report = {
            "total_ingested_logs": total,
            "tier1_absorbed_ratio": (t1_total / total) * 100.0,
            "tier2_evaluated_count": self.metrics["tier2_evaluated_sequences"],
            "tier2_resolved_count": self.metrics["tier2_resolved_locally"],
            "tier3_escalated_count": self.metrics["tier3_escalated_rca"],
            "tier4_duplicate_suppression_count": self.tier4.stats["suppressed_duplicate_logs"],
            "saved_llm_tokens": self.tier4.stats["saved_llm_tokens"],
            "estimated_dollars_saved": self.tier4.stats["estimated_dollars_saved"],
            "latency_p50_ms": p50,
            "latency_p95_ms": p95,
            "latency_p99_ms": p99,
            "avg_latency_ms": sum(lats) / len(lats)
        }
        return report
