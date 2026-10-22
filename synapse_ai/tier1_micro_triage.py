"""
synapse_ai/tier1_micro_triage.py
--------------------------------
Sub-millisecond Micro-Triage Filter (<0.2ms latency).
Dual-path architecture combining:
1. Exact Signature Bloom Filter (instant pass for known benign, instant P0 alert for known fatal).
2. Fast Tree Ensemble (Isolation Forest) for statistical outlier scoring on novel signatures.
"""

import hashlib
import time
from typing import Dict, List, Optional, Set, Tuple, Any
import numpy as np
from sklearn.ensemble import IsolationForest

from synapse_stream.sessionizer import SessionWindow


class SignatureBloomFilter:
    """
    High-speed deterministic signature matcher.
    Simulates a partitioned Bloom filter with bitwise hashing and dynamic TTLs.
    """
    def __init__(self, capacity: int = 100000):
        self.capacity = capacity
        self.known_benign_signatures: Set[str] = set()
        self.known_fatal_signatures: Set[str] = set()
        self.transient_signatures: Dict[str, float] = {} # Signature -> Expiry Timestamp (Tier-4 feedback)

    @staticmethod
    def compute_signature(service: str, masked_message: str) -> str:
        payload = f"{service}::{masked_message}".encode("utf-8")
        return hashlib.md5(payload).hexdigest()[:16]

    def register_benign(self, service: str, masked_message: str):
        sig = self.compute_signature(service, masked_message)
        self.known_benign_signatures.add(sig)

    def register_fatal(self, service: str, masked_message: str):
        sig = self.compute_signature(service, masked_message)
        self.known_fatal_signatures.add(sig)

    def inject_transient_suppression(self, signature: str, ttl_seconds: float = 900.0):
        """Tier-4 feedback injection: suppresses duplicate ongoing incident escalations."""
        self.transient_signatures[signature] = time.time() + ttl_seconds

    def check(self, service: str, masked_message: str) -> str:
        """
        Returns triage status:
        - 'KNOWN_BENIGN': Skip AI evaluation entirely.
        - 'KNOWN_FATAL': Immediate P0 dispatch, bypass AI.
        - 'TRANSIENT_SUPPRESSED': Ongoing diagnosed incident, suppress escalation.
        - 'UNKNOWN_PATTERN': Proceed to statistical ensemble / Tier-2.
        """
        sig = self.compute_signature(service, masked_message)

        # Check transient deduplication cache first (Tier-4)
        if sig in self.transient_signatures:
            if time.time() < self.transient_signatures[sig]:
                return "TRANSIENT_SUPPRESSED"
            else:
                del self.transient_signatures[sig]

        if sig in self.known_benign_signatures:
            return "KNOWN_BENIGN"
        if sig in self.known_fatal_signatures:
            return "KNOWN_FATAL"
        return "UNKNOWN_PATTERN"


class Tier1MicroTriage:
    """
    Tier-1 Micro-Triage engine:
    Processes up to 100,000 logs/second with < 0.2ms latency.
    """
    def __init__(self, contamination: float = 0.05):
        self.bloom = SignatureBloomFilter()
        self.isolation_forest = IsolationForest(
            n_estimators=50,
            contamination=contamination,
            random_state=42,
            n_jobs=-1
        )
        self._is_fitted = False
        self._init_default_signatures()

    def _init_default_signatures(self):
        # Register standard benign patterns
        benign_samples = [
            ("auth-service", "User <NUM> logged in successfully"),
            ("auth-service", "User <NUM> logged out"),
            ("auth-service", "Access token refreshed for user <NUM>"),
            ("auth-service", "New account created for user <NUM>"),
            ("post-service", "Post <UUID> fetched successfully"),
            ("post-service", "Feed refreshed for user <NUM>"),
            ("notification-service", "Push notification delivered to device <NUM>"),
            ("notification-service", "Email digest queued for user <NUM>"),
        ]
        for srv, msg in benign_samples:
            self.bloom.register_benign(srv, msg)

        # Register deterministic fatal signatures (Instant P0)
        fatal_samples = [
            ("auth-service", "Database connection failed during authentication"),
            ("post-service", "Disk volume full /var/log/posts"),
            ("notification-service", "Out of memory: Kill process"),
        ]
        for srv, msg in fatal_samples:
            self.bloom.register_fatal(srv, msg)

    def warm_up_ensemble(self, normal_feature_vectors: List[List[float]]):
        """Fits the Isolation Forest on known normal operational vectors."""
        X = np.array(normal_feature_vectors)
        if len(X) < 10:
            # Generate synthetic baseline if few samples
            synthetic = np.random.normal(loc=0.0, scale=0.1, size=(50, 16))
            X = np.vstack([X, synthetic]) if len(X) > 0 else synthetic
        self.isolation_forest.fit(X)
        self._is_fitted = True

    def evaluate_log(self, raw_log: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Fast per-log evaluation (sub-millisecond).
        """
        t0 = time.perf_counter()
        srv = raw_log.get("service", "unknown")
        masked = raw_log.get("masked_message", raw_log.get("message", ""))

        status = self.bloom.check(srv, masked)
        latency_ms = (time.perf_counter() - t0) * 1000.0

        metadata = {
            "tier": 1,
            "status": status,
            "signature": SignatureBloomFilter.compute_signature(srv, masked),
            "latency_ms": latency_ms
        }
        return status, metadata

    def evaluate_window(self, window: SessionWindow) -> Tuple[bool, float, Dict[str, Any]]:
        """
        Evaluates an aggregated session window.
        Returns: (is_anomalous, outlier_score, metadata)
        """
        t0 = time.perf_counter()
        features = np.array(window.get_features()).reshape(1, -1)

        if not self._is_fitted:
            # Fallback heuristic if not yet trained: check error density
            error_count = sum(1 for l in window.logs if str(l.get("level", "")).upper() == "ERROR")
            is_anomaly = error_count > 0
            score = 1.0 if is_anomaly else 0.0
        else:
            pred = self.isolation_forest.predict(features)[0] # -1 for anomaly, 1 for normal
            score = -self.isolation_forest.score_samples(features)[0] # higher = more anomalous
            is_anomaly = (pred == -1)

        latency_ms = (time.perf_counter() - t0) * 1000.0
        metadata = {
            "tier": 1,
            "latency_ms": latency_ms,
            "window_size": window.size,
            "outlier_score": float(score)
        }
        return is_anomaly, float(score), metadata
