"""
synapse_stream/sessionizer.py
-----------------------------
High-throughput log stream normalization, dynamic variable masking,
and temporal-session sliding window aggregator.
"""

import re
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any

# Compiled high-performance regex patterns for variable masking
RE_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
RE_UUID = re.compile(r"\b[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\b")
RE_HEX = re.compile(r"\b0x[0-9a-fA-F]+\b")
RE_TIMESTAMP = re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?\b")
RE_PATH = re.compile(r"(/[\w\.-]+)+")
RE_DIGITS = re.compile(r"\b\d+\b")


def mask_log_message(message: str) -> str:
    """
    Normalizes dynamic variable tokens into abstract placeholders.
    Converts variable values (IPs, UUIDs, IDs, filepaths) into invariant templates.
    """
    if not message:
        return ""
    masked = RE_TIMESTAMP.sub("<TIME>", message)
    masked = RE_UUID.sub("<UUID>", masked)
    masked = RE_IPV4.sub("<IP>", masked)
    masked = RE_HEX.sub("<HEX>", masked)
    masked = RE_PATH.sub("<PATH>", masked)
    masked = RE_DIGITS.sub("<NUM>", masked)
    return masked


class SessionWindow:
    """
    Represents an aggregated temporal session window of log events for a service.
    """
    def __init__(self, service_name: str, window_id: str):
        self.service_name = service_name
        self.window_id = window_id
        self.logs: List[Dict[str, Any]] = []
        self.start_time: float = time.time()
        self.last_update_time: float = self.start_time

    def add_log(self, log_entry: Dict[str, Any]):
        self.logs.append(log_entry)
        self.last_update_time = time.time()

    @property
    def size(self) -> int:
        return len(self.logs)

    def get_aggregated_text(self) -> str:
        """Returns concatenated masked templates in chronological order."""
        return " [SEP] ".join(l.get("masked_message", "") for l in self.logs[-10:])

    def get_features(self) -> List[float]:
        """
        Extracts a 16-dimensional statistical feature vector for Tier-1 evaluation:
        [log_count, error_count, warn_count, avg_len, max_len, digit_ratio, 
         unique_event_ratio, time_span, arrival_rate, ...]
        """
        count = len(self.logs)
        if count == 0:
            return [0.0] * 16

        error_cnt = sum(1 for l in self.logs if str(l.get("level", "")).upper() == "ERROR")
        warn_cnt = sum(1 for l in self.logs if str(l.get("level", "")).upper() == "WARNING")
        info_cnt = sum(1 for l in self.logs if str(l.get("level", "")).upper() == "INFO")
        
        lens = [len(l.get("message", "")) for l in self.logs]
        avg_len = sum(lens) / count
        max_len = max(lens)
        
        unique_events = len(set(l.get("event", "UNKNOWN") for l in self.logs))
        time_span = max(0.001, self.last_update_time - self.start_time)
        rate = count / time_span
        
        # 16-dim statistical feature sketch
        features = [
            float(count),
            float(error_cnt),
            float(warn_cnt),
            float(info_cnt),
            float(error_cnt / count),
            float(warn_cnt / count),
            float(avg_len),
            float(max_len),
            float(unique_events / count),
            float(time_span),
            float(rate),
            float(1.0 if error_cnt > 0 else 0.0),
            float(1.0 if "timeout" in self.get_aggregated_text().lower() else 0.0),
            float(1.0 if "failed" in self.get_aggregated_text().lower() else 0.0),
            float(1.0 if "exception" in self.get_aggregated_text().lower() else 0.0),
            float(1.0 if "connection" in self.get_aggregated_text().lower() else 0.0),
        ]
        return features


class StreamSessionizer:
    """
    Maintains sliding temporal windows across distributed services.
    """
    def __init__(self, window_duration: float = 5.0, max_window_size: int = 50):
        self.window_duration = window_duration
        self.max_window_size = max_window_size
        self.windows: Dict[str, SessionWindow] = {}
        self._window_counter = 0

    def ingest(self, raw_log: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[SessionWindow]]:
        """
        Masks the log and attaches it to the current service window.
        Returns (enriched_log, expired_window_if_any).
        """
        service = raw_log.get("service", "unknown-service")
        raw_msg = raw_log.get("message", "")
        raw_log["masked_message"] = mask_log_message(raw_msg)
        raw_log["ingest_timestamp"] = time.time()

        if service not in self.windows:
            self._window_counter += 1
            self.windows[service] = SessionWindow(service, f"win-{service}-{self._window_counter}")

        window = self.windows[service]
        window.add_log(raw_log)

        # Check if window expired (by duration or max capacity)
        now = time.time()
        expired_window = None
        if (now - window.start_time >= self.window_duration) or (window.size >= self.max_window_size):
            expired_window = window
            self._window_counter += 1
            self.windows[service] = SessionWindow(service, f"win-{service}-{self._window_counter}")

        return raw_log, expired_window
