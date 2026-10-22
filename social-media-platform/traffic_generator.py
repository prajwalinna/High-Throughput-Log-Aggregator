"""
social-media-platform/traffic_generator.py
-----------------------------------------
High-throughput microservices traffic & chaos fault generator for SynapseLog.
Produces realistic JSON log streams into Kafka topic 'logs' from:
- auth-service
- post-service
- notification-service
"""

import os
import sys
import time
import uuid
import random
import json
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared.kafka_logger import KafkaFallbackHandler

SERVICE_NAMES = ["auth-service", "post-service", "notification-service"]


class MicroserviceTrafficEngine:
    def __init__(self, broker_url: str = "localhost:9092", topic: str = "logs"):
        self.handlers = {
            srv: KafkaFallbackHandler(srv, broker_url=broker_url, topic=topic)
            for srv in SERVICE_NAMES
        }
        print(f"[Traffic Generator] Initialized Kafka producers for: {SERVICE_NAMES}")

    def emit_log(self, service: str, level: str, event: str, message: str, extra: Dict[str, Any]):
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "service": service,
            "level": level,
            "event": event,
            "message": message,
            **extra
        }
        handler = self.handlers.get(service)
        if handler:
            # Emit directly via Kafka producer
            if handler.producer:
                try:
                    handler.producer.send(handler.topic, log_entry)
                except Exception:
                    handler._write_to_fallback(log_entry)
            else:
                handler._write_to_fallback(log_entry)

    def generate_normal_cycle(self, count: int = 100, delay_sec: float = 0.02):
        """Generates sustained normal operational traffic silently."""
        for i in range(count):
            srv = random.choice(SERVICE_NAMES)
            uid = random.randint(1000, 9999)
            extra = {
                "user_id": uid,
                "ip_address": f"192.168.1.{random.randint(2, 254)}",
                "device": random.choice(["iPhone 15", "Pixel 8", "MacBook Pro"]),
                "response_time_ms": random.randint(15, 120),
                "request_id": str(uuid.uuid4())
            }

            if srv == "auth-service":
                event, template = random.choice([
                    ("USER_LOGIN", f"User {uid} logged in successfully"),
                    ("TOKEN_REFRESH", f"Access token refreshed for user {uid}"),
                    ("USER_LOGOUT", f"User {uid} logged out")
                ])
            elif srv == "post-service":
                event, template = random.choice([
                    ("POST_FETCH", f"Post {uuid.uuid4()} fetched successfully"),
                    ("FEED_LOAD", f"Feed refreshed for user {uid}")
                ])
            else:
                event, template = random.choice([
                    ("PUSH_DELIVERED", f"Push notification delivered to device {uid}"),
                    ("EMAIL_QUEUED", f"Email digest queued for user {uid}")
                ])

            self.emit_log(srv, "INFO", event, template, extra)
            if delay_sec > 0:
                time.sleep(delay_sec)

    def inject_incident_burst(self, incident_type: str, burst_count: int = 30):
        """Simulates an ongoing outage storm."""
        print(f"\n[CHAOS FAULT INJECTION] Injecting incident: {incident_type} ({burst_count} logs)...")

        for step in range(burst_count):
            if incident_type == "db_pool_exhaustion":
                srv = "auth-service" if step % 2 == 0 else "post-service"
                if srv == "auth-service":
                    msg = f"Database connection pool exhausted! Timeout waiting for available connection (pool_size=50, active=50, waiter_count={step * 2})"
                    event = "DB_POOL_EXHAUSTED"
                else:
                    msg = f"Failed to authenticate user {random.randint(1000, 9999)}: Upstream auth-service timed out after 3000ms"
                    event = "UPSTREAM_TIMEOUT"
                level = "ERROR"

            elif incident_type == "zero_day_consensus":
                srv = "auth-service" if step % 2 == 0 else "post-service"
                if srv == "auth-service":
                    msg = f"Distributed Raft quorum partition: fatal split-brain deadlock on auth_shard_{step % 2}; circular wait panic"
                    event = "ZERO_DAY_CONSENSUS_PANIC"
                else:
                    msg = f"Protobuf schema corruption: unknown wire type on payload stream; fatal deserialization abort at byte offset {step * 64}"
                    event = "CORRUPT_PAYLOAD_ABORT"
                level = "FATAL"

            else:
                srv = "notification-service"
                msg = f"Out of memory: Kill process {random.randint(1000, 9000)} (worker) score 950 and sacrifice child"
                event = "OOM_KILL"
                level = "FATAL"

            extra = {
                "request_id": str(uuid.uuid4()),
                "response_time_ms": 3000 + (step * 50)
            }
            self.emit_log(srv, level, event, msg, extra)
            time.sleep(0.01) # Rapid burst


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "scenario"
    engine = MicroserviceTrafficEngine()

    if mode == "scenario":
        print("=" * 65)
        print("  RUNNING REAL-TIME MICROSERVICE MULTI-PHASE FAULT SCENARIO")
        print("=" * 65)
        # Phase 1: Normal traffic
        engine.generate_normal_cycle(count=50, delay_sec=0.03)

        # Phase 2: Known operational fault
        engine.inject_incident_burst("db_pool_exhaustion", burst_count=20)

        # Phase 3: Short normal pause
        engine.generate_normal_cycle(count=20, delay_sec=0.03)

        # Phase 4: Zero-Day Novel Outage
        engine.inject_incident_burst("zero_day_consensus", burst_count=25)

        # Phase 5: Recovery
        engine.generate_normal_cycle(count=30, delay_sec=0.03)
        print("\n[Traffic Generator] Multi-phase scenario completed!")

    elif mode == "continuous":
        print("[Traffic Generator] Running in continuous traffic generation mode...")
        while True:
            engine.generate_normal_cycle(count=30, delay_sec=0.05)
            if random.random() < 0.2:
                engine.inject_incident_burst(random.choice(["db_pool_exhaustion", "zero_day_consensus"]), burst_count=15)

    else:
        engine.generate_normal_cycle(count=int(mode), delay_sec=0.02)


if __name__ == "__main__":
    main()
