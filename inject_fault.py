"""
inject_fault.py
---------------
Interactive CLI tool for real-time chaos fault injection & traffic load scaling.
Allows engineers, reviewers, and operators to inject custom failures or traffic spikes
into the live Kafka stream and observe SynapseLog's real-time AI triage.
"""

import sys
import os
import time
import uuid
import json
import argparse
from typing import Dict, Any

from kafka import KafkaProducer

BROKER_URL = "localhost:9092"
TOPIC = "logs"


def get_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=[BROKER_URL],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"[Error] Could not connect to Kafka at {BROKER_URL}: {e}")
        print("Please ensure Docker Kafka is running (run 'run_pipeline.bat' or 'python synapse_orchestrator.py').")
        sys.exit(1)


def send_log(producer: KafkaProducer, service: str, level: str, event: str, message: str, extra: Dict[str, Any] = None):
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "service": service,
        "level": level,
        "event": event,
        "message": message,
        "request_id": str(uuid.uuid4()),
        **(extra or {})
    }
    producer.send(TOPIC, payload)


def inject_db_pool_exhaustion(producer: KafkaProducer, count: int = 15):
    print(f"\n[Injecting] Database Connection Pool Exhaustion ({count} logs)...")
    for step in range(count):
        srv = "auth-service" if step % 2 == 0 else "post-service"
        if srv == "auth-service":
            msg = f"Database connection pool exhausted! Timeout waiting for available connection (pool_size=50, active=50, waiter_count={step * 3})"
            event = "DB_POOL_EXHAUSTED"
        else:
            msg = f"Failed to authenticate user {1000 + step}: Upstream auth-service timed out after 3000ms"
            event = "UPSTREAM_TIMEOUT"
        send_log(producer, srv, "ERROR", event, msg, {"response_time_ms": 3000 + (step * 50)})
        time.sleep(0.02)
    producer.flush()
    print("[Success] Injected database pool fault burst into Kafka!")


def inject_zero_day_panic(producer: KafkaProducer, count: int = 15):
    print(f"\n[Injecting] Zero-Day Distributed Consensus Split-Brain Panic ({count} logs)...")
    for step in range(count):
        srv = "auth-service" if step % 2 == 0 else "post-service"
        if srv == "auth-service":
            msg = f"Distributed Raft quorum partition: fatal split-brain deadlock on auth_shard_{step % 2}; circular wait panic"
            event = "ZERO_DAY_CONSENSUS_PANIC"
        else:
            msg = f"Protobuf schema corruption: unknown wire type on payload stream; fatal deserialization abort at byte offset {step * 64}"
            event = "CORRUPT_PAYLOAD_ABORT"
        send_log(producer, srv, "FATAL", event, msg, {"response_time_ms": 5000})
        time.sleep(0.02)
    producer.flush()
    print("[Success] Injected novel zero-day consensus fault into Kafka!")


def inject_oom_killer(producer: KafkaProducer, count: int = 10):
    print(f"\n[Injecting] Memory Leak / OOM Killer Alert ({count} logs)...")
    for step in range(count):
        msg = f"Out of memory: Kill process {3000 + step} (worker) score 950 and sacrifice child; memory usage 3.98GB/4.0GB"
        send_log(producer, "notification-service", "FATAL", "OOM_KILL", msg, {"memory_usage_mb": 3980})
        time.sleep(0.02)
    producer.flush()
    print("[Success] Injected OOM killer fault into Kafka!")


def inject_custom_error(producer: KafkaProducer, service: str, level: str, message: str, count: int = 5):
    print(f"\n[Injecting] Custom Fault ({count} logs) for [{service}]: {message}...")
    for _ in range(count):
        send_log(producer, service, level, "CUSTOM_FAULT", message)
        time.sleep(0.02)
    producer.flush()
    print("[Success] Injected custom error into Kafka!")


def inject_traffic_spike(producer: KafkaProducer, count: int = 200):
    print(f"\n[Injecting] High-Velocity Traffic Spike ({count} normal logs)...")
    services = ["auth-service", "post-service", "notification-service"]
    for i in range(count):
        srv = services[i % len(services)]
        send_log(producer, srv, "INFO", "NORMAL_TRANSACTION", f"User {1000 + (i % 500)} processed transaction #{i} successfully", {"latency_ms": 12})
    producer.flush()
    print(f"[Success] Flushed {count} high-velocity logs to Kafka!")


def interactive_menu():
    producer = get_producer()
    while True:
        print("\n" + "=" * 65)
        print("  SYNAPSELOG LIVE FAULT & TRAFFIC INJECTOR")
        print("=" * 65)
        print("  [1] Inject Database Pool Exhaustion Burst (Known Operational Fault)")
        print("  [2] Inject Raft Consensus Split-Brain Panic (Novel Zero-Day Fault)")
        print("  [3] Inject Memory Leak / OOM Killer Outage")
        print("  [4] Inject High-Velocity Traffic Spike (200 Normal Logs)")
        print("  [5] Inject Custom Error Message")
        print("  [0] Exit")
        print("-" * 65)

        choice = input("Select an option (0-5): ").strip()
        if choice == "1":
            count = int(input("Enter log count (default 15): ") or 15)
            inject_db_pool_exhaustion(producer, count=count)
        elif choice == "2":
            count = int(input("Enter log count (default 15): ") or 15)
            inject_zero_day_panic(producer, count=count)
        elif choice == "3":
            count = int(input("Enter log count (default 10): ") or 10)
            inject_oom_killer(producer, count=count)
        elif choice == "4":
            count = int(input("Enter count (default 200): ") or 200)
            inject_traffic_spike(producer, count=count)
        elif choice == "5":
            srv = input("Enter service name (default 'auth-service'): ").strip() or "auth-service"
            lvl = input("Enter level [ERROR/FATAL] (default 'ERROR'): ").strip() or "ERROR"
            msg = input("Enter error message: ").strip()
            if not msg:
                print("Error message cannot be empty!")
                continue
            count = int(input("Enter log count (default 5): ") or 5)
            inject_custom_error(producer, srv, lvl, msg, count=count)
        elif choice == "0":
            print("Exiting fault injector.")
            break
        else:
            print("Invalid option, please choose between 0 and 5.")


def main():
    parser = argparse.ArgumentParser(description="SynapseLog Fault & Traffic Injector")
    parser.add_argument("--fault", choices=["db_pool", "zero_day", "oom"], help="Pre-packaged fault type")
    parser.add_argument("--custom", type=str, help="Custom error message string")
    parser.add_argument("--service", type=str, default="auth-service", help="Target microservice")
    parser.add_argument("--level", type=str, default="ERROR", help="Log level (ERROR, FATAL, WARNING)")
    parser.add_argument("--traffic", choices=["spike"], help="Inject traffic spike")
    parser.add_argument("--count", type=int, default=15, help="Number of logs to burst")

    args = parser.parse_args()

    # If no flags passed, open interactive menu
    if len(sys.argv) == 1:
        interactive_menu()
        return

    producer = get_producer()
    if args.fault == "db_pool":
        inject_db_pool_exhaustion(producer, count=args.count)
    elif args.fault == "zero_day":
        inject_zero_day_panic(producer, count=args.count)
    elif args.fault == "oom":
        inject_oom_killer(producer, count=args.count)
    elif args.traffic == "spike":
        inject_traffic_spike(producer, count=args.count)
    elif args.custom:
        inject_custom_error(producer, args.service, args.level, args.custom, count=args.count)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
