"""
synapse_orchestrator.py
-----------------------
Master Orchestrator for SynapseLog.
Runs the entire end-to-end pipeline in a single command:
1. Verifies/Starts Kafka Broker in Docker.
2. Starts the Microservice Traffic & Fault Generator into Kafka topic 'logs'.
3. Starts the 4-Tier AI Stream Consumer running on NVIDIA RTX 2050 GPU (CUDA).
4. Renders live stream telemetry, alerts, and causal RCA in real-time.
"""

import os
import sys
import time
import socket
import subprocess
import threading
import json
from typing import Dict, Any

# Ensure local directories are in python path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

SMP_DIR = os.path.join(CURRENT_DIR, "social-media-platform")
if SMP_DIR not in sys.path:
    sys.path.insert(0, SMP_DIR)

from synapse_ai.pipeline_orchestrator import SynapsePipeline
from traffic_generator import MicroserviceTrafficEngine
from kafka import KafkaConsumer, KafkaProducer


DOCKER_BIN_DIR = r"C:\Users\prajw\AppData\Local\Programs\DockerDesktop\resources\bin"


def ensure_docker_path():
    """Adds Docker Desktop resources/bin to PATH if not already present."""
    if os.path.exists(DOCKER_BIN_DIR) and DOCKER_BIN_DIR not in os.environ.get("PATH", ""):
        os.environ["PATH"] = DOCKER_BIN_DIR + os.pathsep + os.environ.get("PATH", "")


def is_port_open(host="127.0.0.1", port=9092, timeout=2.0) -> bool:
    """Checks if a TCP port is accepting connections."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


def start_kafka_docker():
    """Ensures Kafka container is running via Docker Compose."""
    ensure_docker_path()
    if is_port_open("127.0.0.1", 9092):
        print("[Kafka Check] Kafka broker is already up and listening on port 9092.")
        return

    print("[Kafka Check] Starting Kafka container via Docker Compose...")
    try:
        subprocess.run(["docker", "compose", "up", "-d", "kafka"], cwd=CURRENT_DIR, check=True)
    except Exception as e:
        print(f"[Warning] Could not start Docker Kafka via command: {e}")

    # Wait for Kafka port to open
    print("[Kafka Check] Waiting for Kafka broker to accept connections on port 9092...")
    for _ in range(30):
        if is_port_open("127.0.0.1", 9092):
            print("[Kafka Check] Kafka broker is ready!")
            time.sleep(2)
            return
        time.sleep(1)

    print("[Warning] Kafka broker did not respond on port 9092 in time.")


def start_traffic_producer(stop_event: threading.Event):
    """Background thread that emits realistic microservice traffic into Kafka."""
    print("[Producer Thread] Initializing microservice traffic generator...")
    try:
        engine = MicroserviceTrafficEngine(broker_url="localhost:9092", topic="logs")
    except Exception as e:
        print(f"[Producer Thread] Error initializing producer: {e}")
        return

    phase = 0
    while not stop_event.is_set():
        phase += 1
        # Normal Traffic (40 logs)
        for _ in range(25):
            if stop_event.is_set():
                break
            engine.generate_normal_cycle(count=1, delay_sec=0.08)

        if stop_event.is_set():
            break

        # Periodic Injected Failure
        if phase % 2 == 1:
            # Known operational error: Database Connection Pool Exhaustion
            engine.inject_incident_burst("db_pool_exhaustion", burst_count=12)
        else:
            # Zero-Day Novel Incident: Raft Split-Brain Panic
            engine.inject_incident_burst("zero_day_consensus", burst_count=15)

        time.sleep(1.0)

    print("[Producer Thread] Stopped traffic generation.")


def log_incident_report(diagnosis: Dict[str, Any]):
    """Persists structured incidents for auditing and inspection."""
    report_dir = os.path.join(CURRENT_DIR, "reports")
    os.makedirs(report_dir, exist_ok=True)
    inc_file = os.path.join(report_dir, "incidents.json")

    incidents = []
    if os.path.exists(inc_file):
        try:
            with open(inc_file, "r", encoding="utf-8") as f:
                incidents = json.load(f)
        except Exception:
            incidents = []

    incidents.append(diagnosis)
    with open(inc_file, "w", encoding="utf-8") as f:
        json.dump(incidents, f, indent=2)

    # Human-readable audit line
    audit_file = os.path.join(report_dir, "incident_audit.log")
    with open(audit_file, "a", encoding="utf-8") as f:
        f.write(f"[{diagnosis.get('timestamp')}] {diagnosis.get('incident_id')} | Service: {diagnosis.get('focal_service')} | Team: {diagnosis.get('routing_target')} | Root Cause: {diagnosis.get('root_cause_summary')}\n")


def run_pipeline():
    print("=" * 75)
    print("  SYNAPSELOG END-TO-END PIPELINE: STREAMING + HIERARCHICAL AI ENGINE")
    print("=" * 75)

    # 1. Start Docker Kafka
    start_kafka_docker()

    # 2. Initialize 4-Tier AI Engine (CUDA Warmup)
    pipeline = SynapsePipeline(device="cuda", tau_confident=0.45)

    # 3. Connect Kafka Stream Consumer (Direct Topic Subscriber)
    print("[Consumer] Connecting to Kafka topic 'logs'...")
    consumer = None
    for attempt in range(15):
        try:
            consumer = KafkaConsumer(
                "logs",
                bootstrap_servers=["localhost:9092"],
                api_version=(3, 5, 0),
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            print("[Consumer] Connected to Kafka stream successfully!")
            break
        except Exception as e:
            time.sleep(1.0)

    if not consumer:
        print("[Fatal] Could not connect to Kafka. Exiting.")
        return

    # 4. Start Background Microservice Traffic Generator
    stop_event = threading.Event()
    producer_thread = threading.Thread(target=start_traffic_producer, args=(stop_event,), daemon=True)
    producer_thread.start()

    print("\n" + "=" * 75)
    print("  SYNAPSELOG AI ENGINE LIVE COCKPIT - PRESS CTRL+C TO STOP")
    print("=" * 75 + "\n")

    logs_count = 0
    last_report = time.time()

    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=250)
            except Exception:
                time.sleep(0.05)
                continue
            for tp, msgs in records.items():
                for msg in msgs:
                    raw_log = msg.value
                    if not isinstance(raw_log, dict):
                        continue

                    logs_count += 1
                    res = pipeline.process_log(raw_log)
                    verdict = res["verdict"]
                    srv = raw_log.get("service", "unknown")
                    lat = res["latency_ms"]

                    if verdict == "ESCALATED_LLM_RCA":
                        print("\n" + "!" * 75)
                        print(f"  [TIER-3 GENERATIVE LLM ROOT CAUSE ANALYSIS TRIGGERED]")
                        print(f"  Incident ID       : {res['incident_id']}")
                        print(f"  Focal Service     : {srv}")
                        print(f"  Root Cause        : {res['root_cause_summary']}")
                        print(f"  Blast Radius      : {res['blast_radius']}")
                        print(f"  Assigned Team     : {res['routing_target']} (Pager: {res['oncall_pager']})")
                        print(f"  Mitigation Action : {res['mitigation_steps'][0]}")
                        print(f"  Transient Sig     : {res['transient_signature']}")
                        print(f"  Tier-4 Action     : Injected Bloom suppression (Duplicate filter active)!")
                        print("!" * 75 + "\n")
                        log_incident_report(res)

                    elif verdict == "SUPPRESSED_ONGOING_INCIDENT":
                        if logs_count % 5 == 0:
                            print(f"  Log #{logs_count:04d} | [{srv:20s}] Verdict: {verdict:28s} | Latency: {lat:.3f} ms")

                    elif verdict == "RESOLVED_BY_SLM_LOCALLY":
                        print(f"  Log #{logs_count:04d} | [{srv:20s}] Verdict: {verdict:28s} | Latency: {lat:.3f} ms")

                    else: # PASS_TO_ARCHIVE
                        if logs_count % 10 == 0:
                            print(f"  Log #{logs_count:04d} | [{srv:20s}] Verdict: {verdict:28s} | Latency: {lat:.3f} ms")

            # Periodic Status Banner every 12 seconds
            now = time.time()
            if now - last_report >= 12.0 and logs_count > 0:
                rep = pipeline.get_summary_report()
                print("\n" + "-" * 75)
                print(f"  LIVE TELEMETRY: Ingested: {logs_count} | Tier-1 Benign: {pipeline.metrics['tier1_filtered_benign']} | Tier-4 Suppressed: {rep['tier4_duplicate_suppression_count']} | Tier-2 SLM: {rep['tier2_resolved_count']} | Tier-3 RCA: {rep['tier3_escalated_count']} | P50: {rep['latency_p50_ms']:.3f}ms")
                print("-" * 75 + "\n")
                last_report = now

    except KeyboardInterrupt:
        print("\n\n[Shutdown] Caught Ctrl+C! Stopping pipeline...")
    finally:
        stop_event.set()
        producer_thread.join(timeout=2.0)
        consumer.close()
        
        rep = pipeline.get_summary_report()
        print("\n" + "=" * 75)
        print("  FINAL SESSION TELEMETRY & EFFICIENCY REPORT")
        print("=" * 75)
        print(f"Total Logs Streamed Through Kafka : {logs_count}")
        print(f"Tier-1 Sub-Millisecond Absorbed   : {pipeline.metrics['tier1_filtered_benign']} logs")
        print(f"Tier-1 Duplicate Burst Suppressed : {rep['tier4_duplicate_suppression_count']} logs (Tier-4 Distillation)")
        print(f"Tier-2 Resolved Locally (SLM)     : {rep['tier2_resolved_count']} logs (~6ms local triage)")
        print(f"Tier-3 Deep LLM RCA Diagnoses     : {rep['tier3_escalated_count']} incidents")
        print(f"Estimated LLM Tokens Saved        : {rep['saved_llm_tokens']} tokens")
        print(f"Median Ingestion Latency (P50)    : {rep['latency_p50_ms']:.3f} ms")
        print(f"99th Percentile Latency (P99)     : {rep['latency_p99_ms']:.3f} ms")
        print(f"Reports Saved To                  : {os.path.join(CURRENT_DIR, 'reports')}")
        print("=" * 75 + "\n")


if __name__ == "__main__":
    run_pipeline()
