"""
synapse_stream/kafka_consumer.py
--------------------------------
Production Kafka Consumer for SynapseLog.
Subscribes to live Kafka topic 'logs' produced by microservices,
executes real-time 4-Tier AI Triage on RTX 2050 GPU (CUDA),
and streams live telemetry and alerts.
"""

import json
import time
import sys
import os
from typing import Dict, Any

from kafka import KafkaConsumer
from synapse_ai.pipeline_orchestrator import SynapsePipeline


def run_kafka_consumer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "logs",
    group_id: str = "synapse-ai-consumer-group",
    max_logs: int = 0 # 0 for infinite streaming
):
    print("=" * 70)
    print("  SYNAPSELOG REAL-TIME KAFKA STREAM INGESTION & AI TRIAGE")
    print("=" * 70)
    print(f"Connecting to Kafka Broker at: {bootstrap_servers} | Topic: {topic}...")

    # Wait for Kafka to become available
    consumer = None
    retries = 30
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap_servers],
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            print("[Kafka Consumer] Successfully connected to Kafka!")
            break
        except Exception as e:
            retries -= 1
            print(f"[Kafka Consumer] Waiting for Kafka broker ({e})... Retrying in 2s ({retries} left)")
            time.sleep(2)

    if not consumer:
        print("[Fatal] Could not connect to Kafka. Exiting consumer.")
        return

    # Initialize 4-Tier AI Pipeline on RTX 2050 CUDA
    pipeline = SynapsePipeline(device="cuda", tau_confident=0.45)
    print("\n[Synapse AI] 4-Tier Engine active. Listening for live microservice logs...\n")

    logs_processed = 0
    start_time = time.time()
    last_stat_time = time.time()

    try:
        while True:
            # Poll for messages
            records = consumer.poll(timeout_ms=500)
            
            for tp, messages in records.items():
                for message in messages:
                    log_data = message.value
                    if not isinstance(log_data, dict):
                        continue

                    logs_processed += 1
                    res = pipeline.process_log(log_data)
                    verdict = res["verdict"]
                    srv = log_data.get("service", "unknown")
                    lat = res["latency_ms"]

                    if verdict == "ESCALATED_LLM_RCA":
                        print("\n" + "!" * 70)
                        print(f"  [CRITICAL AI INCIDENT DIAGNOSED - TIER 3 GENERATIVE LLM]")
                        print(f"  Incident ID       : {res['incident_id']}")
                        print(f"  Focal Service     : {srv}")
                        print(f"  Root Cause        : {res['root_cause_summary']}")
                        print(f"  Blast Radius      : {res['blast_radius']}")
                        print(f"  Assigned Team     : {res['routing_target']} (Pager: {res['oncall_pager']})")
                        print(f"  Mitigation Steps  : {res['mitigation_steps'][0]}")
                        print(f"  Transient Sig     : {res['transient_signature']}")
                        print(f"  Tier-4 Distillation: Injected into Tier-1 Bloom filter to suppress duplicate logs!")
                        print("!" * 70 + "\n")

                    elif verdict == "SUPPRESSED_ONGOING_INCIDENT":
                        if logs_processed % 10 == 0:
                            print(f"  Log #{logs_processed:04d} | [{srv:20s}] Verdict: {verdict:28s} | Latency: {lat:.3f} ms")

                    elif verdict == "RESOLVED_BY_SLM_LOCALLY":
                        print(f"  Log #{logs_processed:04d} | [{srv:20s}] Verdict: {verdict:28s} | Latency: {lat:.3f} ms (CUDA GPU)")

                    else: # PASS_TO_ARCHIVE
                        if logs_processed % 20 == 0:
                            print(f"  Log #{logs_processed:04d} | [{srv:20s}] Verdict: {verdict:28s} | Latency: {lat:.3f} ms")

                    if max_logs > 0 and logs_processed >= max_logs:
                        break

            # Print periodic summary report every 15 seconds
            now = time.time()
            if now - last_stat_time >= 15.0 and logs_processed > 0:
                report = pipeline.get_summary_report()
                elapsed = now - start_time
                eps = logs_processed / max(0.001, elapsed)
                print("\n" + "-" * 70)
                print(f"  SYNAPSELOG LIVE STREAM TELEMETRY ({eps:.1f} logs/sec)")
                print(f"  Total Ingested     : {logs_processed}")
                print(f"  Tier-1 Benign Filter: {pipeline.metrics['tier1_filtered_benign']}")
                print(f"  Tier-1 Duplicate Sup: {report['tier4_duplicate_suppression_count']}")
                print(f"  Tier-2 SLM Evaluated: {report['tier2_resolved_count']} (CUDA RTX 2050)")
                print(f"  Tier-3 LLM Diagnosed: {report['tier3_escalated_count']}")
                print(f"  Latency (P50 / P99) : {report['latency_p50_ms']:.3f} ms / {report['latency_p99_ms']:.3f} ms")
                print("-" * 70 + "\n")
                last_stat_time = now

            if max_logs > 0 and logs_processed >= max_logs:
                break

    except KeyboardInterrupt:
        print("\n[Kafka Consumer] User stopped consumer.")
    finally:
        consumer.close()
        print("[Kafka Consumer] Disconnected from Kafka.")


if __name__ == "__main__":
    max_l = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    run_kafka_consumer(max_logs=max_l)
