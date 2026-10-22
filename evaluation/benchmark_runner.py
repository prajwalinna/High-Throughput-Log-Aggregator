"""
evaluation/benchmark_runner.py
------------------------------
Standard Academic Benchmark Evaluator on LogHub BGL & HDFS Datasets.
Evaluates Precision, Recall, Macro-F1, Latency, and Tier Load Distribution.
Produces empirical results for Paper Research Questions (RQ1 - RQ5).
"""

import os
import re
import time
from typing import Dict, List, Any
import numpy as np

from synapse_ai.pipeline_orchestrator import SynapsePipeline


def parse_bgl_line(line: str) -> Dict[str, Any]:
    """
    Parses a single line from LogHub BGL format:
    Format: Label Timestamp Date Node Time Node_Repeat Subsystem Level Message
    Label '-' indicates normal; any other string indicates anomaly alert category.
    """
    parts = line.strip().split(maxsplit=9)
    if len(parts) < 10:
        return {
            "is_anomaly": False,
            "service": "bgl-node",
            "level": "INFO",
            "message": line.strip()
        }

    label = parts[0]
    is_anomaly = (label != "-")
    timestamp = parts[1]
    node = parts[3]
    subsystem = parts[7]
    level = parts[8]
    message = parts[9]

    return {
        "is_anomaly": is_anomaly,
        "label": label,
        "service": f"bgl-{subsystem.lower()}",
        "node": node,
        "level": level,
        "message": message
    }


def run_bgl_benchmark(dataset_path: str = "evaluation_BGL_2k.log", sample_size: int = 1000):
    print("\n" + "=" * 70)
    print("  RUNNING SYNAPSELOG BENCHMARK ON LOGHUB BGL (Blue Gene/L Supercomputer)")
    print("=" * 70)
    
    if not os.path.exists(dataset_path):
        print(f"Error: Dataset not found at {dataset_path}")
        return

    # Initialize Pipeline on NVIDIA RTX 2050 GPU (CUDA)
    pipeline = SynapsePipeline(device="cuda", tau_confident=0.65)

    # Warm up Tier-1 with known normal logs from the first 500 lines (Standard LogHub semi-supervised protocol)
    print("\n[Stage 1] Training Tier-1 Bloom filter & Isolation Forest on normal baseline...")
    normal_features = []
    with open(dataset_path, "r", encoding="utf-8", errors="ignore") as f:
        for idx, line in enumerate(f):
            if idx >= 600:
                break
            parsed = parse_bgl_line(line)
            if not parsed["is_anomaly"]:
                # Register known benign template
                masked = pipeline.sessionizer.ingest(parsed)[0].get("masked_message", parsed["message"])
                pipeline.tier1.bloom.register_benign(parsed["service"], masked)
                normal_features.append(pipeline.sessionizer.windows[parsed["service"]].get_features())

    pipeline.tier1.warm_up_ensemble(normal_features)
    print(f"[Stage 1] Tier-1 trained on {len(normal_features)} normal baseline samples.")

    # Run Benchmark Evaluation on Test Split (Logs 601 to 1600)
    print(f"\n[Stage 2] Evaluating test split (1000 held-out logs) through SynapseLog...")
    ground_truth = []
    predictions = []
    
    start_bench_time = time.time()
    processed_count = 0

    with open(dataset_path, "r", encoding="utf-8", errors="ignore") as f:
        for idx, line in enumerate(f):
            if idx < 600:
                continue
            if idx >= 1600:
                break

            parsed = parse_bgl_line(line)
            ground_truth.append(1 if parsed["is_anomaly"] else 0)

            # Process through SynapseLog Pipeline
            res = pipeline.process_log(parsed)
            processed_count += 1

            # Consider anomalous if Tier-1 flagged fatal, Tier-2 flagged anomaly/uncertain, or escalated to Tier-3
            verdict = res["verdict"]
            is_pred_anomaly = verdict in [
                "INSTANT_P0_ALERT",
                "ESCALATED_LLM_RCA",
                "SUPPRESSED_ONGOING_INCIDENT"
            ] or (verdict == "RESOLVED_BY_SLM_LOCALLY" and res.get("predicted_class") != "BENIGN_NORMAL")

            predictions.append(1 if is_pred_anomaly else 0)

            if (idx + 1) % 250 == 0:
                print(f"  Processed {idx + 1}/{sample_size} logs... (Current verdict: {verdict})")

    total_time = time.time() - start_bench_time
    throughput_eps = processed_count / max(0.001, total_time)

    # Compute Classification Metrics
    tp = sum(1 for yt, yp in zip(ground_truth, predictions) if yt == 1 and yp == 1)
    fp = sum(1 for yt, yp in zip(ground_truth, predictions) if yt == 0 and yp == 1)
    tn = sum(1 for yt, yp in zip(ground_truth, predictions) if yt == 0 and yp == 0)
    fn = sum(1 for yt, yp in zip(ground_truth, predictions) if yt == 1 and yp == 0)

    precision = tp / max(1, (tp + fp))
    recall = tp / max(1, (tp + fn))
    f1 = 2 * (precision * recall) / max(1e-12, (precision + recall))
    far = fp / max(1, (fp + tn)) # False Alarm Rate

    report = pipeline.get_summary_report()

    print("\n" + "=" * 70)
    print("  EMPIRICAL BENCHMARK EVALUATION RESULTS (FOR RESEARCH PAPER)")
    print("=" * 70)
    print(f"Dataset Evaluated        : LogHub BGL ({processed_count} logs)")
    print(f"Hardware Platform        : NVIDIA GeForce RTX 2050 (4.0GB VRAM) + CUDA")
    print(f"Ground Truth Anomalies   : {sum(ground_truth)} ({sum(ground_truth)/processed_count*100:.1f}%)")
    print(f"True Positives (TP)      : {tp}")
    print(f"False Positives (FP)     : {fp}")
    print(f"True Negatives (TN)      : {tn}")
    print(f"False Negatives (FN)     : {fn}")
    print("-" * 70)
    print(f"Precision                : {precision * 100:.2f}%")
    print(f"Recall                   : {recall * 100:.2f}%")
    print(f"Macro F1-Score           : {f1 * 100:.2f}%")
    print(f"False Alarm Rate (FAR)   : {far * 100:.2f}%")
    print("-" * 70)
    print(f"Overall Ingestion Throughput: {throughput_eps:.1f} logs/sec")
    print(f"P50 Ingestion Latency    : {report['latency_p50_ms']:.3f} ms")
    print(f"P95 Ingestion Latency    : {report['latency_p95_ms']:.3f} ms")
    print(f"P99 Ingestion Latency    : {report['latency_p99_ms']:.3f} ms")
    print("-" * 70)
    print(f"Tier-1 Absorbed / Filtered: {report['tier1_absorbed_ratio']:.2f}% of total stream")
    print(f"Tier-2 SLM Evaluated     : {report['tier2_evaluated_count']} logs")
    print(f"Tier-3 LLM Escalations   : {report['tier3_escalated_count']} incidents")
    print(f"Tier-4 Duplicate Suppress: {report['tier4_duplicate_suppression_count']} logs")
    print(f"Estimated LLM Cost Saved : ${report['estimated_dollars_saved']:.2f} ({report['saved_llm_tokens']} tokens)")
    print("=" * 70)

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "far": far,
        "throughput_eps": throughput_eps,
        "report": report
    }


if __name__ == "__main__":
    run_bgl_benchmark(sample_size=1000)
