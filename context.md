# SynapseLog: Research Context & Engineering Master Document

## 1. Executive Summary & Research Aim
The primary aim of **SynapseLog** is to pioneer an academically novel, ultra-high-throughput, multi-tiered AI log anomaly detection and autonomous incident triage architecture capable of acceptance in premier peer-reviewed computer systems and software engineering conferences (**USENIX ATC, ACM CCS, IEEE S&P, ICSE, FSE, or IEEE TDSC**).

### The Problem in Current State-of-the-Art:
1. **Template Rigidity in Traditional ML (Drain, DeepLog, LogRobust):** Modern software systems undergo frequent code updates and dependency upgrades. Traditional parsers and machine learning models break under benign syntactic changes (concept drift), triggering crippling false-positive alarm storms.
2. **Economic & Latency Collapse of Monolithic LLMs:** Streaming hundreds of thousands of raw logs directly to large models (e.g., GPT-4 or Claude 3.5 Sonnet) costs thousands of dollars per day and introduces multi-second inference latencies that violate real-time monitoring SLAs.
3. **The Fatal Flaw of Naive Cascades ("Classifier -> SLM -> LLM"):** Naive cascade proposals are rejected by reviewers because neural networks have uncalibrated confidence (the SLM does not mathematically know when it has failed), repetitive alert storms during outages overwhelm the LLM, and isolated log lines lack causal grounding in distributed microservice topologies.

---

## 2. Hardware Resource Utilization & Local Acceleration
* **Host Operating System:** Windows 11 (64-bit)
* **Runtime:** Python 3.12.6
* **Primary AI Accelerator:** **NVIDIA GeForce RTX 2050 Laptop GPU** (Device 0, 4.0 GB VRAM, Compute Capability 8.6, CUDA 12.4 via PyTorch 2.6.0).
* **Workload Partitioning:**
  * **Tier-1 (CPU):** Vectorized string hashing (MurmurHash3/MD5), compiled regular-expression token masking, and Scikit-Learn Isolation Forest evaluating 16-dimensional statistical feature vectors in **< 0.02 ms**.
  * **Tier-2 (GPU - RTX 2050 CUDA):** Quantized Small Language Model (`distilbert-base-uncased`) fine-tuned for log sequence classification. Evaluates sequences in **8–12 ms** on GPU and computes calibrated **Shannon Entropy $\mathcal{H}(P)$** and **Logit Margin $\mathcal{M}_{\text{margin}}$** to derive the composite Epistemic Uncertainty $U(x)$.
  * **Tier-3 (Async Reasoning):** Generates structured JSON Root Cause Analysis (RCA), blast-radius estimations, and team ownership assignments using a service topology graph.
  * **Tier-4 (Distillation Feedback):** Injects transient Bloom filter signatures and prototype vectors back into Tiers 1 and 2 with dynamic Time-To-Live (TTL).

---

## 3. Benchmark Datasets & Data Origin
SynapseLog is validated against both canonical academic benchmarks and a real-world microservice testbed:

1. **LogHub BGL (Blue Gene/L Supercomputer):**
   * **Source:** Lawrence Livermore National Laboratory (LLNL), hosted in the official LogPAI repository.
   * **Characteristics:** Over 4.7 million raw logs from an HPC supercomputer with 131,072 processors. Contains hardware failure tags and operator RAS alerts (ground-truth labels: `-` for normal, alert category strings for anomalies).
   * **Stored Path:** `evaluation/datasets/BGL_2k.log` (with automatic fallback to `evaluation_BGL_2k.log`)
2. **LogHub HDFS (Hadoop Distributed File System):**
   * **Source:** Amazon EC2 Hadoop clusters running map-reduce workloads.
   * **Characteristics:** 11.1 million logs sessionized by Hadoop Block IDs (`blk_*`), standard benchmark for sequence anomalies.
   * **Stored Path:** `evaluation/datasets/HDFS_2k.log` (with automatic fallback to `evaluation_HDFS_2k.log`)
3. **Synthetic Multi-Service Microservice Telemetry (Platform Testbed):**
   * **Architecture:** Three interconnected services (`auth-service`, `post-service`, and `notification-service`).
   * **Injected Outages:** Database connection pool exhaustion, authentication timeouts, cascading retry storms, and memory leaks.

---

## 4. Models Employed & Training Methodology

### Model Breakdown by Tier:
| Pipeline Tier | Model Architecture | Hardware Device | Role & Function |
| :--- | :--- | :--- | :--- |
| **Tier-1** | Dual-Path Bloom Filter + Scikit-Learn Isolation Forest | Host CPU | Filters >65-98% of routine logs in < 0.02 ms; instantly fires deterministic P0 alerts. |
| **Tier-2** | Fine-Tuned `distilbert-base-uncased` (66M params) | **NVIDIA RTX 2050 (CUDA)** | Evaluates sequence semantics; computes Epistemic Uncertainty $U(x)$; resolves benign drift locally. |
| **Tier-3** | Topology-Grounded Generative LLM Reasoner | Async Engine / Cloud API | Produces causal root-cause summaries, blast-radius maps, mitigation steps, and team dispatch. |
| **Tier-4** | Active Prototype & Transient Signature Feedback | Hybrid RAM / Bloom | Deduplicates repeated logs of ongoing outages; prevents redundant LLM calls. |

### How Training & Calibration Were Executed:
1. **Script:** `synapse_ai/train_slm.py`
2. **Training Data:** 712 baseline samples extracted from LogHub BGL normal sequences combined with microservice operational templates.
3. **Hyperparameters:** Batch size 16, learning rate $3 \times 10^{-5}$, AdamW optimizer, CrossEntropy loss, 3 epochs.
4. **Hardware Execution:** Ran directly on the local **NVIDIA GeForce RTX 2050 GPU (CUDA)**.
5. **Convergence Results:**
   * Epoch 1/3: Loss `0.3151`, Accuracy `88.62%`
   * Epoch 2/3: Loss `0.0261`, Accuracy `99.86%`
   * Epoch 3/3: Loss `0.0112`, Accuracy `99.86%`
   * Wall-clock training time: **36.18 seconds**.
   * Saved model checkpoint: `models/tier2_slm_calibrated/`.

---

## 5. Empirical Results & Test Runs

### Test 1: Live Microservice Stream Simulation (`evaluation/simulate_stream.py`)
Simulated a 4-phase production workload through SynapseLog (Normal -> Known Fault -> Novel Zero-Day Burst -> Recovery):
* **Total Stream Logs:** 140
* **Phase 1 (Normal Operations):** 70 logs filtered at Tier-1 Bloom filter in **0.007–0.015 ms** (`PASS_TO_ARCHIVE`).
* **Phase 2 (Known Fault):** 50 operational error logs evaluated by the fine-tuned SLM on your **NVIDIA RTX 2050 GPU (CUDA)**. Because uncertainty was low ($U(x) < 0.45$), all were resolved locally in **5.1–8.6 ms** without spending any LLM tokens (`RESOLVED_BY_SLM_LOCALLY`).
* **Phase 3 (Novel Zero-Day Burst):** High epistemic uncertainty ($U(x) \ge 0.45$) triggered an automatic escalation to Tier-3:
  * **Tier-3 Generative RCA:** Produced structured diagnosis `INC-2026-1001`, identified database pool exhaustion, computed blast radius, formulated mitigation steps, and routed to the `Identity-and-Access-Management (IAM)` team.
  * **Tier-4 Distillation Injection:** Synthesized transient signature `auth-service::db_pool_exhaustion` and injected it into Tier-1 Bloom filter with 15-min TTL.
  * **Instant Duplicate Suppression:** Subsequent 19 burst logs were intercepted at Tier-1 as `SUPPRESSED_ONGOING_INCIDENT` in **0.047 ms**!
* **Repetitive Escalation Suppression:** **100.0%** (An ongoing failure storm generated exactly 1 LLM escalation!).
* **Estimated LLM Tokens Saved:** 28,500 tokens.
* **Latency Profile:** **P50 = 0.047 ms**, **P99 = 14.221 ms**.

### Test 2: Standard LogHub BGL Benchmark (`evaluation/benchmark_runner.py`)
Evaluated on 1,000 held-out test logs from the Blue Gene/L supercomputer:
* **Ground Truth Anomalies:** 25 (2.5%)
* **True Positives (TP):** 23
* **True Negatives (TN):** 803
* **Recall:** **92.00%**
* **False Alarm Rate (FAR):** 17.64%
* **Throughput:** 251.8 logs/sec
* **Latency Profile:** **P50 = 0.020 ms**, **P99 = 15.329 ms**
* **Duplicate Logs Suppressed by Tier-4:** 83 logs

---

## 6. How to Run the System Locally

### Option A: The One-Click End-to-End Master Pipeline (Kafka + Microservices + CUDA AI Engine)
Double-click `run_pipeline.bat` or run in PowerShell:
```powershell
.\run_pipeline.bat
# OR
python synapse_orchestrator.py
```
This single command automatically:
1. Verifies Docker Desktop and boots the Apache Kafka KRaft container on `localhost:9092`.
2. Starts the background microservices traffic generator (`auth-service`, `post-service`, `notification-service`).
3. Connects the SynapseLog Kafka Consumer.
4. Executes the 4-Tier AI Engine live on your **NVIDIA GeForce RTX 2050 GPU (CUDA)**.
5. Displays real-time live telemetry banners, latencies, and Tier-3 causal root-cause analyses.

### Option B: Standalone Simulations & Benchmarks
```powershell
# 1. Run the standalone live stream simulation with cascading fault injection
python -m evaluation.simulate_stream

# 2. Run the academic benchmark on LogHub BGL supercomputer logs
python -m evaluation.benchmark_runner

# 3. Re-train / recalibrate the Tier-2 SLM locally
python -m synapse_ai.train_slm
```

### Option C: On-Demand Fault & Traffic Injection
To introduce custom failures or scale up log volume while the pipeline is running:
```powershell
# Interactive menu:
python inject_fault.py

# CLI commands:
python inject_fault.py --fault db_pool --count 20
python inject_fault.py --fault zero_day --count 15
python inject_fault.py --traffic spike --count 200
python inject_fault.py --custom "Payment Gateway timeout 504 on Stripe webhook" --service auth-service --level ERROR
```

### Option D: Audit & Inspect Diagnosed Incidents
To review what happened, the causal root causes, blast radius, and mitigation steps:
```powershell
python view_incidents.py
# Or inspect specific incident ID:
python view_incidents.py --id INC-2026-1001
```

### Option E: Full System Cleanup & Resource Teardown
To stop all background Docker containers and clear temporary files so your machine bears zero load:
Double-click `clean_all.bat` or run:
```powershell
.\clean_all.bat
```

---

## 7. Research Paper Blueprint & Research Questions (RQs)

To target top-tier venues (USENIX ATC, ACM CCS, ICSE, FSE, IEEE S&P), the paper structure is mapped to 5 Research Questions:

* **RQ1 (Detection Effectiveness):** Does `SynapseLog` maintain high Recall (>92%) while drastically reducing False Alarm Rates compared to uncalibrated baselines?
* **RQ2 (System Throughput & Scalability):** Can the pipeline sustain real-time high-throughput ingestion with sub-millisecond median latency ($P50 < 0.02$ ms)?
* **RQ3 (Epistemic Calibration & Cost Efficiency):** Does conformal epistemic gating accurately route only truly ambiguous cases, achieving $> 99\%$ reduction in LLM token expenditures?
* **RQ4 (RCA & Routing Quality):** How accurately does the topology-augmented dossier prevent LLM hallucinations and correctly dispatch to the responsible engineering team?
* **RQ5 (Ablation & Distillation Velocity):** How effectively does the Tier-4 closed-loop feedback loop suppress repetitive alert storms during ongoing outages?

---

## 8. Recent Engineering Refinements & Quality Hardening

1. **Benchmark Dataset Auto-Discovery & Teardown Protection:**
   * Benchmark datasets are now isolated in `evaluation/datasets/` (`BGL_2k.log` and `HDFS_2k.log`), tracked via `.gitignore` exception `!evaluation/datasets/*.log`, and auto-discovered with on-demand fallback downloading across all evaluation tools (`benchmark_runner.py` and `train_slm.py`).
   * `clean_all.bat` and `clean_all.ps1` were updated to remove indiscriminate `*.log` deletions, ensuring benchmark ground truth is never lost when stopping Docker containers.
2. **Causal RCA Precision & Trigger Priority:**
   * Updated `synapse_ai/tier3_llm_rca.py` and `synapse_ai/pipeline_orchestrator.py` to prioritize the exact `trigger_log` over background session noise.
   * Novel Zero-Day faults (such as Raft consensus partition / quorum split-brain) are now cleanly isolated and assigned to `Platform-Core-Infrastructure (Infra & SRE)`, while database connection pool exhaustion is dispatched to the database SRE team with dedicated PgBouncer scaling actions.
3. **Incident Audit Inspector (`view_incidents.py`):**
   * Enhanced with flexible dictionary key fallbacks so all diagnosed incidents display exact timestamps, focal services, blast radiuses, and mitigation steps both in the summary table and single-incident deep-dive view (`python view_incidents.py --id <INC_ID>`).
