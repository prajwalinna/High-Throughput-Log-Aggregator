"""
view_incidents.py
-----------------
Audit and Incident Inspection CLI for SynapseLog.
Allows engineers, SREs, and paper reviewers to inspect what happened,
why an incident was flagged, the causal root-cause analysis, and blast-radius routing.
"""

import os
import sys
import json
import argparse
from typing import List, Dict, Any

REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
INCIDENTS_FILE = os.path.join(REPORTS_DIR, "incidents.json")
AUDIT_LOG_FILE = os.path.join(REPORTS_DIR, "incident_audit.log")


def load_incidents() -> List[Dict[str, Any]]:
    if not os.path.exists(INCIDENTS_FILE):
        return []
    try:
        with open(INCIDENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def print_table(incidents: List[Dict[str, Any]]):
    if not incidents:
        print("\nNo incidents recorded yet.")
        print(f"Reports are stored in: {REPORTS_DIR}")
        print("Run 'python synapse_orchestrator.py' or 'python inject_fault.py --fault zero_day' to trigger incidents.")
        return

    print("\n" + "=" * 90)
    print("  SYNAPSELOG INCIDENT AUDIT TRAIL & CAUSAL ROOT CAUSE DOSSIERS")
    print("=" * 90)
    print(f"{'Incident ID':<15} | {'Timestamp':<20} | {'Focal Service':<18} | {'Assigned Team':<25}")
    print("-" * 90)

    for inc in incidents:
        inc_id = inc.get("incident_id", "N/A")
        ts = inc.get("timestamp", "N/A")[:19]
        srv = inc.get("focal_service", "N/A")
        team = inc.get("routing_target", "N/A")
        print(f"{inc_id:<15} | {ts:<20} | {srv:<18} | {team:<25}")

    print("=" * 90)
    print(f"Total Diagnosed Incidents: {len(incidents)}")
    print("To view full causal dossier for an incident, run:")
    print(f"  python view_incidents.py --id {incidents[-1].get('incident_id', 'INC-2026-1001')}\n")


def print_incident_detail(incident_id: str):
    incidents = load_incidents()
    target = None
    for inc in incidents:
        if inc.get("incident_id") == incident_id:
            target = inc
            break

    if not target:
        print(f"[Error] Incident '{incident_id}' not found in audit trail.")
        return

    print("\n" + "=" * 80)
    print(f"  DETAILED CAUSAL ROOT CAUSE DOSSIER: {incident_id}")
    print("=" * 80)
    print(f"Timestamp           : {target.get('timestamp')}")
    print(f"Focal Microservice  : {target.get('focal_service')}")
    print(f"Assigned Team       : {target.get('routing_target')} (Pager: {target.get('oncall_pager')})")
    print(f"Blast Radius        : {target.get('blast_radius')}")
    print(f"Transient Signature : {target.get('transient_signature')}")
    print("-" * 80)
    print(f"Root Cause Summary  :\n  {target.get('root_cause_summary')}")
    print("-" * 80)
    print("Recommended Mitigation Steps:")
    for idx, step in enumerate(target.get("mitigation_steps", []), 1):
        print(f"  {idx}. {step}")
    print("-" * 80)
    print("Antecedent Focal Logs Observed by AI:")
    for log in target.get("dossier", {}).get("recent_focal_logs", []):
        print(f"  - {log}")
    print("=" * 80 + "\n")


def main():
    parser = argparse.ArgumentParser(description="SynapseLog Incident Inspector")
    parser.add_argument("--id", type=str, help="Specific Incident ID to inspect in detail")
    args = parser.parse_args()

    if args.id:
        print_incident_detail(args.id)
    else:
        print_table(load_incidents())


if __name__ == "__main__":
    main()
