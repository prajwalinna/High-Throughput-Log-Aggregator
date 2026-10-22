"""
synapse_ai/pager_dispatcher.py
------------------------------
Autonomous On-Call Pager, Continuous Failure Escalation, & Self-Healing Circuit Breaker.
Dispatches critical incident alerts and continuous outage reminders to:
1. Native Windows Desktop Notifications (Visual Toast / Balloon)
2. Audible System Alarms (Windows MB_ICONWARNING Beep)
3. Configurable Webhooks (Slack, Discord, Microsoft Teams, PagerDuty)
4. Dedicated On-Call Pager Outbox & Audit Logs (reports/pager_notifications.json)
5. Autonomous Circuit Breaker: Automatically sheds traffic and isolates the failing service
   if the on-call engineer is unresponsive after 5 escalation reminders!
"""

import os
import sys
import json
import time
import ctypes
import threading
import subprocess
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import urllib.request


REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
PAGER_FILE = os.path.join(REPORTS_DIR, "pager_notifications.json")
PAGER_LOG = os.path.join(REPORTS_DIR, "pager_alerts.log")
CIRCUIT_FILE = os.path.join(REPORTS_DIR, "circuit_breaker_status.json")
CONFIG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "pager_config.json")


class PagerDispatcher:
    """
    Manages real-time on-call alerts, continuous failure tracking,
    automatic escalation reminders, and autonomous circuit-breaker failovers.
    """
    def __init__(
        self,
        reminder_interval_logs: int = 40,
        reminder_cooldown_sec: float = 30.0,
        max_reminders_before_circuit_break: int = 5
    ):
        self.reminder_interval_logs = reminder_interval_logs
        self.reminder_cooldown_sec = reminder_cooldown_sec
        self.max_reminders_before_circuit_break = max_reminders_before_circuit_break
        self.active_outages: Dict[str, Dict[str, Any]] = {}
        self.tripped_circuits: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        os.makedirs(REPORTS_DIR, exist_ok=True)
        self.webhook_url = self._load_webhook_url()
        self._load_config_overrides()
        self._load_persisted_circuit_status()

    def _load_webhook_url(self) -> Optional[str]:
        env_url = os.environ.get("SYNAPSE_PAGER_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL")
        if env_url:
            return env_url
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                    return cfg.get("webhook_url")
            except Exception:
                pass
        return None

    def _load_config_overrides(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                    cb_cfg = cfg.get("circuit_breaker", {})
                    self.max_reminders_before_circuit_break = cb_cfg.get(
                        "max_unresponsive_reminders", self.max_reminders_before_circuit_break
                    )
            except Exception:
                pass

    def _load_persisted_circuit_status(self):
        if os.path.exists(CIRCUIT_FILE):
            try:
                with open(CIRCUIT_FILE, "r", encoding="utf-8") as f:
                    self.tripped_circuits = json.load(f)
            except Exception:
                self.tripped_circuits = {}

    def _persist_circuit_status(self):
        try:
            with open(CIRCUIT_FILE, "w", encoding="utf-8") as f:
                json.dump(self.tripped_circuits, f, indent=2)
        except Exception:
            pass

    def is_circuit_tripped(self, service: str) -> bool:
        """Checks if autonomous circuit breaker is active for a service."""
        with self._lock:
            status = self.tripped_circuits.get(service)
            return bool(status and status.get("state") == "TRIPPED")

    def record_shed_log(self, service: str):
        """Records traffic shed counts under active circuit breaker."""
        with self._lock:
            if service in self.tripped_circuits:
                self.tripped_circuits[service]["shed_logs_count"] = (
                    self.tripped_circuits[service].get("shed_logs_count", 0) + 1
                )

    def reset_circuit_breaker(self, service: str) -> bool:
        """Manually resets the circuit breaker after developer resolves the outage."""
        with self._lock:
            if service in self.tripped_circuits:
                del self.tripped_circuits[service]
                self._persist_circuit_status()
                # Acknowledge active outages for that service
                for sig, out in self.active_outages.items():
                    if out.get("focal_service") == service:
                        out["status"] = "RESOLVED"
                        out["acknowledged"] = True
                self._persist_alert({
                    "alert_type": "CIRCUIT_BREAKER_RESET",
                    "focal_service": service,
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "NORMAL_TRAFFIC_RESTORED"
                })
                return True
            return False

    def trigger_incident_alert(self, incident: Dict[str, Any]) -> Dict[str, Any]:
        """Fires an initial high-priority P0 / Tier-3 Incident Alert to on-call engineers."""
        with self._lock:
            inc_id = incident.get("incident_id", f"INC-{int(time.time())}")
            srv = incident.get("focal_service", "unknown-service")
            team = incident.get("routing_target", "On-Call SRE")
            pager = incident.get("oncall_pager", "sre-alerts@company.internal")
            root_cause = incident.get("root_cause_summary", "Unclassified operational anomaly")
            blast_radius = incident.get("blast_radius", [srv])
            sig = incident.get("transient_signature", f"{srv}::fault")
            ts = incident.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

            alert_record = {
                "alert_type": "INITIAL_INCIDENT_PAGING",
                "incident_id": inc_id,
                "timestamp": ts,
                "focal_service": srv,
                "assigned_team": team,
                "oncall_pager": pager,
                "severity": "CRITICAL_P0",
                "root_cause": root_cause,
                "blast_radius": blast_radius,
                "transient_signature": sig,
                "status": "OPEN",
                "acknowledged": False,
                "failure_count": 1,
                "reminders_sent": 0,
                "last_reminder_time": time.time(),
                "circuit_breaker_tripped": False
            }

            self.active_outages[sig] = alert_record
            self._dispatch_channels(alert_record, is_reminder=False)
            self._persist_alert(alert_record)
            return alert_record

    def record_suppressed_failure(self, service: str, transient_sig: str) -> Optional[Dict[str, Any]]:
        """
        Called when Tier-4 intercepts an ongoing failure log.
        Tracks continuous failure volume, triggers escalation reminders,
        and trips the Autonomous Circuit Breaker if 5 reminders are ignored!
        """
        with self._lock:
            # If circuit breaker already tripped for this service, ignore further reminders
            if self.is_circuit_tripped(service):
                return None

            outage = self.active_outages.get(transient_sig)
            if not outage:
                for sig, rec in self.active_outages.items():
                    if rec["focal_service"] == service:
                        outage = rec
                        break

            if not outage or outage.get("acknowledged") or outage.get("status") in ["RESOLVED", "CIRCUIT_BREAKER_TRIPPED"]:
                return None

            outage["failure_count"] += 1
            now = time.time()
            count = outage["failure_count"]
            time_since_last = now - outage.get("last_reminder_time", 0)

            # Check if reminder interval reached
            if (count % self.reminder_interval_logs == 0) and (time_since_last >= self.reminder_cooldown_sec):
                outage["reminders_sent"] += 1
                outage["last_reminder_time"] = now

                # AUTONOMOUS CIRCUIT BREAKER THRESHOLD CHECK:
                # If on-call engineer has ignored max_reminders (default: 5), TRIP CIRCUIT BREAKER!
                if outage["reminders_sent"] >= self.max_reminders_before_circuit_break and not outage.get("circuit_breaker_tripped"):
                    return self._activate_circuit_breaker(outage, service)

                reminder_record = {
                    "alert_type": "CONTINUOUS_FAILURE_REMINDER",
                    "incident_id": outage["incident_id"],
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "focal_service": outage["focal_service"],
                    "assigned_team": outage["assigned_team"],
                    "oncall_pager": outage["oncall_pager"],
                    "severity": "ESCALATION_REMINDER",
                    "root_cause": outage["root_cause"],
                    "blast_radius": outage["blast_radius"],
                    "transient_signature": outage["transient_signature"],
                    "status": "ESCALATING",
                    "continuous_failures_intercepted": count,
                    "reminders_sent": outage["reminders_sent"],
                    "reminders_remaining_before_circuit_break": max(
                        0, self.max_reminders_before_circuit_break - outage["reminders_sent"]
                    )
                }

                self._dispatch_channels(reminder_record, is_reminder=True)
                self._persist_alert(reminder_record)
                return reminder_record

            return None

    def _activate_circuit_breaker(self, outage: Dict[str, Any], service: str) -> Dict[str, Any]:
        """
        Autonomous Fail-Safe:
        Trips circuit breaker to isolate the failing microservice, shed traffic,
        and halt cascading degradation when human operators are unresponsive.
        """
        outage["circuit_breaker_tripped"] = True
        outage["status"] = "CIRCUIT_BREAKER_TRIPPED"

        cb_entry = {
            "service": service,
            "incident_id": outage["incident_id"],
            "state": "TRIPPED",
            "tripped_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "trigger_reason": f"Unresponsive on-call after {outage['reminders_sent']} escalation reminders",
            "assigned_team": outage["assigned_team"],
            "shed_logs_count": 0,
            "fallback_mode": "CIRCUIT_ISOLATION_AND_SHEDDING"
        }
        self.tripped_circuits[service] = cb_entry
        self._persist_circuit_status()

        alert_record = {
            "alert_type": "EMERGENCY_CIRCUIT_BREAKER_TRIPPED",
            "incident_id": outage["incident_id"],
            "timestamp": cb_entry["tripped_at"],
            "focal_service": service,
            "assigned_team": outage["assigned_team"],
            "oncall_pager": outage["oncall_pager"],
            "severity": "EMERGENCY_AUTONOMOUS_INTERVENTION",
            "root_cause": outage["root_cause"],
            "blast_radius": outage["blast_radius"],
            "automated_action": f"Autonomous Circuit Breaker Tripped! Ingress traffic shed and [{service}] isolated to protect cluster.",
            "status": "CIRCUIT_BROKEN_SAFE_FALLBACK",
            "reminders_ignored": outage["reminders_sent"]
        }

        self._dispatch_channels(alert_record, is_emergency=True)
        self._persist_alert(alert_record)
        return alert_record

    def acknowledge_incident(self, incident_id: str) -> bool:
        """Engineer acknowledges the alert (silences automated reminders)."""
        with self._lock:
            for sig, rec in self.active_outages.items():
                if rec["incident_id"] == incident_id:
                    rec["acknowledged"] = True
                    rec["status"] = "ACKNOWLEDGED"
                    self._persist_alert({
                        "alert_type": "INCIDENT_ACKNOWLEDGED",
                        "incident_id": incident_id,
                        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "status": "ACKNOWLEDGED"
                    })
                    return True
            return False

    def _dispatch_channels(self, alert: Dict[str, Any], is_reminder: bool = False, is_emergency: bool = False):
        """Dispatches the alert across audio, desktop toast, and webhooks."""
        # 1. Audible alert tone
        try:
            if sys.platform == "win32":
                if is_emergency:
                    ctypes.windll.user32.MessageBeep(0x00000010) # Urgent Hand/Stop sound
                else:
                    ctypes.windll.user32.MessageBeep(0x00000030) # Warning sound
        except Exception:
            pass

        # 2. Windows Desktop Notification
        if is_emergency:
            title = f"🚨 AUTONOMOUS CIRCUIT BREAKER TRIPPED ({alert['incident_id']})"
            body = f"Emergency! [{alert['focal_service']}] isolated into safe fallback after {alert['reminders_ignored']} unanswered reminders. Traffic shed!"
        elif is_reminder:
            rem_left = alert.get("reminders_remaining_before_circuit_break", 0)
            title = f"🔔 ESCALATION REMINDER #{alert['reminders_sent']} ({alert['incident_id']})"
            body = f"Continuous failures in [{alert['focal_service']}]. {rem_left} reminders left before auto circuit break! Paged: {alert['assigned_team']}"
        else:
            title = f"🚨 CRITICAL INCIDENT ALERT ({alert['incident_id']})"
            body = f"Service: [{alert['focal_service']}] | Paged: {alert['assigned_team']} | Cause: {alert['root_cause'][:80]}..."

        threading.Thread(target=self._send_windows_toast, args=(title, body), daemon=True).start()

        # 3. Webhook (Slack / Discord / Teams)
        if self.webhook_url:
            threading.Thread(target=self._send_webhook, args=(alert, is_reminder, is_emergency), daemon=True).start()

    def _send_windows_toast(self, title: str, message: str):
        try:
            clean_title = title.replace("'", "").replace('"', '')
            clean_msg = message.replace("'", "").replace('"', '')
            ps_script = f"""
            [reflection.assembly]::loadwithpartialname('System.Windows.Forms') | Out-Null
            $notify = New-Object System.Windows.Forms.NotifyIcon
            $notify.Icon = [System.Drawing.SystemIcons]::Warning
            $notify.BalloonTipTitle = '{clean_title}'
            $notify.BalloonTipText = '{clean_msg}'
            $notify.Visible = $True
            $notify.ShowBalloonTip(4000)
            Start-Sleep -Milliseconds 400
            $notify.Dispose()
            """
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            subprocess.Popen(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=flags
            )
        except Exception:
            pass

    def _send_webhook(self, alert: Dict[str, Any], is_reminder: bool, is_emergency: bool):
        try:
            color = "#ff0000" if is_emergency else ("#ff6600" if is_reminder else "#ff9900")
            header = "🚨 EMERGENCY CIRCUIT BREAKER ACTIVATED" if is_emergency else ("🔔 ESCALATION REMINDER" if is_reminder else "🚨 SYNAPSELOG PAGER ALERT")
            payload = {
                "text": f"*{header}*",
                "attachments": [
                    {
                        "color": color,
                        "title": f"{alert['incident_id']} - {alert['focal_service']}",
                        "fields": [
                            {"title": "Assigned Team", "value": f"{alert['assigned_team']} ({alert['oncall_pager']})", "short": True},
                            {"title": "Severity", "value": alert['severity'], "short": True},
                            {"title": "Root Cause", "value": alert.get('root_cause', 'N/A'), "short": False},
                            {"title": "Action", "value": alert.get('automated_action', alert.get('status', 'OPEN')), "short": False}
                        ]
                    }
                ]
            }
            req = urllib.request.Request(
                self.webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=3)
        except Exception:
            pass

    def _persist_alert(self, record: Dict[str, Any]):
        try:
            existing = []
            if os.path.exists(PAGER_FILE):
                try:
                    with open(PAGER_FILE, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = []
            existing.append(record)
            with open(PAGER_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)

            with open(PAGER_LOG, "a", encoding="utf-8") as f:
                f.write(f"[{record.get('timestamp')}] {record.get('alert_type')} | {record.get('incident_id')} | Service: {record.get('focal_service')} | Team: {record.get('assigned_team')} | Status: {record.get('status')}\n")
        except Exception:
            pass
