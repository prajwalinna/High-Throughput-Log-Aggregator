"""
synapse_ai/pager_dispatcher.py
------------------------------
Autonomous On-Call Pager & Continuous Failure Escalation Reminder Engine for SynapseLog.
Dispatches critical incident alerts and continuous outage reminders to:
1. Native Windows Desktop Notifications (Visual Toast / Balloon)
2. Audible System Alarms (Windows MB_ICONWARNING Beep)
3. Configurable Webhooks (Slack, Discord, Microsoft Teams, PagerDuty)
4. Dedicated On-Call Pager Outbox & Audit Logs (reports/pager_notifications.json)
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
CONFIG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "pager_config.json")


class PagerDispatcher:
    """
    Manages real-time on-call alerts, continuous failure tracking,
    and automatic escalation reminders when an outage is not recovering.
    """
    def __init__(self, reminder_interval_logs: int = 50, reminder_cooldown_sec: float = 45.0):
        self.reminder_interval_logs = reminder_interval_logs
        self.reminder_cooldown_sec = reminder_cooldown_sec
        self.active_outages: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        os.makedirs(REPORTS_DIR, exist_ok=True)
        self.webhook_url = self._load_webhook_url()

    def _load_webhook_url(self) -> Optional[str]:
        # 1. Environment variable priority
        env_url = os.environ.get("SYNAPSE_PAGER_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL")
        if env_url:
            return env_url
        # 2. Config file fallback
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                    return cfg.get("webhook_url")
            except Exception:
                pass
        return None

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
                "last_reminder_time": time.time()
            }

            self.active_outages[sig] = alert_record
            self._dispatch_channels(alert_record, is_reminder=False)
            self._persist_alert(alert_record)
            return alert_record

    def record_suppressed_failure(self, service: str, transient_sig: str) -> Optional[Dict[str, Any]]:
        """
        Called when Tier-4 intercepts an ongoing failure log.
        Tracks continuous failure volume and triggers escalation reminders
        if logs are failing continuously without resolution.
        """
        with self._lock:
            # Look up active outage by signature or focal service
            outage = self.active_outages.get(transient_sig)
            if not outage:
                for sig, rec in self.active_outages.items():
                    if rec["focal_service"] == service:
                        outage = rec
                        break

            if not outage or outage.get("acknowledged") or outage.get("status") == "RESOLVED":
                return None

            outage["failure_count"] += 1
            now = time.time()
            count = outage["failure_count"]
            time_since_last = now - outage.get("last_reminder_time", 0)

            # Check if reminder threshold reached (every N continuous logs & cooldown passed)
            if (count % self.reminder_interval_logs == 0) and (time_since_last >= self.reminder_cooldown_sec):
                outage["reminders_sent"] += 1
                outage["last_reminder_time"] = now

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
                    "reminders_sent": outage["reminders_sent"]
                }

                self._dispatch_channels(reminder_record, is_reminder=True)
                self._persist_alert(reminder_record)
                return reminder_record

            return None

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

    def _dispatch_channels(self, alert: Dict[str, Any], is_reminder: bool = False):
        """Dispatches the alert simultaneously across multiple communication channels."""
        # 1. Audible alert tone (Windows MessageBeep warning sound)
        try:
            if sys.platform == "win32":
                # MB_ICONWARNING (0x30) or MB_ICONHAND (0x10)
                ctypes.windll.user32.MessageBeep(0x00000010 if is_reminder else 0x00000030)
        except Exception:
            pass

        # 2. Native Windows Desktop Notification (Toast / Balloon)
        title = f"{'🔔 ESCALATION REMINDER' if is_reminder else '🚨 CRITICAL AI INCIDENT ALERT'} ({alert['incident_id']})"
        if is_reminder:
            body = f"Continuous failures! [{alert['continuous_failures_intercepted']} logs intercepted in {alert['focal_service']}]. Assigned: {alert['assigned_team']}"
        else:
            body = f"Service: [{alert['focal_service']}] | Paged: {alert['assigned_team']} | Cause: {alert['root_cause'][:90]}..."

        threading.Thread(target=self._send_windows_toast, args=(title, body), daemon=True).start()

        # 3. Webhook (Slack / Discord / Teams)
        if self.webhook_url:
            threading.Thread(target=self._send_webhook, args=(alert, is_reminder), daemon=True).start()

    def _send_windows_toast(self, title: str, message: str):
        """Renders native Windows notification pop-up asynchronously."""
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
            $notify.ShowBalloonTip(5000)
            Start-Sleep -Milliseconds 600
            $notify.Dispose()
            """
            subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], capture_output=True, timeout=5)
        except Exception:
            pass

    def _send_webhook(self, alert: Dict[str, Any], is_reminder: bool):
        """Sends rich formatted JSON card to Slack / Discord / Teams."""
        try:
            payload = {
                "text": f"*{'🔔 CONTINUOUS FAILURE REMINDER' if is_reminder else '🚨 SYNAPSELOG PAGER ALERT'}*",
                "attachments": [
                    {
                        "color": "#ff0000" if is_reminder else "#ff9900",
                        "title": f"{alert['incident_id']} - {alert['focal_service']}",
                        "fields": [
                            {"title": "Assigned Team", "value": f"{alert['assigned_team']} ({alert['oncall_pager']})", "short": True},
                            {"title": "Severity", "value": alert['severity'], "short": True},
                            {"title": "Root Cause", "value": alert.get('root_cause', 'N/A'), "short": False},
                            {"title": "Blast Radius", "value": str(alert.get('blast_radius', [])), "short": True},
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
        """Persists notification record into reports directory."""
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
                f.write(f"[{record.get('timestamp')}] {record.get('alert_type')} | {record.get('incident_id')} | Service: {record.get('focal_service')} | Team: {record.get('assigned_team')} | Pager: {record.get('oncall_pager')}\n")
        except Exception:
            pass
