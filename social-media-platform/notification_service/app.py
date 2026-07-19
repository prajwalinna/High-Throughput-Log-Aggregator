import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

SERVICE_NAME = "notification-service"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "notification.log")

DEVICES = ["iPhone 15", "Samsung Galaxy S24", "Pixel 8", "MacBook Pro", "Windows PC", "iPad Air"]
LOCATIONS = ["New York, US", "London, UK", "Tokyo, JP", "Berlin, DE", "Sydney, AU", "Toronto, CA"]
IP_PREFIXES = ["192.168.", "10.0.", "172.16.", "203.0."]

INFO_EVENTS = [
    ("PUSH_SENT", "Push notification {notification_id} sent to user {user_id}"),
    ("EMAIL_SENT", "Email notification {notification_id} sent to user {user_id}"),
    ("FOLLOW_NOTIFICATION", "Follow notification {notification_id} sent to user {user_id}"),
    ("LIKE_NOTIFICATION", "Like notification {notification_id} sent to user {user_id}"),
    ("COMMENT_NOTIFICATION", "Comment notification {notification_id} sent to user {user_id}"),
    ("MESSAGE_NOTIFICATION", "Message notification {notification_id} sent to user {user_id}"),
    ("MENTION_NOTIFICATION", "Mention notification {notification_id} sent to user {user_id}"),
]

WARNING_EVENTS = [
    ("Notification queue growing", "Notification queue size exceeded threshold for user {user_id}"),
    ("Retrying notification", "Retrying delivery of notification {notification_id} for user {user_id}"),
]

ERROR_EVENTS = [
    ("SMTP server unavailable", "SMTP server unavailable while sending notification {notification_id}"),
    ("Push notification service down", "Push service down for notification {notification_id}"),
    ("Notification delivery failed", "Failed to deliver notification {notification_id} to user {user_id}"),
]


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "service": SERVICE_NAME,
            "level": record.levelname,
            "event": getattr(record, "event", "UNKNOWN"),
            "message": record.getMessage(),
        }

        for field in (
            "user_id",
            "post_id",
            "notification_id",
            "ip_address",
            "device",
            "location",
            "response_time_ms",
            "request_id",
        ):
            value = getattr(record, field, None)
            if value is not None:
                log_entry[field] = value

        return json.dumps(log_entry)


def setup_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(SERVICE_NAME)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    return logger


def random_ip() -> str:
    return f"{random.choice(IP_PREFIXES)}{random.randint(0, 255)}.{random.randint(0, 255)}"


def base_metadata() -> dict:
    return {
        "user_id": random.randint(1000, 9999),
        "post_id": random.randint(10000, 99999),
        "notification_id": random.randint(100000, 999999),
        "ip_address": random_ip(),
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
        "response_time_ms": random.randint(20, 600),
        "request_id": str(uuid.uuid4()),
    }


def pick_level() -> str:
    roll = random.random()
    if roll < 0.80:
        return "INFO"
    if roll < 0.95:
        return "WARNING"
    return "ERROR"


def build_log_payload(level: str) -> tuple[str, dict]:
    metadata = base_metadata()

    if level == "INFO":
        event, template = random.choice(INFO_EVENTS)
    elif level == "WARNING":
        event, template = random.choice(WARNING_EVENTS)
    else:
        event, template = random.choice(ERROR_EVENTS)

    message = template.format(**metadata)
    extra = {**metadata, "event": event}
    return message, extra


def run() -> None:
    logger = setup_logger()
    logger.info(
        "Notification service started",
        extra={"event": "SERVICE_STARTED", **base_metadata()},
    )

    while True:
        level = pick_level()
        message, extra = build_log_payload(level)

        if level == "INFO":
            logger.info(message, extra=extra)
        elif level == "WARNING":
            logger.warning(message, extra=extra)
        else:
            logger.error(message, extra=extra)

        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    run()
