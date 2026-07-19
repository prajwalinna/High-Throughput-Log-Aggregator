import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

SERVICE_NAME = "auth-service"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "auth.log")

DEVICES = ["iPhone 15", "Samsung Galaxy S24", "Pixel 8", "MacBook Pro", "Windows PC", "iPad Air"]
LOCATIONS = ["New York, US", "London, UK", "Tokyo, JP", "Berlin, DE", "Sydney, AU", "Toronto, CA"]
IP_PREFIXES = ["192.168.", "10.0.", "172.16.", "203.0."]

INFO_EVENTS = [
    ("USER_LOGIN", "User {user_id} logged in successfully"),
    ("USER_LOGOUT", "User {user_id} logged out"),
    ("PASSWORD_RESET", "Password reset initiated for user {user_id}"),
    ("TOKEN_REFRESH", "Access token refreshed for user {user_id}"),
    ("INVALID_PASSWORD", "Invalid password attempt for user {user_id}"),
    ("ACCOUNT_LOCKED", "Account locked for user {user_id} after failed attempts"),
    ("ACCOUNT_CREATED", "New account created for user {user_id}"),
    ("SESSION_EXPIRED", "Session expired for user {user_id}"),
    ("TWO_FACTOR_SUCCESS", "Two-factor authentication succeeded for user {user_id}"),
    ("TWO_FACTOR_FAILED", "Two-factor authentication failed for user {user_id}"),
]

WARNING_EVENTS = [
    ("Too many failed login attempts", "Too many failed login attempts for user {user_id}"),
    ("Suspicious login location", "Suspicious login location detected for user {user_id}"),
    ("Password expires soon", "Password for user {user_id} expires in 3 days"),
]

ERROR_EVENTS = [
    ("Database unavailable", "Database connection failed during authentication"),
    ("JWT generation failed", "Failed to generate JWT token for user {user_id}"),
    ("Authentication service timeout", "Authentication request timed out for user {user_id}"),
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
        "ip_address": random_ip(),
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
        "response_time_ms": random.randint(10, 500),
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
    logger.info("Auth service started", extra={"event": "SERVICE_STARTED", **base_metadata()})

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
