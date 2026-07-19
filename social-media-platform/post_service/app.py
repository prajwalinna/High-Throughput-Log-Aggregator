import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

SERVICE_NAME = "post-service"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_FILE = os.path.join(LOG_DIR, "post.log")

DEVICES = ["iPhone 15", "Samsung Galaxy S24", "Pixel 8", "MacBook Pro", "Windows PC", "iPad Air"]
LOCATIONS = ["New York, US", "London, UK", "Tokyo, JP", "Berlin, DE", "Sydney, AU", "Toronto, CA"]
IP_PREFIXES = ["192.168.", "10.0.", "172.16.", "203.0."]

INFO_EVENTS = [
    ("POST_CREATED", "User {user_id} created post {post_id}"),
    ("POST_EDITED", "User {user_id} edited post {post_id}"),
    ("POST_DELETED", "User {user_id} deleted post {post_id}"),
    ("COMMENT_ADDED", "User {user_id} added comment on post {post_id}"),
    ("COMMENT_DELETED", "User {user_id} deleted comment on post {post_id}"),
    ("LIKE_ADDED", "User {user_id} liked post {post_id}"),
    ("LIKE_REMOVED", "User {user_id} removed like from post {post_id}"),
    ("MEDIA_UPLOADED", "User {user_id} uploaded media to post {post_id}"),
    ("MEDIA_DELETED", "User {user_id} deleted media from post {post_id}"),
    ("TRENDING_POST", "Post {post_id} is now trending"),
]

WARNING_EVENTS = [
    ("Large media upload", "Large media upload detected for post {post_id} by user {user_id}"),
    ("Spam detection triggered", "Spam detection triggered for user {user_id} on post {post_id}"),
    ("High posting frequency", "High posting frequency detected for user {user_id}"),
]

ERROR_EVENTS = [
    ("Media upload failed", "Media upload failed for post {post_id} by user {user_id}"),
    ("Database write failed", "Database write failed while saving post {post_id}"),
    ("Image processing timeout", "Image processing timed out for post {post_id}"),
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
        "ip_address": random_ip(),
        "device": random.choice(DEVICES),
        "location": random.choice(LOCATIONS),
        "response_time_ms": random.randint(15, 800),
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
    logger.info("Post service started", extra={"event": "SERVICE_STARTED", **base_metadata()})

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
