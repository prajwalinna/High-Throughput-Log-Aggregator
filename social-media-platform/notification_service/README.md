# Social Media Platform Log Simulator

A Python project that simulates a social media platform consisting of three independent microservices. Each service continuously generates realistic JSON logs for testing log aggregation, monitoring, and observability pipelines.

## Project Structure

```
social-media-platform/
│
├── auth_service/
│   └── app.py              # Authentication & session events
│
├── post_service/
│   └── app.py              # Posts, comments, likes, media events
│
├── notification_service/
│   └── app.py              # Push, email, and in-app notifications
│
├── logs/
│   ├── auth.log            # Auth service output (JSON lines)
│   ├── post.log            # Post service output (JSON lines)
│   └── notification.log    # Notification service output (JSON lines)
│
├── requirements.txt
└── README.md
```

## Log Format

Each service writes one JSON object per line:

```json
{
    "timestamp": "2026-07-19T12:34:56Z",
    "service": "auth-service",
    "level": "INFO",
    "event": "USER_LOGIN",
    "message": "User 1045 logged in successfully",
    "user_id": 1045,
    "ip_address": "192.168.12.45",
    "device": "iPhone 15",
    "location": "New York, US",
    "response_time_ms": 42,
    "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

### Log Level Distribution

| Level   | Approximate Share |
|---------|-------------------|
| INFO    | 80%               |
| WARNING | 15%               |
| ERROR   | 5%                |

## Prerequisites

- Python 3.9 or higher
- No external packages required (stdlib only)

## Running a Single Service

From the `social-media-platform` directory:

```bash
python auth_service/app.py
python post_service/app.py
python notification_service/app.py
```

Each service runs indefinitely, writing logs every 0.5–2 seconds. Press `Ctrl+C` to stop.

## Running All Three Services Simultaneously

### Windows (PowerShell)

Open three separate terminal windows and run one command in each:

```powershell
cd social-media-platform
python auth_service/app.py
```

```powershell
cd social-media-platform
python post_service/app.py
```

```powershell
cd social-media-platform
python notification_service/app.py
```

Or start all three in the background from one PowerShell session:

```powershell
cd social-media-platform
Start-Process python -ArgumentList "auth_service/app.py"
Start-Process python -ArgumentList "post_service/app.py"
Start-Process python -ArgumentList "notification_service/app.py"
```

### Linux / macOS

Open three terminal tabs or use background processes:

```bash
cd social-media-platform

python auth_service/app.py &
python post_service/app.py &
python notification_service/app.py &
```

## Viewing Live Logs

**Windows (PowerShell):**

```powershell
Get-Content logs\auth.log -Wait -Tail 5
Get-Content logs\post.log -Wait -Tail 5
Get-Content logs\notification.log -Wait -Tail 5
```

**Linux / macOS:**

```bash
tail -f logs/auth.log
tail -f logs/post.log
tail -f logs/notification.log
```

## Services Overview

| Service                | Events Generated                                                                 |
|------------------------|----------------------------------------------------------------------------------|
| **auth-service**       | USER_LOGIN, USER_LOGOUT, PASSWORD_RESET, TOKEN_REFRESH, TWO_FACTOR_SUCCESS, etc. |
| **post-service**       | POST_CREATED, COMMENT_ADDED, LIKE_ADDED, MEDIA_UPLOADED, TRENDING_POST, etc.     |
| **notification-service** | PUSH_SENT, EMAIL_SENT, FOLLOW_NOTIFICATION, MENTION_NOTIFICATION, etc.         |

The `logs/` directory is created automatically on first run if it does not exist.
