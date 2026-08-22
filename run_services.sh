#!/bin/bash

echo "Starting social media microservices..."

echo "Setting up virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install kafka-python -q

# Set PYTHONPATH so services can find the shared module
export PYTHONPATH=$PWD

# Start all three services in the background
python social-media-platform/auth_service/app.py &
AUTH_PID=$!

python social-media-platform/post_service/app.py &
POST_PID=$!

python social-media-platform/notification_service/app.py &
NOTIF_PID=$!

echo "All services are running! Generating logs in social-media-platform/logs/"
echo "Press Ctrl+C to stop all services."

# Trap Ctrl+C to kill all background processes
trap "echo 'Stopping services...'; kill $AUTH_PID $POST_PID $NOTIF_PID; exit" SIGINT

# Wait for background processes to prevent the script from exiting immediately
wait
