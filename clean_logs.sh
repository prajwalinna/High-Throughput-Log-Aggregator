#!/bin/bash
echo "Stopping Docker containers and deleting ALL their storage volumes (Loki, Kafka)..."
docker compose down -v

echo "Deleting S3 cold-storage backups from Floci..."
rm -rf floci-data/*

echo "Deleting any leftover local fallback log files..."
rm -f social-media-platform/logs/*.log

echo "Done! Your disk is completely clean."
echo "To start everything fresh, just run:"
echo "  docker compose up -d"
echo "  ./run_services.sh"
