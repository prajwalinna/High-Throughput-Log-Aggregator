# SynapseLog Full Cleanup & Resource Teardown Script
$ErrorActionPreference = "SilentlyContinue"

Write-Host "=======================================================================" -ForegroundColor Red
Write-Host "  SYNAPSELOG: SYSTEM CLEANUP & RESOURCE RELEASE" -ForegroundColor Red
Write-Host "=======================================================================" -ForegroundColor Red
Write-Host ""

$dockerBin = "C:\Users\prajw\AppData\Local\Programs\DockerDesktop\resources\bin"
if (Test-Path $dockerBin) {
    $env:PATH = "$dockerBin;" + $env:PATH
}

# 1. Stop Docker Containers
Write-Host "[1/4] Stopping and removing Docker containers..." -ForegroundColor Yellow
docker compose down -v 2>$null
docker rm -f kafka floci loki grafana storage-consumer analysis-consumer s3-consumer 2>$null

# 2. Clean temporary fallback logs
Write-Host "[2/4] Removing temporary log files..." -ForegroundColor Yellow
Remove-Item -Path "social-media-platform\logs\*" -Force -Recurse 2>$null
Remove-Item -Path "*.log" -Force 2>$null

# 3. Clean Python bytecode
Write-Host "[3/4] Cleaning Python bytecode caches..." -ForegroundColor Yellow
Get-ChildItem -Path . -Recurse -Directory -Filter "__pycache__" | Remove-Item -Force -Recurse 2>$null

Write-Host "[4/4] Done! All background containers stopped and memory released." -ForegroundColor Green
Write-Host ""
