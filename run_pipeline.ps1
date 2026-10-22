# SynapseLog One-Click PowerShell Launcher
$ErrorActionPreference = "Stop"

Write-Host "=======================================================================" -ForegroundColor Cyan
Write-Host "  SYNAPSELOG: HIERARCHICAL AI LOG ANOMALY DETECTION & TRIAGE PIPELINE" -ForegroundColor Cyan
Write-Host "=======================================================================" -ForegroundColor Cyan
Write-Host ""

# Ensure Docker is in PATH
$dockerBin = "C:\Users\prajw\AppData\Local\Programs\DockerDesktop\resources\bin"
if (Test-Path $dockerBin) {
    $env:PATH = "$dockerBin;" + $env:PATH
}

# Verify Docker
try {
    docker ps | Out-Null
    Write-Host "[1/3] Docker Desktop is active." -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Docker Desktop is not running. Please start Docker Desktop." -ForegroundColor Red
    exit 1
}

# Start Kafka
Write-Host "[2/3] Ensuring Apache Kafka container is up..." -ForegroundColor Yellow
docker compose up -d kafka

# Launch Orchestrator
Write-Host "[3/3] Launching Master SynapseLog Orchestrator (RTX 2050 CUDA)..." -ForegroundColor Green
Write-Host ""

python synapse_orchestrator.py
