@echo off
title SynapseLog AI Stream Pipeline (RTX 2050 CUDA + Kafka)
color 0A

echo =======================================================================
echo   SYNAPSELOG: HIERARCHICAL AI LOG ANOMALY DETECTION ^& TRIAGE PIPELINE
echo =======================================================================
echo.

:: Add Docker Desktop binaries to PATH
set "PATH=C:\Users\prajw\AppData\Local\Programs\DockerDesktop\resources\bin;%PATH%"

:: Check if Docker is running
docker ps >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker is not running or docker.exe not found!
    echo Please make sure Docker Desktop is started.
    pause
    exit /b 1
)

echo [1/3] Docker Desktop detected.
echo [2/3] Checking Apache Kafka container status...
docker compose up -d kafka

echo [3/3] Launching Master SynapseLog Orchestrator...
echo.
python synapse_orchestrator.py

pause
