@echo off
title SynapseLog System Cleanup ^& Resource Teardown
color 0C

echo =======================================================================
echo   SYNAPSELOG: SYSTEM CLEANUP ^& RESOURCE RELEASE
echo =======================================================================
echo.
echo Stopping all running background containers and removing temporary files...
echo.

:: Add Docker Desktop binaries to PATH
set "PATH=C:\Users\prajw\AppData\Local\Programs\DockerDesktop\resources\bin;%PATH%"

:: 1. Stop and Remove Docker Containers
echo [1/4] Stopping Docker Kafka, Loki, and Grafana containers...
docker compose down -v >nul 2>&1
docker rm -f kafka floci loki grafana storage-consumer analysis-consumer s3-consumer >nul 2>&1

:: 2. Clean temporary fallback logs and report logs
echo [2/4] Removing temporary runtime logs...
if exist "social-media-platform\logs" (
    del /q /f "social-media-platform\logs\*.*" >nul 2>&1
)
del /q /f "*.log" >nul 2>&1
del /q /f "reports\incident_audit.log" >nul 2>&1

:: 3. Remove Python caches
echo [3/4] Cleaning Python bytecode (__pycache__)...
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" >nul 2>&1

:: 4. Done
echo [4/4] All background processes stopped and system resources released!
echo.
echo =======================================================================
echo   CLEANUP COMPLETE: Your system is 100%% light and clear.
echo =======================================================================
echo.
pause
