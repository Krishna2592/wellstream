REM filepath: c:\Development\Projects\wellstream\spark-processor\run.bat
@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo Building Docker image for Spark Pipeline...
docker build -f Dockerfile -t wellstream-spark:latest .

echo.
echo Starting Spark Pipeline in Docker...
docker run -it --rm ^
  --network streaming-network ^
  --name spark-pipeline ^
  -v %CD%\target:/app/target ^
  wellstream-spark:latest

pause