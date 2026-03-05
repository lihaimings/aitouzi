@echo off
setlocal

cd /d "%~dp0"

if not exist "logs" mkdir "logs"
if not exist "reports" mkdir "reports"

echo Starting daily runner...
echo Log file: logs\daily_runner.log

start "aitouzi-daily-runner" cmd /k "python scripts\daemon_daily_runner.py --time 18:30 --run-on-start"

echo Started. You can close this window.
endlocal
