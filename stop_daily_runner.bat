@echo off
setlocal

cd /d "%~dp0"

if not exist "reports" mkdir "reports"

echo stop > "reports\STOP_DAILY_RUNNER"
echo Stop signal written: reports\STOP_DAILY_RUNNER
echo The runner will stop within about 60 seconds.

endlocal
