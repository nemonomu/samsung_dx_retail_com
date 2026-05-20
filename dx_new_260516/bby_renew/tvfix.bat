@echo off
setlocal

cd /d "%~dp0"
if not exist logs mkdir logs

set RUN_DATE=%~1
if "%RUN_DATE%"=="" set RUN_DATE=20260519

set BESTBUY_RUN_DATE=%RUN_DATE%
set BESTBUY_FINAL_TARGET_SIZE=300
set BESTBUY_DETAIL_REBUILD_ONLY=1
set BESTBUY_DETAIL_RETRY_ONLY=
set BESTBUY_DETAIL_LIMIT=
set BESTBUY_DETAIL_WORKERS=3
set BESTBUY_DETAIL_MAX_ATTEMPTS=5
set ZENROWS_TIMEOUT=240

set LOG=logs\bestbuy_tv_fix_%RUN_DATE%.log

echo ===== TV fix start %date% %time% run_date=%RUN_DATE% =====
echo log=%LOG%

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Continue';" ^
  "$cmd='python -u -m bestbuy.bestbuy_orchestrator --category TV 07 08 10 14 15';" ^
  "Write-Output ('[cmd] ' + $cmd);" ^
  "cmd /c $cmd 2>&1 | Tee-Object -FilePath '%LOG%';" ^
  "exit $LASTEXITCODE"

set EXITCODE=%ERRORLEVEL%
echo ===== TV fix end %date% %time% exit=%EXITCODE% =====
exit /b %EXITCODE%
