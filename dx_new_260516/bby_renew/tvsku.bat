@echo off
setlocal

cd /d "%~dp0"
if not exist logs mkdir logs

set RUN_DATE=%~1
if "%RUN_DATE%"=="" set RUN_DATE=20260519

set TARGETS=%~2
if "%TARGETS%"=="" set TARGETS=J3ZYG2V4Y2,J3ZYGXCL3J,JJGRF3688Y,JJ8VPZTRCL,JJ8VPZW5KG,J3ZYG2FZPF,J3ZYG2V9P3,J3ZYG2VWG2,J3ZYG2VWGP,JJ8VPZKFYQ,J3ZYG2HVJ9,J3ZYG2HL2Q,J3ZYG2H8VC,J3ZYG2FZP8,J3Z9Z42R49,J3ZYG2VFX5,J3ZYG2HRV8

set BESTBUY_RUN_DATE=%RUN_DATE%
set BESTBUY_DETAIL_SKUS=%TARGETS%
set BESTBUY_DETAIL_REBUILD_ONLY=
set BESTBUY_DETAIL_RETRY_ONLY=
set BESTBUY_DETAIL_LIMIT=
set BESTBUY_DETAIL_WORKERS=2
set BESTBUY_DETAIL_MAX_ATTEMPTS=3
set ZENROWS_TIMEOUT=240

set LOG=logs\bestbuy_tv_sku_%RUN_DATE%.log

echo ===== TV SKU refresh start %date% %time% run_date=%RUN_DATE% =====
echo targets=%TARGETS%
echo log=%LOG%

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Continue';" ^
  "$cmd='python -u -m bestbuy.bestbuy_orchestrator --category TV 08 10 14';" ^
  "Write-Output ('[cmd] ' + $cmd);" ^
  "cmd /c $cmd 2>&1 | Tee-Object -FilePath '%LOG%';" ^
  "exit $LASTEXITCODE"

set EXITCODE=%ERRORLEVEL%
echo ===== TV SKU refresh end %date% %time% exit=%EXITCODE% =====
exit /b %EXITCODE%
