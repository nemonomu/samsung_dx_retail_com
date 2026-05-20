@echo off
setlocal

cd /d "%~dp0"
if not exist logs mkdir logs

set RUN_DATE=%~1
if "%RUN_DATE%"=="" set RUN_DATE=20260519
set BATCH_ID=%~2
if "%BATCH_ID%"=="" set BATCH_ID=b_20260519_220052

set BESTBUY_RUN_DATE=%RUN_DATE%
set BESTBUY_BATCH_ID=%BATCH_ID%
set BESTBUY_OUTPUT_TABLE_TV=tv_retail_com
set BESTBUY_PRODUCT_LIST_TABLE_TV=bby_tv_product_list
set BESTBUY_FINAL_TARGET_SIZE=0
set BESTBUY_MAIN_RANK_LIMIT=300
set BESTBUY_DETAIL_REBUILD_ONLY=
set BESTBUY_DETAIL_RETRY_ONLY=1
set BESTBUY_DETAIL_LIMIT=
set BESTBUY_DETAIL_WORKERS=3
set BESTBUY_DETAIL_MAX_ATTEMPTS=5
set BESTBUY_EXCLUDED_PROMOTION_TYPES=Featured deals
set ZENROWS_TIMEOUT=240

set LOG=logs\bestbuy_tv_fix_%RUN_DATE%.log

echo ===== TV fix start %date% %time% run_date=%RUN_DATE% batch_id=%BATCH_ID% =====
echo log=%LOG%
echo target_size=%BESTBUY_FINAL_TARGET_SIZE% excluded_promotion_types=%BESTBUY_EXCLUDED_PROMOTION_TYPES%

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Continue';" ^
  "$cmd='python -u -m bestbuy.bestbuy_orchestrator --category TV 07 08 09 10 14 15';" ^
  "Write-Output ('[cmd] ' + $cmd);" ^
  "cmd /c $cmd 2>&1 | Tee-Object -FilePath '%LOG%';" ^
  "exit $LASTEXITCODE"

set EXITCODE=%ERRORLEVEL%
echo ===== TV fix end %date% %time% exit=%EXITCODE% =====
exit /b %EXITCODE%
