@echo off
setlocal EnableExtensions

cd /d "%~dp0"
if not exist logs mkdir logs

set RUN_DATE=%~1
if "%RUN_DATE%"=="" set RUN_DATE=20260521

set BATCH_ID=%~2
if "%BATCH_ID%"=="" set BATCH_ID=b_20260521_000000

set CRAWL_DATETIME=%~3
if "%CRAWL_DATETIME%"=="" set CRAWL_DATETIME=2026-05-21 00:30:00
shift /1
shift /1
shift /1
set EXTRA_ARGS=
:collect_extra_args
if "%~1"=="" goto extra_args_done
set EXTRA_ARGS=%EXTRA_ARGS% %~1
shift /1
goto collect_extra_args
:extra_args_done

set BESTBUY_CATEGORY=TV
set BESTBUY_RUN_DATE=%RUN_DATE%
set BESTBUY_BATCH_TIME=
set BESTBUY_BATCH_ID=%BATCH_ID%
set BESTBUY_CRAWL_DATETIME=%CRAWL_DATETIME%
set BESTBUY_POSTAL_CODE=10010
set BESTBUY_ZIP_CODE=10010

set BESTBUY_RUN_ROOT=
set BESTBUY_TRENDING_LIMIT=10
set BESTBUY_TRENDING_HTML=
set BESTBUY_TRENDING_RUN_ROOT=
set BESTBUY_TRENDING_OUTPUT=

set BESTBUY_FORCE_REFRESH=0
set BESTBUY_FORCE_STEP_ENV=1
set BESTBUY_FINAL_TARGET_SIZE=0
set BESTBUY_DETAIL_LIMIT=
set BESTBUY_DETAIL_WORKERS=3
set BESTBUY_OUTPUT_TABLE_TV=tv_retail_com
set BESTBUY_PRODUCT_LIST_TABLE_TV=bby_tv_product_list
set ZENROWS_TIMEOUT=240

set STEPS=06 07 08 09 10 13 14 15
set LOG=logs\bestbuy_tv_update_%RUN_DATE%_%BATCH_ID%.log

echo ===== TV update start %date% %time% =====
echo run_date=%BESTBUY_RUN_DATE%
echo batch_id=%BESTBUY_BATCH_ID%
echo crawl_datetime=%BESTBUY_CRAWL_DATETIME%
echo postal_code=%BESTBUY_POSTAL_CODE%
echo trending_limit=%BESTBUY_TRENDING_LIMIT%
echo log=%LOG%
echo steps=%STEPS%
if not "%EXTRA_ARGS%"=="" echo extra_args=%EXTRA_ARGS%

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Continue';" ^
  "$cmd='python -u -m bestbuy.bestbuy_orchestrator --category TV %STEPS% %EXTRA_ARGS%';" ^
  "Write-Output ('[cmd] ' + $cmd);" ^
  "cmd /c $cmd 2>&1 | Tee-Object -FilePath '%LOG%';" ^
  "exit $LASTEXITCODE"

set EXITCODE=%ERRORLEVEL%
echo ===== TV update end %date% %time% exit=%EXITCODE% =====
exit /b %EXITCODE%
