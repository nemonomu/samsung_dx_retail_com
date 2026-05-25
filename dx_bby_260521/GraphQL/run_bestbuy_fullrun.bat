@echo off
setlocal EnableExtensions

cd /d "%~dp0"

set "CATEGORY=%~1"
if "%CATEGORY%"=="" set "CATEGORY=TV"
set "CATEGORY=%CATEGORY:"=%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "RUN_DATE=%%i"
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "RUN_TS=%%i"
for /f %%i in ('powershell -NoProfile -Command "'%CATEGORY%'.ToLowerInvariant()"') do set "CATEGORY_DIR=%%i"
for /f %%i in ('powershell -NoProfile -Command "$categoryRoot = Join-Path '%~dp0bestbuy\data' '%CATEGORY_DIR%'; $baseName = '%RUN_DATE%'; $candidate = Join-Path $categoryRoot $baseName; if (-not (Test-Path -LiteralPath $candidate)) { $baseName } else { $n = 2; do { $name = '{0}_{1}' -f $baseName, $n; $candidate = Join-Path $categoryRoot $name; $n++ } while (Test-Path -LiteralPath $candidate); $name }"') do set "RUN_FOLDER=%%i"

set "BESTBUY_CATEGORY=%CATEGORY%"
set "BESTBUY_RUN_DATE=%RUN_FOLDER%"
set "BESTBUY_RUN_ROOT=%~dp0bestbuy\data\%CATEGORY_DIR%\%RUN_FOLDER%"
set "BESTBUY_BATCH_ID=b_%RUN_TS%"
set "BESTBUY_FETCH_MODE=zenrows"
set "BESTBUY_GRAPHQL_FETCH_MODE=zenrows"
set "BESTBUY_DETAIL_FETCH_MODE=zenrows"
set "BESTBUY_DETAIL_FETCH_COMPARE=1"
set "BESTBUY_DETAIL_SKU_BATCH_SIZE=5"
set "BESTBUY_DETAIL_WORKERS=3"
set "BESTBUY_AVAILABILITY_BACKFILL_BATCH_ID=%BESTBUY_BATCH_ID%"
set "BESTBUY_AVAILABILITY_BACKFILL_CHUNK_SIZE=1"
set "BESTBUY_AVAILABILITY_BACKFILL_ALLOW_MULTI_SKU=0"
set "BESTBUY_AVAILABILITY_BACKFILL_CANDIDATE_MODE=missing_any"
set "BESTBUY_FORCE_STEP_ENV=1"
set "PYTHONUNBUFFERED=1"

set "LOG_DIR=%BESTBUY_RUN_ROOT%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\fullrun_%RUN_TS%.log"

echo ==================================================
echo BestBuy %CATEGORY% full run started
echo batch_id=%BESTBUY_BATCH_ID%
echo run_folder=%BESTBUY_RUN_DATE%
echo run_root=%BESTBUY_RUN_ROOT%
echo log=%LOG_FILE%
echo ==================================================
echo BestBuy %CATEGORY% full run started > "%LOG_FILE%"
echo batch_id=%BESTBUY_BATCH_ID% >> "%LOG_FILE%"
echo run_folder=%BESTBUY_RUN_DATE% >> "%LOG_FILE%"
echo run_root=%BESTBUY_RUN_ROOT% >> "%LOG_FILE%"

call :run_step 01 13 "main_list" 01
if errorlevel 1 goto :fail
call :run_step 02 13 "main_targets" 02
if errorlevel 1 goto :fail
call :run_step 03 13 "bsr_list" 03
if errorlevel 1 goto :fail
call :run_step 04 13 "bsr_rank" 04
if errorlevel 1 goto :fail
call :run_step 05 13 "promotion_deals" 05
if errorlevel 1 goto :fail
call :run_step 06 13 "trending_deals" 06
if errorlevel 1 goto :fail
call :run_step 07 13 "final_targets" 07
if errorlevel 1 goto :fail
call :run_step 08 13 "detail_html" 08
if errorlevel 1 goto :fail
call :run_step 09 13 "review20" 09
if errorlevel 1 goto :fail
call :run_step 10 13 "availability_backfill" 10
if errorlevel 1 goto :fail
call :run_step 11 13 "status_check" 11
if errorlevel 1 goto :fail

call :run_step 12 13 "db_prepare" 14
if errorlevel 1 goto :fail
call :run_step 13 13 "db_load" 15
if errorlevel 1 goto :fail

echo ==================================================
echo BestBuy %CATEGORY% full run completed
echo log=%LOG_FILE%
echo ==================================================
echo BestBuy %CATEGORY% full run completed >> "%LOG_FILE%"
exit /b 0

:run_step
set "CUR=%~1"
set "TOTAL=%~2"
set "NAME=%~3"
set "STEP=%~4"
echo.
echo [%CUR%/%TOTAL%] %NAME% started
echo [%CUR%/%TOTAL%] %NAME% started >> "%LOG_FILE%"
powershell -NoProfile -ExecutionPolicy Bypass -Command "& python -m bestbuy.bestbuy_orchestrator --category '%CATEGORY%' '%STEP%' 2>&1 | Tee-Object -FilePath '%LOG_FILE%' -Append"
if errorlevel 1 (
  echo [%CUR%/%TOTAL%] %NAME% failed
  echo [%CUR%/%TOTAL%] %NAME% failed >> "%LOG_FILE%"
  exit /b 1
)
echo [%CUR%/%TOTAL%] %NAME% completed
echo [%CUR%/%TOTAL%] %NAME% completed >> "%LOG_FILE%"
exit /b 0

:fail
echo.
echo BestBuy %CATEGORY% full run failed. See log: %LOG_FILE%
echo BestBuy %CATEGORY% full run failed >> "%LOG_FILE%"
exit /b 1
