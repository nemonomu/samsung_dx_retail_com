@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"
set "BBY_ROOT=%CD%"
set "CHAIN_NAME=BBY_TV_HHP_LOWES_REF_LDY"
set "CHAIN_ORDER=BBY:TV HHP / LOWES:REF LDY"
set "OVERALL_EXIT_CODE=0"
set "FAILURE_COUNT=0"
set "FAILURES="

set "CHAIN_LOG_DIR=%BBY_ROOT%\task_logs"
if not exist "%CHAIN_LOG_DIR%" mkdir "%CHAIN_LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "CHAIN_TS=%%i"
for /f %%i in ('powershell -NoProfile -Command "[int][double]::Parse((Get-Date -UFormat %%s))"') do set "CHAIN_START_EPOCH=%%i"
set "CHAIN_LOG=%CHAIN_LOG_DIR%\bby_tv_hhp_lowes_ref_ldy_%CHAIN_TS%.log"

echo ==================================================
echo BBY TV+HHP + Lowes REF+LDY daily task started
echo order=%CHAIN_ORDER%
echo continue_on_category_failure=true
echo bby_root=%BBY_ROOT%
echo chain_log=%CHAIN_LOG%
echo ==================================================
(
  echo ==================================================
  echo BBY TV+HHP + Lowes REF+LDY daily task started
  echo order=%CHAIN_ORDER%
  echo continue_on_category_failure=true
  echo bby_root=%BBY_ROOT%
  echo chain_log=%CHAIN_LOG%
  echo ==================================================
) > "%CHAIN_LOG%"

call :run_bestbuy_category TV
call :record_failure BBY_TV !CATEGORY_EXIT!

call :run_bestbuy_category HHP
call :record_failure BBY_HHP !CATEGORY_EXIT!

call :resolve_lowes_task
if defined LOWES_TASK_BAT (
  call :run_lowes_group
  call :record_failure LOWES_REF_LDY !GROUP_EXIT!
) else (
  echo [error] Lowes task bat missing. Set LOWES_REF_LDY_DAILY_TASK_BAT or place lowes_ref_ldy_daily_task.bat under the expected Lowes folder.
  >>"%CHAIN_LOG%" echo [error] Lowes task bat missing. Set LOWES_REF_LDY_DAILY_TASK_BAT or place lowes_ref_ldy_daily_task.bat under the expected Lowes folder.
  call :record_failure LOWES_REF_LDY 9002
)

for /f %%i in ('powershell -NoProfile -Command "[int][double]::Parse((Get-Date -UFormat %%s))"') do set "CHAIN_END_EPOCH=%%i"
set /a CHAIN_ELAPSED_SEC=CHAIN_END_EPOCH-CHAIN_START_EPOCH

echo.
echo ==================================================
>>"%CHAIN_LOG%" echo.
>>"%CHAIN_LOG%" echo ==================================================
if "%FAILURE_COUNT%"=="0" (
  echo BBY TV+HHP + Lowes REF+LDY daily task completed successfully
  >>"%CHAIN_LOG%" echo BBY TV+HHP + Lowes REF+LDY daily task completed successfully
) else (
  echo BBY TV+HHP + Lowes REF+LDY daily task completed with failures
  echo failed_count=%FAILURE_COUNT%
  echo failures=%FAILURES%
  >>"%CHAIN_LOG%" echo BBY TV+HHP + Lowes REF+LDY daily task completed with failures
  >>"%CHAIN_LOG%" echo failed_count=%FAILURE_COUNT%
  >>"%CHAIN_LOG%" echo failures=%FAILURES%
)
echo elapsed_sec=%CHAIN_ELAPSED_SEC%
echo chain_log=%CHAIN_LOG%
echo ==================================================
>>"%CHAIN_LOG%" echo elapsed_sec=%CHAIN_ELAPSED_SEC%
>>"%CHAIN_LOG%" echo chain_log=%CHAIN_LOG%
>>"%CHAIN_LOG%" echo ==================================================
exit /b %OVERALL_EXIT_CODE%

:run_bestbuy_category
set "CATEGORY=%~1"
set "CATEGORY_EXIT=0"
echo.
echo ==================================================
echo BestBuy %CATEGORY% daily task started
echo ==================================================
>>"%CHAIN_LOG%" echo.
>>"%CHAIN_LOG%" echo ==================================================
>>"%CHAIN_LOG%" echo BestBuy %CATEGORY% daily task started
>>"%CHAIN_LOG%" echo ==================================================
call :clear_bestbuy_env
if "%BBY_TV_HHP_LOWES_REF_LDY_DRY_RUN%"=="1" (
  echo [dry-run] call "%BBY_ROOT%\_bby_daily_task.bat" %CATEGORY%
  >>"%CHAIN_LOG%" echo [dry-run] call "%BBY_ROOT%\_bby_daily_task.bat" %CATEGORY%
  exit /b 0
)
call "%BBY_ROOT%\_bby_daily_task.bat" %CATEGORY%
set "CATEGORY_EXIT=!ERRORLEVEL!"
echo BestBuy %CATEGORY% daily task exit_code=!CATEGORY_EXIT!
>>"%CHAIN_LOG%" echo BestBuy %CATEGORY% daily task exit_code=!CATEGORY_EXIT!
exit /b !CATEGORY_EXIT!

:run_lowes_group
set "GROUP_EXIT=0"
echo.
echo ==================================================
echo Lowes REF+LDY group started
echo task_bat=%LOWES_TASK_BAT%
echo ==================================================
>>"%CHAIN_LOG%" echo.
>>"%CHAIN_LOG%" echo ==================================================
>>"%CHAIN_LOG%" echo Lowes REF+LDY group started
>>"%CHAIN_LOG%" echo task_bat=%LOWES_TASK_BAT%
>>"%CHAIN_LOG%" echo ==================================================
call :clear_bestbuy_env
call :clear_lowes_env
if "%BBY_TV_HHP_LOWES_REF_LDY_DRY_RUN%"=="1" (
  echo [dry-run] call "%LOWES_TASK_BAT%"
  >>"%CHAIN_LOG%" echo [dry-run] call "%LOWES_TASK_BAT%"
  exit /b 0
)
call "%LOWES_TASK_BAT%"
set "GROUP_EXIT=!ERRORLEVEL!"
echo Lowes REF+LDY group exit_code=!GROUP_EXIT!
>>"%CHAIN_LOG%" echo Lowes REF+LDY group exit_code=!GROUP_EXIT!
exit /b !GROUP_EXIT!

:resolve_lowes_task
set "LOWES_TASK_BAT="
if not "%LOWES_REF_LDY_DAILY_TASK_BAT%"=="" call :try_lowes_candidate "%LOWES_REF_LDY_DAILY_TASK_BAT%"
call :try_lowes_candidate "%BBY_ROOT%\..\..\lowes\lowes_ref_ldy_daily_task.bat"
call :try_lowes_candidate "%BBY_ROOT%\..\..\..\..\lowes\lowes_ref_ldy_daily_task.bat"
call :try_lowes_candidate "%BBY_ROOT%\..\lowes\lowes_ref_ldy_daily_task.bat"
call :try_lowes_candidate "%BBY_ROOT%\lowes\lowes_ref_ldy_daily_task.bat"
if defined LOWES_TASK_BAT (
  echo Lowes task resolved: !LOWES_TASK_BAT!
  >>"%CHAIN_LOG%" echo Lowes task resolved: !LOWES_TASK_BAT!
)
exit /b 0

:try_lowes_candidate
if defined LOWES_TASK_BAT exit /b 0
set "LOWES_CANDIDATE=%~1"
if "%LOWES_CANDIDATE%"=="" exit /b 0
for %%F in ("%LOWES_CANDIDATE%") do (
  set "LOWES_CANDIDATE_FULL=%%~fF"
  set "LOWES_CANDIDATE_DIR=%%~dpF"
)
if exist "!LOWES_CANDIDATE_FULL!" if exist "!LOWES_CANDIDATE_DIR!_lowes_daily_task.bat" (
  set "LOWES_TASK_BAT=!LOWES_CANDIDATE_FULL!"
)
exit /b 0

:record_failure
set "FAIL_GROUP=%~1"
set "FAIL_CODE=%~2"
if "%FAIL_CODE%"=="" set "FAIL_CODE=0"
if not "%FAIL_CODE%"=="0" (
  set /a FAILURE_COUNT+=1
  set "OVERALL_EXIT_CODE=%FAIL_CODE%"
  if "!FAILURES!"=="" (
    set "FAILURES=%FAIL_GROUP%(%FAIL_CODE%)"
  ) else (
    set "FAILURES=!FAILURES!, %FAIL_GROUP%(%FAIL_CODE%)"
  )
  echo [warn] %FAIL_GROUP% failed exit_code=%FAIL_CODE%; continuing.
  >>"%CHAIN_LOG%" echo [warn] %FAIL_GROUP% failed exit_code=%FAIL_CODE%; continuing.
)
exit /b 0

:clear_bestbuy_env
set "BESTBUY_PRESERVE_RUN_ENV="
set "BESTBUY_FORCE_STEP_ENV="
set "BESTBUY_FORCE_RUN_PATH_ENV="
set "BESTBUY_FINAL_TARGET_SIZE="
set "BESTBUY_MAIN_RANK_LIMIT="
set "BESTBUY_BSR_RANK_LIMIT="
set "BESTBUY_FINAL_ROW_LIMIT="
set "BESTBUY_DETAIL_LIMIT="
set "BESTBUY_DETAIL_SKUS="
set "BESTBUY_AVAILABILITY_BACKFILL_LIMIT="
set "BESTBUY_DB_LOAD_DRY_RUN="
set "BESTBUY_DB_UPDATE_SIMILAR_ONLY="
set "BESTBUY_DB_UPDATE_AVAILABILITY_ONLY="
set "BESTBUY_RUN_ROOT="
set "BESTBUY_OUTPUT_ROOT="
set "BESTBUY_DETAIL_RUN_ROOT="
set "BESTBUY_DETAIL_TARGET_CSV="
set "BESTBUY_FINAL_OUTPUT_CSV="
set "BESTBUY_PRODUCT_LIST_OUTPUT="
set "BESTBUY_ITEM_MST_OUTPUT_CSV="
exit /b 0

:clear_lowes_env
set "LOWES_PRODUCT_TYPE="
set "LOWES_RUN_ROOT="
set "LOWES_OUTPUT_ROOT="
set "LOWES_MAIN_RUN_ID="
set "LOWES_BSR_RUN_ID="
set "LOWES_MAIN_TARGET_RUN_ID="
set "LOWES_PAGE_LIST="
set "LOWES_START_PAGE="
set "LOWES_END_PAGE="
set "LOWES_PAGES="
set "LOWES_DETAIL_LIMIT="
set "LOWES_FINAL_TARGET_SIZE="
set "LOWES_MAIN_RANK_LIMIT="
set "LOWES_BSR_RANK_LIMIT="
set "LOWES_DB_LOAD_DRY_RUN="
set "LOWES_SKIP_S3="
exit /b 0
