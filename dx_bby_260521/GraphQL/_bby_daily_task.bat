@echo off
setlocal EnableExtensions

cd /d "%~dp0"

set "CATEGORY=%~1"
if "%CATEGORY%"=="" set "CATEGORY=TV"
set "CATEGORY=%CATEGORY:"=%"

set "LOCK_FILE=%~dp0bestbuy_daily_%CATEGORY%.lock"
set "TASK_LOG_DIR=%~dp0bestbuy\data\%CATEGORY%\task_logs"
if not exist "%TASK_LOG_DIR%" mkdir "%TASK_LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TASK_TS=%%i"
set "TASK_LOG=%TASK_LOG_DIR%\daily_task_%TASK_TS%.log"
for /f %%i in ('powershell -NoProfile -Command "[int][double]::Parse((Get-Date -UFormat %%s))"') do set "TASK_START_EPOCH=%%i"

python -m bestbuy.step00_daily_lock acquire --lock "%LOCK_FILE%" --category "%CATEGORY%" --log "%TASK_LOG%" --root "%~dp0"
set "LOCK_STATUS=%ERRORLEVEL%"
if "%LOCK_STATUS%"=="2" exit /b 2
if not "%LOCK_STATUS%"=="0" exit /b %LOCK_STATUS%

echo ================================================== >> "%TASK_LOG%"
echo BestBuy %CATEGORY% daily task started >> "%TASK_LOG%"
echo cwd=%CD% >> "%TASK_LOG%"
echo task_log=%TASK_LOG% >> "%TASK_LOG%"
echo ================================================== >> "%TASK_LOG%"

echo ==================================================
echo BestBuy %CATEGORY% daily task started
echo cwd=%CD%
echo task_log=%TASK_LOG%
echo ==================================================

powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%~dp0run_bestbuy_fullrun.bat' '%CATEGORY%' 2>&1 | Tee-Object -FilePath '%TASK_LOG%' -Append; exit $LASTEXITCODE"
set "EXIT_CODE=%ERRORLEVEL%"

python -m bestbuy.step00_daily_lock release --lock "%LOCK_FILE%" --category "%CATEGORY%" --log "%TASK_LOG%" --root "%~dp0" >nul 2>nul
if exist "%LOCK_FILE%" del "%LOCK_FILE%"

for /f %%i in ('powershell -NoProfile -Command "[int][double]::Parse((Get-Date -UFormat %%s))"') do set "TASK_END_EPOCH=%%i"
set /a TASK_ELAPSED_SEC=TASK_END_EPOCH-TASK_START_EPOCH

echo ================================================== >> "%TASK_LOG%"
echo BestBuy %CATEGORY% daily task finished exit_code=%EXIT_CODE% >> "%TASK_LOG%"
echo elapsed_sec=%TASK_ELAPSED_SEC% >> "%TASK_LOG%"
echo ================================================== >> "%TASK_LOG%"

echo ==================================================
echo BestBuy %CATEGORY% daily task finished exit_code=%EXIT_CODE%
echo elapsed_sec=%TASK_ELAPSED_SEC%
echo task_log=%TASK_LOG%
echo ==================================================

exit /b %EXIT_CODE%
