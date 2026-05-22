@echo off
setlocal EnableExtensions

cd /d "%~dp0"

set "CATEGORY=TV"

set "LOCK_FILE=%~dp0bestbuy_daily_%CATEGORY%.lock"
set "TASK_LOG_DIR=%~dp0bestbuy\data\%CATEGORY%\task_logs"
if not exist "%TASK_LOG_DIR%" mkdir "%TASK_LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TASK_TS=%%i"
set "TASK_LOG=%TASK_LOG_DIR%\daily_task_%TASK_TS%.log"

if exist "%LOCK_FILE%" (
  echo [%DATE% %TIME%] Previous BestBuy %CATEGORY% task is still locked. >> "%TASK_LOG%"
  echo lock_file=%LOCK_FILE% >> "%TASK_LOG%"
  exit /b 2
)

echo %DATE% %TIME% > "%LOCK_FILE%"

echo ================================================== > "%TASK_LOG%"
echo BestBuy %CATEGORY% daily task started >> "%TASK_LOG%"
echo cwd=%CD% >> "%TASK_LOG%"
echo ================================================== >> "%TASK_LOG%"

call "%~dp0run_bestbuy_fullrun.bat" "%CATEGORY%" >> "%TASK_LOG%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

if exist "%LOCK_FILE%" del "%LOCK_FILE%"

echo ================================================== >> "%TASK_LOG%"
echo BestBuy %CATEGORY% daily task finished exit_code=%EXIT_CODE% >> "%TASK_LOG%"
echo ================================================== >> "%TASK_LOG%"

exit /b %EXIT_CODE%
