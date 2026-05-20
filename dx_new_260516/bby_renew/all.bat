@echo off
setlocal

cd /d "%~dp0"

git pull
if not exist logs mkdir logs

set BESTBUY_SKIP_PULL=1

call tv.bat
if errorlevel 1 exit /b %ERRORLEVEL%

call hhp.bat
if errorlevel 1 exit /b %ERRORLEVEL%

call ref.bat
if errorlevel 1 exit /b %ERRORLEVEL%

call ldy.bat
if errorlevel 1 exit /b %ERRORLEVEL%

exit /b 0
