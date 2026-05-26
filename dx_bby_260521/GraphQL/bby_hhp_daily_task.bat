@echo off
setlocal EnableExtensions
cd /d "%~dp0"
call "%~dp0_bby_daily_task.bat" HHP
exit /b %ERRORLEVEL%
