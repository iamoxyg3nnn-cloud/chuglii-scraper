@echo off
setlocal
title Chuglii - Buu Mal (Myanmar) Scraper Runner
color 0b

echo ============================================================================
echo   CHUGLII - BUU MAL (MYANMAR) SCRAPER PIPELINE RUNNER
echo   Target: https://www.buumal.com
echo   Storage: scraper.myanmar_videos (Segregated Isolated Schema)
echo ============================================================================
echo.

cd /d "%~dp0"

set PYTHON_BIN="%~dp0..\..\.venv\Scripts\python.exe"

if not exist %PYTHON_BIN% (
    set PYTHON_BIN=python
)

echo [*] Using Python executable: %PYTHON_BIN%
echo.

echo Options:
echo  1. Scrape Myanmar category (5 pages)
echo  2. Scrape Myanmar category (25 pages)
echo  3. Scrape All Recent videos (10 pages)
echo  4. Run 24/7 continuous watcher daemon
echo.
set /p opt="Select an option [1-4] (default 1): "

if "%opt%"=="2" (
    %PYTHON_BIN% pipeline.py --tag myanmar --pages 25 --workers 6
) else if "%opt%"=="3" (
    %PYTHON_BIN% pipeline.py --tag all --pages 10 --workers 6
) else if "%opt%"=="4" (
    %PYTHON_BIN% pipeline.py --tag myanmar --daemon --workers 6
) else (
    %PYTHON_BIN% pipeline.py --tag myanmar --pages 5 --workers 6
)

echo.
echo [✓] Scraper run ended.
pause
