@echo off
setlocal
title Chuglii - Multi-Site OnlyFans Scraper Runner
color 0d

echo ============================================================================
echo   CHUGLII - MULTI-SITE ONLYFANS SCRAPER & DEDUPLICATION RUNNER
echo   Targets:
echo     1. InternetChicks      (https://internetchicks.com)
echo     2. InfluencersGoneWild  (https://influencersgonewild.com)
echo     3. SheeshFans          (https://sheeshfans.com)
echo   Engine: Scrapling Stealth + Per-Site Isolation Pipeline
echo   Storage: scraper.onlyfans_videos / scraper.videos
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
echo  1. Scrape ALL 3 sites (1 page each)
echo  2. Scrape ALL 3 sites (5 pages each)
echo  3. Scrape InternetChicks only (5 pages)
echo  4. Scrape InfluencersGoneWild only (5 pages)
echo  5. Scrape SheeshFans only (5 pages)
echo  6. Dry-run test across all 3 sites with JSON export (1 page each)
echo  7. Run 24/7 continuous watcher daemon across all 3 sites
echo.
set /p opt="Select an option [1-7] (default 1): "

if "%opt%"=="2" (
    %PYTHON_BIN% pipeline.py --site all --pages 5
) else if "%opt%"=="3" (
    %PYTHON_BIN% pipeline.py --site internetchicks --pages 5
) else if "%opt%"=="4" (
    %PYTHON_BIN% pipeline.py --site influencersgonewild --pages 5
) else if "%opt%"=="5" (
    %PYTHON_BIN% pipeline.py --site sheeshfans --pages 5
) else if "%opt%"=="6" (
    %PYTHON_BIN% pipeline.py --site all --pages 1 --dry-run --save-json
) else if "%opt%"=="7" (
    %PYTHON_BIN% pipeline.py --daemon
) else (
    %PYTHON_BIN% pipeline.py --site all --pages 1
)

echo.
echo [✓] Scraper run ended.
pause
