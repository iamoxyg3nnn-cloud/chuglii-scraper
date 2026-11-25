@echo off
title Chuglii Scraper - Chinese Category Pipeline (uxj838238)
cd /d "%~dp0..\..\.."

:menu
cls
echo =================================================================
echo       CHUGLII SCRAPER - CHINESE CATEGORY PIPELINE
echo       Source: uxj.838238.xyz (95da network) - Isolated DB
echo       37 zones: Chinese videos + magnets, Western magnets
echo       (JAV and livestream zones are excluded by design)
echo =================================================================
echo.
echo   [1] QUICK SYNC: Ingest new releases (page 1 across all zones)
echo   [2] 24/7 DAEMON: Continuous watcher mode
echo   [3] DEEP SYNC: Ingest 5 pages per zone
echo   [4] FULL ARCHIVE BACKFILL: Page 1 to end with checkpoints
echo   [5] NEW ONLY: Stop each zone early when everything is known
echo   [6] SPECIFIC ZONES: Enter zone id(s), e.g. 20100496,19920497
echo   [7] DRY RUN: Scrape without writing to the database
echo   [8] STATUS: View checkpoints, known IDs and progress
echo   [9] CUSTOM CLI: Run custom parameters
echo   [10] EXIT
echo.
echo =================================================================
set /p opt="Select an option (1-10): "

if "%opt%"=="1" (
    echo.
    echo [*] Running quick sync across all 37 zones...
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --pages 1 --new-only
    echo.
    pause
    goto menu
) else if "%opt%"=="2" (
    echo.
    echo [*] Starting 24/7 daemon mode. Press Ctrl+C to stop.
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --daemon
    echo.
    pause
    goto menu
) else if "%opt%"=="3" (
    echo.
    echo [*] Running deep sync (5 pages per zone)...
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --pages 5 --new-only
    echo.
    pause
    goto menu
) else if "%opt%"=="4" (
    echo.
    echo [*] Running FULL ARCHIVE BACKFILL with checkpoints.
    echo [*] This can take a long time. Progress is saved after every page.
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --full-site --new-only
    echo.
    pause
    goto menu
) else if "%opt%"=="5" (
    echo.
    echo [*] Running new-only sync (2 pages per zone, early stop)...
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --pages 2 --new-only
    echo.
    pause
    goto menu
) else if "%opt%"=="6" (
    echo.
    echo   Zone id format: video zones end in 496, magnet zones end in 497.
    echo   Examples: 20100496 (Housewives), 19920497 (Chinese Magnet)
    set /p zone_ids="Enter zone id(s), comma-separated: "
    set /p zone_pages="How many pages per zone? (e.g. 2): "
    echo.
    echo [*] Ingesting %zone_pages% page(s) of zones: %zone_ids%
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --categories %zone_ids% --pages %zone_pages% --new-only
    echo.
    pause
    goto menu
) else if "%opt%"=="7" (
    echo.
    echo [*] Dry run: scrape 1 page of 3 zones, no database writes...
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py --categories 20100496,20060496,19920497 --pages 1 --dry-run
    echo.
    pause
    goto menu
) else if "%opt%"=="8" (
    echo.
    scraper\.venv\Scripts\python.exe -X utf8 -c "import json,io; p=json.load(io.open('scraper/categories/chinese/progress.json',encoding='utf-8')); print('status:',p.get('status')); print('totals:',p.get('totals')); c=json.load(io.open('scraper/categories/chinese/checkpoint.json',encoding='utf-8')); print('zones with checkpoints:',len(c)); [print(' ',k,'-> page',v.get('current_page'),'/',v.get('max_pages')) for k,v in sorted(c.items())]"
    echo.
    pause
    goto menu
) else if "%opt%"=="9" (
    echo.
    set /p custom_args="Enter CLI arguments (e.g. --categories 20100496 --pages 3): "
    scraper\.venv\Scripts\python.exe -X utf8 scraper/categories/chinese/pipeline.py %custom_args%
    echo.
    pause
    goto menu
) else if "%opt%"=="10" (
    exit /b 0
) else (
    echo Invalid selection.
    pause
    goto menu
)
