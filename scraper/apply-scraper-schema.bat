@echo off
setlocal
title Chuglii - Scraper Schema Migration Runner
color 0b

echo ============================================================================
echo   CHUGLII ISOLATED SCRAPER SCHEMA APPLIER
echo ============================================================================
echo.

cd /d "%~dp0"

if not exist "%~dp0..\.env" (
    color 0c
    echo [ERROR] .env file not found in parent directory.
    echo Please make sure .env exists with SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.
    echo.
    pause
    exit /b 1
)

if not exist "%~dp0schema.sql" (
    color 0c
    echo [ERROR] schema.sql not found in %~dp0
    echo.
    pause
    exit /b 1
)

echo [*] Reading credentials from .env and applying scraper.schema...
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command "$envFile = Join-Path '%~dp0..' '.env'; $sqlFile = Join-Path '%~dp0' 'schema.sql'; $envMap = @{}; Get-Content -Path $envFile | ForEach-Object { if ($_ -match '^[A-Z0-9_]+=') { $parts = $_ -split '=', 2; $val = $parts[1].Trim(); if ($val.StartsWith('''') -and $val.EndsWith('''')) { $val = $val.Substring(1, $val.Length - 2) }; if ($val.StartsWith([char]34) -and $val.EndsWith([char]34)) { $val = $val.Substring(1, $val.Length - 2) }; $envMap[$parts[0]] = $val } }; $url = $envMap['SUPABASE_URL']; $key = if ($envMap['SUPABASE_SERVICE_ROLE_KEY']) { $envMap['SUPABASE_SERVICE_ROLE_KEY'] } else { $envMap['SUPABASE_ANON_KEY'] }; if (-not $url -or -not $key) { Write-Host '[ERROR] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env' -ForegroundColor Red; exit 1 }; Write-Host ('[+] Supabase Endpoint : ' + $url) -ForegroundColor Green; $sqlContent = [System.IO.File]::ReadAllText($sqlFile, [System.Text.Encoding]::UTF8); Write-Host ('[+] Scraper Schema    : ' + $sqlContent.Length + ' bytes') -ForegroundColor Green; Write-Host '[*] Applying isolated scraper schema via /functions/v1/migrate...' -ForegroundColor Yellow; $bodyObj = @{ name = 'scraper_schema.sql'; sql = $sqlContent; force = $true }; $jsonPayload = $bodyObj | ConvertTo-Json -Depth 5 -Compress; try { [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12; $bytes = [System.Text.Encoding]::UTF8.GetBytes($jsonPayload); $webClient = New-Object System.Net.WebClient; $webClient.Headers.Add('apikey', $key); $webClient.Headers.Add('authorization', ('Bearer ' + $key)); $webClient.Headers.Add('content-type', 'application/json; charset=utf-8'); $respBytes = $webClient.UploadData(($url.TrimEnd('/') + '/functions/v1/migrate'), 'POST', $bytes); $respStr = [System.Text.Encoding]::UTF8.GetString($respBytes); $response = $respStr | ConvertFrom-Json; if ($response.ok -eq $true -or $response.result.status -eq 'applied') { Write-Host ''; Write-Host '============================================================================' -ForegroundColor Green; Write-Host ' SUCCESS: Scraper schema created successfully in isolated namespace.' -ForegroundColor Green; Write-Host ' Existing backend and frontend tables in public remain untouched.' -ForegroundColor Green; Write-Host '============================================================================' -ForegroundColor Green; exit 0 } else { Write-Host ''; Write-Host '[FAILED] Migration reported failure:' -ForegroundColor Red; Write-Host $respStr -ForegroundColor Red; exit 1 } } catch { Write-Host ''; Write-Host ('[ERROR] Request failed: ' + $_.Exception.Message) -ForegroundColor Red; exit 1 }"

if %ERRORLEVEL% equ 0 (
    echo.
    echo [OK] Migration completed successfully.
) else (
    color 0c
    echo.
    echo [ERROR] Migration failed with exit code %ERRORLEVEL%.
)

echo.
pause
