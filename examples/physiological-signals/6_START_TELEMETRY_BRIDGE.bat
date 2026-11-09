@echo off
title XR-telemetry bridge - Concert Engagement AR
REM ===================================================================
REM  OPTIONAL. Copies what reaches the Redis hub to the AMPLIFY cloud
REM  archive (XR-telemetry). It only reads Redis: if this window is
REM  closed or the Internet drops, the venue setup works exactly as
REM  before. Start it after 1_START_REDIS.bat.
REM
REM  Once, on this PC (needs Git and Node.js 22 or newer):
REM    git clone https://github.com/amplify-project/XR-telemetry.git
REM  Then set the values below and copy this file next to the other
REM  START_*.bat files.
REM ===================================================================

REM ---- set these -----------------------------------------------------
REM Where XR-telemetry was cloned.
set "XRTEL_DIR=%USERPROFILE%\XR-telemetry"
REM Ingest address and token, from the XR-telemetry deployment.
set "TELEMETRY_URL=wss://CHANGE-ME.cloudfront.net/telemetry/ingest"
set "TELEMETRY_TOKEN=CHANGE-ME"
REM The hub. 127.0.0.1 when this PC runs 1_START_REDIS.bat, else the hub's IP.
set "REDIS_URL=redis://127.0.0.1:6379"
REM What goes to the cloud. engagement includes the crowd engagement_score.
set "SIGNALS=physio_metrics,valence_cont,arousal_cont,engagement,raw_sensors"
REM --------------------------------------------------------------------

echo "%TELEMETRY_URL% %TELEMETRY_TOKEN%" | findstr /C:"CHANGE-ME" >nul && (
    echo ERROR: set TELEMETRY_URL and TELEMETRY_TOKEN at the top of this file.
    pause
    exit /b 1
)

where node >nul 2>nul || (
    echo ERROR: Node.js not found. Install Node.js 22 LTS or newer from https://nodejs.org
    pause
    exit /b 1
)
for /f "tokens=1 delims=." %%v in ('node -v') do set "NODE_MAJOR=%%v"
set "NODE_MAJOR=%NODE_MAJOR:v=%"
if %NODE_MAJOR% LSS 22 (
    echo ERROR: Node.js %NODE_MAJOR% found, 22 or newer needed. Update it from https://nodejs.org
    pause
    exit /b 1
)

if not exist "%XRTEL_DIR%\tools\bridge.mjs" (
    echo ERROR: XR-telemetry not found in %XRTEL_DIR%
    echo   git clone https://github.com/amplify-project/XR-telemetry.git "%XRTEL_DIR%"
    echo or set XRTEL_DIR at the top of this file.
    pause
    exit /b 1
)

if not exist "%XRTEL_DIR%\gateway\node_modules\ioredis" (
    echo Installing the bridge dependencies, once...
    pushd "%XRTEL_DIR%\gateway"
    call npm install --omit=dev
    popd
)

echo.
echo Forwarding %REDIS_URL% to the cloud: %SIGNALS%
echo Each wearable shows up as "serial -^> wss://...". Close this window to stop.
echo.

:run
node "%XRTEL_DIR%\tools\bridge.mjs" --redis "%REDIS_URL%" --url "%TELEMETRY_URL%" --token "%TELEMETRY_TOKEN%" --signals %SIGNALS%
echo.
echo Bridge stopped (exit code %errorlevel%). Restarting in 5 s - close this window to stop for good.
timeout /t 5 /nobreak >nul
goto run
