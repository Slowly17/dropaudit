@echo off
setlocal enabledelayedexpansion
title DropAudit Bot
color 0A
echo ============================================
echo         DROPAUDIT BOT - AUTO UPDATE
echo ============================================
echo.

cd /d "%~dp0"

:: ===== CAU HINH AUTO-UPDATE =====
set "REPO_RAW=https://raw.githubusercontent.com/Slowly17/dropaudit/main"
set "REPO_ZIP=https://github.com/Slowly17/dropaudit/archive/refs/heads/main.zip"

:: ===== KIEM TRA PYTHON =====
python --version >nul 2>&1
if errorlevel 1 (
    echo [LOI] Chua cai Python. Tai tai: https://www.python.org/downloads/
    echo      Nho tick "Add Python to PATH" khi cai!
    pause
    exit /b 1
)

:: ===== KIEM TRA CAP NHAT =====
echo [*] Kiem tra ban cap nhat moi...
set "REMOTE_VER="
set "LOCAL_VER="

:: Lay version local
if exist "version.json" (
    for /f "tokens=2 delims=:," %%a in ('findstr /i "version" version.json') do (
        set "LOCAL_VER=%%~a"
    )
)
set "LOCAL_VER=!LOCAL_VER: =!"
set "LOCAL_VER=!LOCAL_VER:"=!"

:: Lay version remote (tai version.json tu GitHub)
curl -s -o "%TEMP%\dropaudit_ver.json" "%REPO_RAW%/version.json?nocache=%RANDOM%%RANDOM%" 2>nul
if exist "%TEMP%\dropaudit_ver.json" (
    for /f "tokens=2 delims=:," %%a in ('findstr /i "version" "%TEMP%\dropaudit_ver.json"') do (
        set "REMOTE_VER=%%~a"
    )
)
set "REMOTE_VER=!REMOTE_VER: =!"
set "REMOTE_VER=!REMOTE_VER:"=!"

echo     Ban hien tai: !LOCAL_VER!
echo     Ban moi nhat: !REMOTE_VER!

if "!REMOTE_VER!"=="" (
    echo [!] Khong ket noi duoc server cap nhat, dung ban hien tai.
    goto :run_app
)

if "!LOCAL_VER!"=="!REMOTE_VER!" (
    echo [OK] Da la ban moi nhat.
    goto :run_app
)

:: ===== CO BAN MOI - TAI VE =====
echo.
echo [*] Co ban moi! Dang tai ve...
curl -L -s -o "%TEMP%\dropaudit_new.zip" "%REPO_ZIP%?nocache=%RANDOM%%RANDOM%"
if not exist "%TEMP%\dropaudit_new.zip" (
    echo [!] Tai that bai, dung ban hien tai.
    goto :run_app
)

echo [*] Dang giai nen va cap nhat code...
:: Giai nen vao thu muc tam
if exist "%TEMP%\dropaudit_extract" rmdir /s /q "%TEMP%\dropaudit_extract"
powershell -NoProfile -Command "Expand-Archive -Path '%TEMP%\dropaudit_new.zip' -DestinationPath '%TEMP%\dropaudit_extract' -Force" 2>nul

:: Copy de len (GIU LAI du lieu nguoi dung)
if exist "%TEMP%\dropaudit_extract\dropaudit-main" (
    :: Sao luu du lieu nguoi dung
    if exist "data.json" copy /y "data.json" "%TEMP%\_bak_data.json" >nul 2>&1
    if exist "proxies.json" copy /y "proxies.json" "%TEMP%\_bak_proxies.json" >nul 2>&1
    if exist "declined_results.json" copy /y "declined_results.json" "%TEMP%\_bak_declined.json" >nul 2>&1
    if exist "queue.json" copy /y "queue.json" "%TEMP%\_bak_queue.json" >nul 2>&1

    :: Copy code moi (main.py, static, requirements, version.json...)
    xcopy /y /e /q "%TEMP%\dropaudit_extract\dropaudit-main\*" "." >nul 2>&1

    :: Khoi phuc du lieu nguoi dung
    if exist "%TEMP%\_bak_data.json" copy /y "%TEMP%\_bak_data.json" "data.json" >nul 2>&1
    if exist "%TEMP%\_bak_proxies.json" copy /y "%TEMP%\_bak_proxies.json" "proxies.json" >nul 2>&1
    if exist "%TEMP%\_bak_declined.json" copy /y "%TEMP%\_bak_declined.json" "declined_results.json" >nul 2>&1
    if exist "%TEMP%\_bak_queue.json" copy /y "%TEMP%\_bak_queue.json" "queue.json" >nul 2>&1

    echo [OK] Da cap nhat len ban !REMOTE_VER!
) else (
    echo [!] Giai nen loi, dung ban hien tai.
)

:run_app
echo.
:: ===== CAI DEPENDENCIES =====
echo [*] Kiem tra dependencies...
pip install -r requirements.txt -q 2>nul

:: ===== CAI PLAYWRIGHT BROWSER =====
echo [*] Kiem tra trinh duyet Playwright...
python -m playwright install chromium >nul 2>&1

:: ===== KILL PORT 8099 =====
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr :8099 ^| findstr LISTENING') do (
    taskkill /PID %%a /F >nul 2>&1
)

:: ===== CHAY APP =====
echo.
echo [*] Khoi dong DropAudit tai http://localhost:8099
echo [*] Nhan Ctrl+C de dung
echo.
python main.py

pause
