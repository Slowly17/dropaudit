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

:: ===== CO BAN MOI - TAI TUNG FILE QUA RAW (KHONG BI CACHE) =====
echo.
echo [*] Co ban moi ^(!REMOTE_VER!^)! Dang tai ve...
set "NC=%RANDOM%%RANDOM%"
set "RAW=%REPO_RAW%"

:: Tai cac file chinh qua raw.githubusercontent.com (khong bi CDN cache)
curl -s -f -o "main.py.new"              "%RAW%/main.py?nc=!NC!"              2>nul
curl -s -f -o "version.json.new"         "%RAW%/version.json?nc=!NC!"         2>nul
curl -s -f -o "requirements.txt.new"     "%RAW%/requirements.txt?nc=!NC!"     2>nul

:: Kiem tra tai thanh cong chua
if not exist "main.py.new" (
    echo [!] Tai main.py that bai, dung ban hien tai.
    goto :run_app
)

:: Ap dung file moi
move /y "main.py.new"          "main.py"          >nul 2>&1
move /y "version.json.new"     "version.json"      >nul 2>&1
if exist "requirements.txt.new" move /y "requirements.txt.new" "requirements.txt" >nul 2>&1

:: Tai static/index.html neu co
if not exist "static" mkdir static
curl -s -f -o "static\index.html.new" "%RAW%/static/index.html?nc=!NC!" 2>nul
if exist "static\index.html.new" move /y "static\index.html.new" "static\index.html" >nul 2>&1

echo [OK] Da cap nhat len ban !REMOTE_VER!

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
