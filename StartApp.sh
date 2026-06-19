#!/bin/bash
echo "============================================"
echo "        STARTING DROPAUDIT BOT"
echo "============================================"
echo ""

# Kiem tra Python
if ! command -v python3 &>/dev/null; then
    echo "[LOI] Chua cai Python3."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Chay: brew install python3"
    else
        echo "Chay: sudo apt install python3 python3-pip"
    fi
    read -p "Nhan Enter de thoat..."
    exit 1
fi

# Cai dependencies
echo "[*] Kiem tra va cai dependencies..."
python3 -m pip install -r requirements.txt -q
if [ $? -ne 0 ]; then
    echo "[LOI] Cai dependencies that bai!"
    read -p "Nhan Enter de thoat..."
    exit 1
fi

# Cai Playwright browsers
echo "[*] Kiem tra Playwright browsers..."
python3 -m playwright install chromium 2>/dev/null

# Kill port 8099 neu dang bi chiem
echo "[*] Kiem tra port 8099..."
PID=$(lsof -ti:8099 2>/dev/null)
if [ -n "$PID" ]; then
    echo "[*] Kill process $PID dang chiem port 8099..."
    kill -9 $PID 2>/dev/null
    sleep 1
fi

# KHONG mo browser o day - main.py se tu mo 1 lan (tranh mo 2 tab)

# Chay server
echo "[*] Khoi dong server tai http://localhost:8099"
echo "[*] Nhan Ctrl+C de dung"
echo ""
python3 main.py
