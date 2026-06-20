# DropAudit — Handover cho AI mới

## Git / Repo

| | |
|---|---|
| Repo | `https://github.com/Slowly17/dropaudit` |
| Branch | `main` |
| Clone (có token) | `git clone https://Slowly17:<YOUR_GITHUB_TOKEN>@github.com/Slowly17/dropaudit.git` |
| GitHub Token | xem file `/home/user/.gh_env` trong sandbox (không lưu vào git) |
| Version hiện tại | `1.2.0` |

---

## Môi trường local (sandbox Runable)

```
App path:       /home/user/dropaudit-app/
Token file:     /home/user/.gh_env   (GH_TOKEN="ghp_...")
Debug venv:     /home/user/dbgenv
```

Git remote đã nhúng token sẵn — push trực tiếp không cần `source .gh_env`:
```bash
cd /home/user/dropaudit-app
git add -A && git commit -m "message" && git push origin main
```
Nếu cần source token thủ công:
```bash
source /home/user/.gh_env
```

---

## Cấu trúc project

```
dropaudit-app/
├── main.py              ← Toàn bộ backend FastAPI (file duy nhất, ~5800 dòng)
├── static/
│   └── index.html       ← Toàn bộ frontend (single file HTML + JS + CSS)
├── StartApp.bat         ← Entry point Windows: auto-update + chạy app
├── StartApp.sh          ← Entry point Linux/Mac
├── requirements.txt     ← fastapi, uvicorn, invisible_playwright, PySocks
├── version.json         ← Version tracking cho auto-update
├── data.json            ← Profiles (được giữ lại qua update)
├── proxies.json         ← Proxy pool (được giữ lại qua update)
├── queue.json           ← Automation queue (được giữ lại qua update)
├── declined_results.json← Kết quả declined (được giữ lại qua update)
└── profiles/            ← Browser profile dirs (gitignore)
```

---

## Tech stack

| Layer | Chi tiết |
|---|---|
| Backend | Python FastAPI + Uvicorn, port `8099` |
| Frontend | Single HTML file (`static/index.html`), dark theme |
| Browser automation | `invisible_playwright` (Firefox, headless=True, humanize=0.4) |
| Profile storage | `data.json` (JSON flat file, không dùng DB) |
| Proxy | SOCKS5, lưu trong `proxies.json` |

---

## Cách auto-update hoạt động

`StartApp.bat` khi mở:
1. Đọc `version.json` local
2. Tải `version.json` từ GitHub raw
3. Nếu version khác → tải `main.zip` → giải nén → ghi đè code mới
4. **Backup trước khi ghi đè**: `data.json`, `proxies.json`, `declined_results.json`, `queue.json`
5. **Restore sau khi ghi đè**: 4 file trên được copy lại
6. Chạy `python main.py`

**Rule bắt buộc mỗi lần fix**: edit xong → tăng version trong `version.json` → `git push origin main` → user mở lại `StartApp.bat` là tự update.

---

## Quy trình fix code

```bash
# 1. Edit file
nano /home/user/dropaudit-app/main.py
# hoặc
nano /home/user/dropaudit-app/static/index.html

# 2. Tăng version (ví dụ 1.2.0 → 1.2.1)
# Edit version.json:
# { "version": "1.2.1", "updated": "YYYY-MM-DD", "notes": "mô tả" }

# 3. Syntax check (bắt buộc trước khi push)
python3 -c "import ast; ast.parse(open('main.py').read()); print('OK')"

# 4. Push
cd /home/user/dropaudit-app
git add -A && git commit -m "v1.2.1: mô tả fix" && git push origin main
```

---

## Cấu trúc main.py (các section chính)

| Dòng ~ | Nội dung |
|---|---|
| 1–50 | Imports, FastAPI init, storage helpers |
| 50–220 | `load_data()`, `save_data()`, profile helpers |
| 223–540 | Profile APIs (`/api/profiles/*`) |
| 543–1717 | `_run_dropaudit_signup()` — script automation chính |
| 1718–2345 | `_run_simen_trial()` — script thứ 2 |
| 2346 | `SCRIPT_RUNNERS` dict |
| 2350–2470 | Queue system (`_data_queue`, `queue_save`, `queue_load`, queue APIs) |
| 2482–2545 | Bulk profile APIs |
| 2547–2614 | `/api/tasks/run-multi` — chạy nhiều profile |
| 2615–2783 | Proxy APIs (`/api/proxies/*`) |
| 2784–2970 | Task run/stop/status APIs |
| 2972–2990 | `/api/version`, `/api/dashboard` |
| 2993 | `if __name__ == "__main__"` — entry point |

> **Lưu ý**: file có 2 block trùng (do refactor cũ), block thứ 2 bắt đầu ~dòng 3000+. Block đầu (dòng thấp hơn) là bản mới nhất, đã được sửa đúng.

---

## Data files

### `data.json`
```json
{
  "profiles": [
    {
      "id": "uuid",
      "name": "Profile Name",
      "proxy_server": "socks5://host:port",  // null nếu không có proxy
      "created_at": "ISO datetime"
    }
  ]
}
```

### `proxies.json`
```json
{
  "proxies": [
    {
      "id": "uuid",
      "host": "38.154.185.97",
      "port": 6370,
      "protocol": "socks5",
      "username": "",
      "password": "",
      "used_count": 0,
      "last_used": null
    }
  ]
}
```

### `queue.json`
```json
[
  {
    "_idx": 0,
    "_status": "pending",  // pending | running | done | failed | declined | consumed
    "email": "...",
    "password": "...",
    // ... các field khác tùy script
  }
]
```

---

## invisible_playwright — cách dùng

```python
from invisible_playwright import AsyncInvisiblePlaywright

async with AsyncInvisiblePlaywright() as p:
    browser = await p.firefox.launch(
        profile_dir="profiles/uuid/",
        seed=12345,           # stable fingerprint
        humanize=0.4,
        headless=True
    )
    context = browser.contexts[0]
    page = await context.new_page()
```

- `profile_dir` + `seed` = stable identity (fingerprint không đổi)
- Engine: **Firefox** (không dùng Chromium)
- `headless=True` mặc định

---

## Stripe checkout — cách fill thẻ (QUAN TRỌNG)

Checkout URL dạng `checkout.stripe.com` — ô card là **INPUT TRỰC TIẾP** trên trang chính.

```python
# Selector đúng (1 trong 3):
loc = page.locator("input[name=cardnumber]")
# hoặc: input[autocomplete="cc-number"]
# hoặc: input[placeholder="1234"]

# Cách fill ĐÚNG:
await loc.click()
await loc.press("Control+a")
await loc.press("Backspace")
await loc.press_sequentially("4111111111111111", delay=80)

# Verify:
val = await loc.input_value()
assert val.replace(" ", "") == "4111111111111111"
```

**TUYỆT ĐỐI KHÔNG dùng**:
- `loc.triple_click()` → không tồn tại, crash silent
- `loc.scroll_into_view_if_needed()` trên ô card → gây treo

---

## UI style (index.html)

| | |
|---|---|
| Background | `#0e0e0e` |
| Accent 1 | `#4dffa0` (xanh lá) |
| Accent 2 | `#00c9ff` (xanh dương) |
| Font chính | Poppins |
| Font code/mono | JetBrains Mono |
| Theme | Dark, tối giản |

---

## Các file được bảo vệ qua update

| File | Backup key |
|---|---|
| `data.json` | `%TEMP%\_bak_data.json` |
| `proxies.json` | `%TEMP%\_bak_proxies.json` |
| `declined_results.json` | `%TEMP%\_bak_declined.json` |
| `queue.json` | `%TEMP%\_bak_queue.json` |

---

## Lịch sử version gần nhất

| Version | Thay đổi |
|---|---|
| 1.2.0 | Queue persist ra `queue.json`, backup qua update |
| 1.1.9 | 5 thẻ max, proxy used_count single profile, queue row delete (nút ✕), bảo vệ `declined_results.json` |
| 1.1.8 | (trước đó) |
