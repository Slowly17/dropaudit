"""
automation_example.py
─────────────────────
Template viết automation cho từng profile trong Anti-Detect Browser Manager.

Cách dùng:
  python automation_example.py --profile <profile_id>
  python automation_example.py --all          # chạy tất cả profile song song
  python automation_example.py --list         # xem danh sách profile

Yêu cầu:
  - main.py đang chạy (http://localhost:8099)
  - pip install requests invisible_playwright @ git+https://...
"""

import argparse
import json
import threading
import requests
from invisible_playwright import InvisiblePlaywright

API = "http://localhost:8099"


# ══════════════════════════════════════════════════════════════════════════════
# ① VIẾT LOGIC AUTOMATION CỦA BẠN Ở ĐÂY
# ══════════════════════════════════════════════════════════════════════════════
def run_automation(page, profile_name: str, profile_id: str):
    """
    Hàm này nhận `page` là Playwright Page object đã được khởi tạo
    với đúng proxy + fingerprint của profile.

    Viết bất kỳ automation nào tại đây.
    """

    print(f"[{profile_name}] Bắt đầu automation...")

    # ── Ví dụ 1: Mở trang và chụp ảnh ──────────────────────────────────────
    page.goto("https://example.com")
    page.screenshot(path=f"screenshot_{profile_id}.png")
    print(f"[{profile_name}] Đã chụp screenshot.")

    # ── Ví dụ 2: Điền form login ─────────────────────────────────────────────
    # page.goto("https://yoursite.com/login")
    # page.fill("#username", "your_user")
    # page.fill("#password", "your_pass")
    # page.click("#submit")
    # page.wait_for_load_state("networkidle")
    # print(f"[{profile_name}] Đã login.")

    # ── Ví dụ 3: Lấy text nội dung trang ────────────────────────────────────
    # title = page.title()
    # print(f"[{profile_name}] Title: {title}")

    # ── Ví dụ 4: Click theo tọa độ (anti-bot friendly) ──────────────────────
    # page.mouse.move(200, 300)   # di chuột tự nhiên
    # page.mouse.click(200, 300)

    print(f"[{profile_name}] Automation hoàn tất.")


# ══════════════════════════════════════════════════════════════════════════════
# CORE — Không cần sửa bên dưới
# ══════════════════════════════════════════════════════════════════════════════
def get_profiles() -> list[dict]:
    """Lấy danh sách profiles từ app."""
    r = requests.get(f"{API}/api/profiles", timeout=10)
    r.raise_for_status()
    return r.json()


def get_context(profile_id: str) -> dict:
    """Lấy kwargs InvisiblePlaywright cho profile."""
    r = requests.get(f"{API}/api/profiles/{profile_id}/context", timeout=10)
    r.raise_for_status()
    return r.json()


def run_profile(profile_id: str):
    """Khởi tạo InvisiblePlaywright với tài nguyên của profile và chạy automation."""
    try:
        ctx = get_context(profile_id)
        name = ctx["name"]
        kwargs = ctx["kwargs"]   # proxy, seed, timezone — đúng từ profile

        print(f"\n[{name}] Khởi tạo browser (proxy={kwargs.get('proxy', {}).get('server', 'none')}, seed={kwargs.get('seed', 'random')})...")

        ip = InvisiblePlaywright(**kwargs)
        with ip as browser:
            print(f"[{name}] Browser ready | seed={ip.seed}")
            page = browser.new_page()
            run_automation(page, name, profile_id)

    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Không kết nối được app. Đảm bảo main.py đang chạy tại {API}")
    except Exception as e:
        print(f"[ERROR] Profile {profile_id}: {e}")


def run_all_parallel(profiles: list[dict]):
    """Chạy tất cả profiles cùng lúc, mỗi profile một thread."""
    threads = []
    for p in profiles:
        t = threading.Thread(target=run_profile, args=(p["id"],), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n✓ Tất cả profiles đã chạy xong.")


def list_profiles(profiles: list[dict]):
    print(f"\n{'ID':<12} {'Tên':<25} {'Proxy':<35} {'Seed':<8} {'Timezone'}")
    print("─" * 95)
    for p in profiles:
        proxy = p.get("proxy_server") or "—"
        seed  = str(p.get("seed")) if p.get("seed") is not None else "random"
        tz    = p.get("timezone") or "auto"
        print(f"{p['id']:<12} {p['name']:<25} {proxy:<35} {seed:<8} {tz}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automation cho Anti-Detect Browser Manager")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--profile", metavar="ID", help="Chạy 1 profile theo ID")
    group.add_argument("--all",     action="store_true", help="Chạy tất cả profiles song song")
    group.add_argument("--list",    action="store_true", help="Liệt kê danh sách profiles")
    args = parser.parse_args()

    profiles = get_profiles()

    if args.list:
        list_profiles(profiles)

    elif args.all:
        if not profiles:
            print("Không có profile nào. Tạo profile trong app trước.")
        else:
            print(f"Chạy {len(profiles)} profiles song song...")
            run_all_parallel(profiles)

    elif args.profile:
        run_profile(args.profile)
