#!/usr/bin/env python3
"""
Headed (or headless) test for NAIC SERFF Filing Access compact ZIP download.

Uses download_zip_via_naic_portal from src/browser_utils.py.

Run from repo root with venv activated:
  python scripts/test_naic_compact_zip.py --tracking-number FRFB-129373332
  python scripts/test_naic_compact_zip.py --tracking-number ... --state ME   # SERFF auth first
  python scripts/test_naic_compact_zip.py ... --storage-state path/to/state.json
  python scripts/test_naic_compact_zip.py ... --chromium [--chrome-channel chrome]

Env (same as production): NAIC_STORAGE_STATE_PATH, NAIC_USE_CHROMIUM, NAIC_CHROME_CHANNEL

Outputs on failure:
  - outputs/naic_compact_fail.png
  - outputs/naic_compact_fail.html
"""

import argparse
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
OUTPUT_DIR = os.path.join(REPO_ROOT, "outputs")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)

from playwright.sync_api import sync_playwright

from browser_utils import (
    authenticate,
    download_zip_via_naic_portal,
    _launch_browser,
    _new_stealth_context,
    set_headed_mode,
)


def _dump_failure(page):
    png = os.path.join(OUTPUT_DIR, "naic_compact_fail.png")
    html_path = os.path.join(OUTPUT_DIR, "naic_compact_fail.html")
    try:
        page.screenshot(path=png, full_page=True)
        print(f"[FAIL] Screenshot -> {png}")
    except Exception as e:
        print(f"[FAIL] Screenshot failed: {e}")
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"[FAIL] HTML -> {html_path}")
    except Exception as e:
        print(f"[FAIL] HTML dump failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Test NAIC compact SERFF zip flow")
    parser.add_argument(
        "--tracking-number",
        required=True,
        help="SERFF tracking number to search (e.g. FRFB-129373332)",
    )
    parser.add_argument(
        "--state",
        default="",
        help="If set, run filingaccess.serff.com authenticate() for this state first",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless (default is headed)",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(OUTPUT_DIR, "naic_compact_test.zip"),
        help="Destination zip path",
    )
    parser.add_argument(
        "--manual-login-seconds",
        type=float,
        default=0,
        help="Sleep after loading NAIC URL (manual SSO/login in browser)",
    )
    parser.add_argument(
        "--storage-state",
        default="",
        help="Playwright storage_state JSON path (or set NAIC_STORAGE_STATE_PATH)",
    )
    parser.add_argument(
        "--chromium",
        action="store_true",
        help="Use dedicated Chromium for NAIC (or set NAIC_USE_CHROMIUM=1)",
    )
    parser.add_argument(
        "--chrome-channel",
        default="",
        help="e.g. chrome — sets NAIC_CHROME_CHANNEL for this run",
    )
    args = parser.parse_args()

    if args.storage_state.strip():
        os.environ["NAIC_STORAGE_STATE_PATH"] = args.storage_state.strip()
    if args.chromium:
        os.environ["NAIC_USE_CHROMIUM"] = "1"
    if args.chrome_channel.strip():
        os.environ["NAIC_CHROME_CHANNEL"] = args.chrome_channel.strip()

    headed = not args.headless
    set_headed_mode(headed)
    with sync_playwright() as pw:
        browser = _launch_browser(pw, headed=headed)
        context = _new_stealth_context(browser)
        page = context.new_page()
        try:
            if args.state.strip():
                print(f"[AUTH] filingaccess.serff.com home/{args.state.strip()} ...")
                if not authenticate(page, args.state.strip()):
                    print("[AUTH] Failed — continuing to NAIC URL anyway (may need SSO)")
            out = args.output
            print(f"[NAIC] Download to {out} ...")
            path = download_zip_via_naic_portal(
                page,
                args.tracking_number,
                out,
                manual_login_seconds=args.manual_login_seconds,
            )
            if path and os.path.isfile(path) and os.path.getsize(path) > 0:
                print(f"[OK] Saved {path} ({os.path.getsize(path)} bytes)")
                return 0
            print("[FAIL] No zip saved")
            _dump_failure(page)
            return 1
        except Exception as e:
            print(f"[FAIL] {str(e).splitlines()[0]}")
            _dump_failure(page)
            return 1
        finally:
            if headed:
                print("[INFO] Keeping browser open 5s ...")
                try:
                    page.wait_for_timeout(5000)
                except Exception:
                    pass
            context.close()
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
