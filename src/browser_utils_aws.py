"""
Playwright-based browser utilities for SERFF filing scraper (AWS EC2 version).

Standalone module — does NOT import from browser_utils.py (local version).
Provides helpers for browser creation, authentication, navigation,
session management, and file downloads using Playwright's sync API.

AWS-specific features:
  - CDP-based cache/cookie clearing (handles persistent Chromium state)
  - /tmp cleanup for stale Chromium temp directories
  - --no-sandbox and --disable-dev-shm-usage (required on EC2 Linux)
  - Anti-detection stealth (user agent, JS overrides)
  - 500 error handling with exponential backoff
"""

import os
import random
import shutil
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# ---------------------------------------------------------------------------
# Throttle / backoff settings
# ---------------------------------------------------------------------------

BACKOFF_ON_500 = [15, 30, 60]  # seconds to wait on successive 500 errors

# Realistic user-agent (Chrome 131 on Linux — matches EC2 environment)
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# JS to inject on every new page to hide automation fingerprints (Dynatrace)
_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    window.chrome = { runtime: {} };
"""


# ---------------------------------------------------------------------------
# AWS-specific: cache & temp cleanup
# ---------------------------------------------------------------------------


def _clear_cache(context, page):
    """Clear browser cache, cookies, and storage via CDP (AWS-specific)."""
    try:
        client = context.new_cdp_session(page)
        client.send("Network.clearBrowserCache")
        client.send("Network.clearBrowserCookies")
        client.detach()
    except Exception as e:
        print(f"[WARN] Cache clear failed: {str(e).splitlines()[0]}")


def cleanup_tmp_chromium_dirs(max_age_hours=2):
    """
    Remove stale Chromium temp directories from /tmp to prevent disk exhaustion.
    Only removes directories older than *max_age_hours* to avoid killing
    active browser sessions.
    """
    cleaned_count = 0
    cleaned_size = 0
    now = time.time()
    max_age_secs = max_age_hours * 3600

    tmp_locations = [
        "/tmp",
        "/tmp/snap-private-tmp/snap.chromium/tmp",
    ]

    # Prefixes used by Playwright / Chromium for temp dirs
    chromium_prefixes = (
        "playwright",
        "chromium",
        "chrome_cache_",
        ".org.chromium.",
        "puppeteer_dev_chrome_profile",
    )

    for base_dir in tmp_locations:
        if not os.path.exists(base_dir):
            continue
        try:
            for item in os.listdir(base_dir):
                item_path = os.path.join(base_dir, item)
                if not os.path.isdir(item_path):
                    continue

                # Check if it looks like a Chromium temp dir
                item_lower = item.lower()
                if not any(item_lower.startswith(p) for p in chromium_prefixes):
                    continue

                # Only remove if older than max_age_hours
                try:
                    mtime = os.path.getmtime(item_path)
                    if (now - mtime) < max_age_secs:
                        continue
                except OSError:
                    continue

                try:
                    size = sum(
                        os.path.getsize(os.path.join(dp, fn))
                        for dp, _, fns in os.walk(item_path)
                        for fn in fns
                    )
                    cleaned_size += size
                except OSError:
                    pass

                shutil.rmtree(item_path, ignore_errors=True)
                cleaned_count += 1
        except OSError:
            pass

    if cleaned_count > 0:
        size_mb = cleaned_size / (1024 * 1024)
        print(f"[CLEANUP] Removed {cleaned_count} stale Chromium temp dirs (~{size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Browser lifecycle
# ---------------------------------------------------------------------------


def _new_stealth_context(browser):
    """Create a new browser context with anti-detection settings and download support."""
    context = browser.new_context(
        accept_downloads=True,
        user_agent=_USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
    )
    # Inject stealth JS before every page navigation (bypasses Dynatrace RUM)
    context.add_init_script(_STEALTH_SCRIPT)
    return context


def create_browser():
    """
    Launch a headless Chromium instance with an isolated context.
    Returns (pw, browser, context, page).

    Includes:
      - EC2-required launch args (--no-sandbox, --disable-dev-shm-usage)
      - Anti-detection (stealth JS, user agent, disable automation flag)
      - CDP cache clearing for clean state
    """
    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    context = _new_stealth_context(browser)
    page = context.new_page()
    # One-time cleanup of any leftover cookies/cache before authentication
    _clear_cache(context, page)
    return pw, browser, context, page


def restart_browser(pw, browser, state_name):
    """
    Close the current browser and start a fresh one. Re-authenticates.
    Clears CDP cache on the new instance.
    Returns (browser, context, page, auth_ok).
    """
    print(f"[RESTART] Killing browser and starting fresh for {state_name}...")
    try:
        browser.close()
    except Exception:
        pass

    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    context = _new_stealth_context(browser)
    page = context.new_page()
    # Clear cache on the fresh browser to ensure clean state
    _clear_cache(context, page)

    auth_ok = authenticate(page, state_name)
    if not auth_ok:
        print(
            f"[ERROR] Re-authentication failed after browser restart for {state_name}"
        )
    return browser, context, page, auth_ok


def ensure_single_tab(context, page):
    """Close extra tabs that SERFF may have spawned, keep the main page."""
    for p in context.pages:
        if p != page:
            try:
                p.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 500 error handling
# ---------------------------------------------------------------------------


def is_server_error(page):
    """Check if the current page is a 500 Internal Server Error."""
    try:
        return "500.xhtml" in page.url or "Internal Server Error" in page.content()
    except Exception:
        return False


def wait_for_server_recovery(page, state_name, attempt=0):
    """
    When the server returns 500, wait with exponential backoff before retrying.
    Re-authenticates after waiting. Returns True if re-auth succeeded.
    """
    delay = BACKOFF_ON_500[min(attempt, len(BACKOFF_ON_500) - 1)]
    delay += random.uniform(2, 10)
    print(
        f"[500] Server error detected (attempt {attempt + 1}). "
        f"Waiting {delay:.0f}s before retry..."
    )
    time.sleep(delay)
    return authenticate(page, state_name)


# ---------------------------------------------------------------------------
# Authentication & session
# ---------------------------------------------------------------------------


def authenticate(page, state_name, max_500_retries=3):
    """
    Authenticate for a specific state on SERFF.
    Returns True if successful, False otherwise.
    Handles 500 errors by waiting and retrying.
    """
    auth_url = f"https://filingaccess.serff.com/sfa/home/{state_name}"

    for attempt in range(max_500_retries):
        try:
            page.goto(auth_url, wait_until="domcontentloaded")
            time.sleep(2)

            # Check for 500 error on auth page
            if is_server_error(page):
                delay = BACKOFF_ON_500[min(attempt, len(BACKOFF_ON_500) - 1)]
                delay += random.uniform(2, 10)
                print(
                    f"[500] Auth page returned 500 for {state_name} "
                    f"(attempt {attempt + 1}/{max_500_retries}), waiting {delay:.0f}s..."
                )
                time.sleep(delay)
                continue

            page.locator(
                "a[href*='userAgreement.xhtml']:has-text('Begin Search')"
            ).click(timeout=10000)
            page.locator("span:has-text('Accept')").click(timeout=10000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            return True
        except Exception as e:
            print(
                f"[ERROR] Authentication failed for {state_name} "
                f"(attempt {attempt + 1}): {str(e).splitlines()[0]}"
            )
            if attempt < max_500_retries - 1:
                jitter = 5 + random.uniform(2, 10)
                time.sleep(jitter)

    print(
        f"[ERROR] Authentication failed for {state_name} "
        f"after {max_500_retries} attempts"
    )
    return False


def is_session_expired(page):
    """
    Check whether the current page indicates a session expiration or redirect
    back to the agreement/home page.
    """
    try:
        content = page.content()
        return (
            "Session Expired" in content
            or "Begin Search" in content
            or "userAgreement" in content
        )
    except Exception:
        return True  # If we can't read the page, treat as expired


# ---------------------------------------------------------------------------
# Navigation helpers
# ---------------------------------------------------------------------------


def navigate_with_session_check(page, url, state_name, max_retries=3):
    """
    Navigate to URL with session expiration and 500 error handling.
    If session expired or 500 error, re-authenticate and retry.
    Returns True if navigation successful, False otherwise.
    """
    for attempt in range(max_retries):
        page.goto(url, wait_until="domcontentloaded")

        # Check for 500 server error first
        if is_server_error(page):
            print(
                f"[500] Server error for {url} "
                f"(attempt {attempt + 1}/{max_retries})"
            )
            wait_for_server_recovery(page, state_name, attempt)
            continue

        if is_session_expired(page):
            print(
                f"[INFO] Session expired for {url} "
                f"(attempt {attempt + 1}/{max_retries}), re-authenticating..."
            )
            if authenticate(page, state_name):
                continue  # Retry navigation after re-auth
            else:
                return False  # Re-auth failed
        else:
            # Session valid, wait for page load
            try:
                page.locator("div.row").first.wait_for(
                    state="attached", timeout=30000
                )
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                return True
            except Exception as e:
                print(
                    f"[WARN] Page load timeout for {url}: "
                    f"{str(e).splitlines()[0]}"
                )
                return False

    print(f"[ERROR] All retries failed for {url}")
    return False


def navigate_via_search(page, context, tracking_number, state_name):
    """
    Navigate to a filing summary page by searching for the SERFF Tracking Number.
    Used for filings with alphanumeric IDs that can't be loaded via direct URL.
    Returns True if the filing summary page loaded successfully, False otherwise.
    """
    search_url = "https://filingaccess.serff.com/sfa/search/filingSearch.xhtml"
    ensure_single_tab(context, page)
    page.goto(search_url, wait_until="domcontentloaded")

    # Check for 500 error on search page load
    if is_server_error(page):
        print(f"[500] Search page returned 500 for {state_name}, waiting...")
        wait_for_server_recovery(page, state_name, 0)
        page.goto(search_url, wait_until="domcontentloaded")
        if is_server_error(page):
            print("[500] Search page still returning 500 after wait")
            return False

    # Wait for search input
    input_locator = page.locator("#simpleSearch\\:serffTrackingNumber")
    try:
        input_locator.wait_for(state="attached", timeout=15000)
    except PlaywrightTimeout:
        # Search page didn't load — likely session expired or 500
        if is_server_error(page):
            print(
                f"[500] Search page 500 error, waiting before re-auth "
                f"for {state_name}..."
            )
            wait_for_server_recovery(page, state_name, 0)
        else:
            print(
                f"[INFO] Search page not loaded, re-authenticating "
                f"for {state_name}..."
            )
        authenticate(page, state_name)
        page.goto(search_url, wait_until="domcontentloaded")
        try:
            input_locator.wait_for(state="attached", timeout=15000)
        except PlaywrightTimeout:
            print("[ERROR] Search page still not available after re-auth")
            return False

    input_locator.fill(str(tracking_number))

    # Click search button
    try:
        page.locator("#simpleSearch\\:saveBtn").click(timeout=15000)
    except Exception:
        print(f"[ERROR] Could not click search button for {tracking_number}")
        return False

    # Wait for results table and click the first row
    try:
        page.locator("xpath=//tr[@data-ri='0']").click(timeout=20000)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
    except Exception:
        print(f"[ERROR] No search results found for {tracking_number}")
        return False

    # Wait for the filing summary page to load
    try:
        page.locator("div.row").first.wait_for(state="attached", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        return True
    except Exception:
        print(
            f"[WARN] Filing summary page did not load for {tracking_number}"
        )
        return False


# ---------------------------------------------------------------------------
# Page interaction helpers
# ---------------------------------------------------------------------------


def click_select_buttons(page):
    """
    Click the 'Select Current Version Only' buttons for form, rate/rule,
    and supporting document attachments, plus 'Select All' for correspondence.
    Silently skips buttons that don't exist on the page.
    """
    select_current_buttons = [
        "formAttachmentSelectCurrentButton",
        "rateRuleAttachmentSelectCurrentButton",
        "supportingDocumentAttachmentSelectCurrentButton",
    ]
    for btn_id in select_current_buttons:
        try:
            btn = page.locator(f"#{btn_id}")
            if btn.count() > 0:
                btn.click(force=True, timeout=3000)
                page.wait_for_timeout(300)
        except Exception:
            pass  # Button may not exist if panel is empty

    # For Correspondence, click "Select All"
    try:
        corr = page.locator("#correspondenceAttachmentSelectAllButton")
        if corr.count() > 0:
            corr.click(force=True, timeout=3000)
            page.wait_for_timeout(300)
    except Exception:
        pass


def wait_for_ajax(page):
    """
    Wait for PrimeFaces AJAX queue to drain after button clicks.
    Silently proceeds if PrimeFaces is not present or check fails.
    """
    try:
        page.wait_for_function(
            "typeof PrimeFaces === 'undefined' || PrimeFaces.ajax.Queue.isEmpty()",
            timeout=10000,
        )
    except Exception:
        pass  # Proceed anyway
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download_zip(page, download_path):
    """
    Click the 'Download Zip File' button and wait for the download to complete.
    Returns the full path to the saved ZIP file, or None on failure.

    Uses Playwright's first-class expect_download() — no CDP hacks,
    no .crdownload polling, no before_files snapshot needed.
    """
    try:
        dl_btn = page.locator("#summaryForm\\:downloadLink")
        dl_btn.wait_for(state="attached", timeout=15000)

        with page.expect_download(timeout=120000) as dl_info:
            dl_btn.scroll_into_view_if_needed()
            dl_btn.click(force=True)

        download = dl_info.value
        filename = download.suggested_filename or "download.zip"
        dest = os.path.join(download_path, filename)
        download.save_as(dest)
        return dest

    except PlaywrightTimeout:
        print("[WARN] ZIP download timed out (120s)")
        return None
    except Exception as e:
        print(f"[WARN] Download failed: {str(e).splitlines()[0]}")
        return None
