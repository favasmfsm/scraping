"""
Shared Playwright browser utilities for SERFF scraping.

Replaces duplicated Selenium create_driver / authenticate_driver / restart_browser
logic across all scraping scripts with a single centralized module.

Usage (sync API, compatible with multiprocessing):
    from browser_utils import (
        create_browser, authenticate, navigate_via_search,
        navigate_with_session_check, click_select_buttons,
        wait_for_ajax, download_zip, restart_browser,
        ensure_single_tab, is_session_expired, human_delay,
        set_reauth_lock, new_context_and_reauth,
        create_http_session, create_http_session_from_cookies,
        refresh_http_cookies, fetch_page, keepalive_session,
        pre_authenticate_all_states,
    )
"""

import os
import queue
import random
import re
import subprocess
import sys
from typing import Optional
import threading
import time
import requests as _requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# ---------------------------------------------------------------------------
# Throttle / backoff settings
# ---------------------------------------------------------------------------

BACKOFF_ON_500 = [15, 30, 60]  # seconds to wait on successive 500 errors

# Firefox user-agent strings — must match the actual browser engine to avoid
# TLS/UA mismatch detection.
_FIREFOX_USER_AGENTS = [
    # Firefox 133 – macOS Sonoma
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Firefox 132 – macOS Sonoma
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Firefox 131 – macOS Ventura
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:131.0) Gecko/20100101 Firefox/131.0",
    # Firefox 133 – Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Firefox 132 – Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Firefox 131 – Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    # Firefox 133 – Windows 11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Firefox 130 – macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:130.0) Gecko/20100101 Firefox/130.0",
    # Firefox 129 – macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:129.0) Gecko/20100101 Firefox/129.0",
    # Firefox 128 ESR – Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

# Safari/WebKit-aligned user-agent strings.
_WEBKIT_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]

# Chrome user-agent strings — used when the launched engine is Chromium.
# These MUST be Chrome UAs (not Firefox) or anti-bot WAFs flag the
# UA/TLS/Client-Hints mismatch and return 403 (observed on SERFF).
_CHROME_USER_AGENTS = [
    # Chrome 134 – macOS Sonoma
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    # Chrome 133 – macOS Sonoma
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Chrome 132 – macOS Ventura
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # Chrome 134 – Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    # Chrome 133 – Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Chrome 132 – Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
]

# Viewport sizes that look like real monitors
_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
    {"width": 2560, "height": 1440},
]

# Stealth JS that avoids engine-specific spoofing.
_STEALTH_SCRIPT_COMMON = """
    // Hide webdriver flag (primary automation indicator)
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    try { delete navigator.__proto__.webdriver; } catch(e) {}

    // Languages
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    Object.defineProperty(navigator, 'language', {get: () => 'en-US'});

    Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});

    // Fake permissions API
    if (navigator.permissions && navigator.permissions.query) {
        const origQuery = navigator.permissions.query.bind(navigator.permissions);
        navigator.permissions.query = (params) => (
            params.name === 'notifications'
                ? Promise.resolve({state: Notification.permission})
                : origQuery(params)
        );
    }

    // Override hardwareConcurrency
    Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});

    // Canvas fingerprint noise
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {
        if (type === 'image/png' || type === undefined) {
            const ctx = this.getContext('2d');
            if (ctx) {
                const imageData = ctx.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 100) {
                    imageData.data[i] = imageData.data[i] ^ 1;
                }
                ctx.putImageData(imageData, 0, 0);
            }
        }
        return origToDataURL.apply(this, arguments);
    };
"""

# Firefox-only additions (keep browser fingerprints internally consistent).
_STEALTH_SCRIPT_FIREFOX = """
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                {name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format'},
                {name: 'Firefox PDF Viewer', filename: 'internal-pdf-viewer', description: ''},
            ];
            plugins.length = 2;
            return plugins;
        }
    });
"""

# Optional proxy — set env var SCRAPER_PROXY to use (e.g. "http://user:pass@host:port")
PROXY_URL = os.environ.get("SCRAPER_PROXY", "")

# Jittered exponential backoff for auth retries (min_seconds, max_seconds)
_AUTH_BACKOFF = [(10, 20), (30, 90), (60, 180), (120, 300)]

# Global re-auth lock — set by worker initializer via set_reauth_lock()
_reauth_lock = None
# Max seconds to wait for the lock (another worker may be hung in authenticate).
# 0 means wait indefinitely (legacy). Default avoids permanent cross-worker deadlock.
_reauth_lock_timeout_sec = 600

# When True, all browser launches are headed (visible) so the user can
# solve CAPTCHAs.  Set via set_headed_mode() in worker initializer.
_headed_mode = False


def set_reauth_lock(lock):
    """Set the global re-auth lock (called from worker initializer)."""
    global _reauth_lock
    _reauth_lock = lock


def set_reauth_lock_timeout(seconds):
    """Set max wait for re-auth lock (0 = block forever). Called from worker initializer."""
    global _reauth_lock_timeout_sec
    _reauth_lock_timeout_sec = max(0, int(seconds))


def set_headed_mode(headed):
    """Set global headed mode (called from worker initializer)."""
    global _headed_mode
    _headed_mode = headed


# ---------------------------------------------------------------------------
# Browser lifecycle
# ---------------------------------------------------------------------------


def _pick_identity(browser_family):
    """Pick a random user-agent + viewport combo for a new context.

    The UA family MUST match the launched engine. Mismatch (e.g. Chromium
    engine announcing Firefox UA) is a strong anti-bot signal — SERFF's WAF
    returns 403 in that case.
    """
    if browser_family == "webkit":
        agents = _WEBKIT_USER_AGENTS
    elif browser_family == "chromium":
        agents = _CHROME_USER_AGENTS
    else:
        agents = _FIREFOX_USER_AGENTS
    return random.choice(agents), random.choice(_VIEWPORTS)


_FIREFOX_PREFS = {
    "dom.webdriver.enabled": False,
    "useAutomationExtension": False,
    "privacy.trackingprotection.enabled": False,
    "network.http.connection-timeout": 90,
}


def _launch_browser(pw, headed=None, state_name=None):
    """Launch preferred browser with optional proxy and anti-detection prefs.
    If headed is None, uses the global _headed_mode setting.

    Browser selection precedence (first wins):
      1. SERFF_BROWSER_<STATE>  e.g. SERFF_BROWSER_CO=firefox
      2. SERFF_BROWSER          (global override)
      3. Platform default       (chromium on macOS, firefox elsewhere)
    """
    if headed is None:
        headed = _headed_mode
    default_browser = "chromium" if sys.platform == "darwin" else "firefox"
    per_state_env = None
    if state_name:
        per_state_env = os.environ.get(f"SERFF_BROWSER_{state_name.upper()}")
    preferred = (
        per_state_env or os.environ.get("SERFF_BROWSER") or default_browser
    ).strip().lower()

    def _launch_chromium():
        chrome_channel = (os.environ.get("SERFF_CHROME_CHANNEL") or "chrome").strip()
        kwargs = dict(
            headless=not headed,
            args=["--disable-blink-features=AutomationControlled"],
        )
        if PROXY_URL:
            kwargs["proxy"] = {"server": PROXY_URL}
        if sys.platform == "linux":
            kwargs["args"].extend(["--no-sandbox", "--disable-dev-shm-usage"])
        if chrome_channel:
            kwargs["channel"] = chrome_channel
        try:
            return pw.chromium.launch(**kwargs)
        except Exception as e:
            # If system Chrome channel is unavailable, fall back to bundled Chromium.
            if kwargs.get("channel"):
                channel_name = kwargs.pop("channel")
                print(
                    f"[BROWSER][WARN] Chromium channel '{channel_name}' unavailable: "
                    f"{str(e).splitlines()[0]} — trying bundled Chromium."
                )
                return pw.chromium.launch(**kwargs)
            raise

    def _launch_firefox():
        kwargs = dict(
            headless=not headed,
            firefox_user_prefs=_FIREFOX_PREFS,
        )
        if PROXY_URL:
            kwargs["proxy"] = {"server": PROXY_URL}
        return pw.firefox.launch(**kwargs)

    def _launch_webkit():
        kwargs = dict(
            headless=not headed,
        )
        if PROXY_URL:
            kwargs["proxy"] = {"server": PROXY_URL}
        return pw.webkit.launch(**kwargs)

    if preferred in ("webkit", "safari"):
        try:
            return _launch_webkit()
        except Exception as e:
            print(
                f"[BROWSER][WARN] WebKit launch failed: {str(e).splitlines()[0]} "
                f"— falling back to Chromium."
            )
            try:
                return _launch_chromium()
            except Exception as e2:
                print(
                    f"[BROWSER][WARN] Chromium fallback failed: {str(e2).splitlines()[0]} "
                    f"— falling back to Firefox."
                )
                return _launch_firefox()

    if preferred in ("chromium", "chrome"):
        try:
            return _launch_chromium()
        except Exception as e:
            print(
                f"[BROWSER][WARN] Chromium launch failed: {str(e).splitlines()[0]} "
                f"— falling back to Firefox."
            )
            return _launch_firefox()

    try:
        return _launch_firefox()
    except Exception as e:
        print(
            f"[BROWSER][WARN] Firefox launch failed: {str(e).splitlines()[0]} "
            f"— falling back to Chromium."
        )
        return _launch_chromium()


def _new_stealth_context(browser):
    """Create a new browser context with randomized anti-detection settings.

    Set env var SERFF_NATIVE_UA=1 to skip the user_agent override entirely
    and let the launched browser use its native UA + Client Hints. That is
    the most authentic possible fingerprint (zero chance of UA/TLS/Sec-CH-UA
    mismatch) and is recommended when launching real Chrome via channel='chrome'.
    """
    browser_family = "firefox"
    try:
        browser_family = browser.browser_type.name
    except Exception:
        pass
    _, vp = _pick_identity(browser_family)
    use_native_ua = os.environ.get("SERFF_NATIVE_UA", "").strip() in ("1", "true", "yes")
    ctx_kwargs = dict(
        accept_downloads=True,
        viewport=vp,
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    if not use_native_ua:
        ua, _vp_ignored = _pick_identity(browser_family)
        ctx_kwargs["user_agent"] = ua
    context = browser.new_context(**ctx_kwargs)
    context.add_init_script(_STEALTH_SCRIPT_COMMON)
    if browser_family == "firefox":
        context.add_init_script(_STEALTH_SCRIPT_FIREFOX)
    return context


def human_delay(low=0.5, high=2.0):
    """Small random delay to mimic human interaction timing."""
    time.sleep(random.uniform(low, high))


def create_browser():
    """
    Launch a browser instance with an isolated context.
    Returns (pw, browser, context, page).

    Honors the global headed flag (set via set_headed_mode); pass --headed at
    the CLI to make every worker browser visible, or --headless (default in
    most invocations) to keep it hidden.
    """
    pw = sync_playwright().start()
    browser = _launch_browser(pw, headed=None)  # None -> use _headed_mode
    context = _new_stealth_context(browser)
    page = context.new_page()
    return pw, browser, context, page


def restart_browser(pw, browser, state_name):
    """
    Kill the current browser and start a fresh one with a new identity.
    Re-authenticates. Returns (browser, context, page, auth_ok).
    """
    print(f"[RESTART] Killing browser and starting fresh for {state_name}...")
    try:
        browser.close()
    except Exception:
        pass

    # Pause before restart to avoid rapid reconnect pattern
    time.sleep(random.uniform(3, 8))

    browser = _launch_browser(pw)
    context = _new_stealth_context(browser)
    page = context.new_page()
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
# Authentication & session
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
    print(
        f"[500] Server error detected (attempt {attempt + 1}). Waiting {delay}s before retry..."
    )
    time.sleep(delay)
    return authenticate(page, state_name)


_CAPTCHA_REAUTH_WAIT = 90  # seconds to wait for user in headed re-auth


def _is_verification_screen_body(body_text):
    """Return True if the page body indicates a human verification/captcha screen."""
    if not body_text:
        return False
    lower = body_text.lower()
    return "confirm you are human" in lower or "complete the security check" in lower


def _headed_reauth_impl(state_name):
    """
    Open a temporary headed browser for the user to solve a CAPTCHA.
    Returns cookie list on success, None on timeout.
    Must be run in a dedicated thread to avoid "Sync API inside asyncio loop" errors.
    """
    print(f"\n[HEADED] Opening visible browser for {state_name} — solve CAPTCHA...")
    pw = sync_playwright().start()
    browser = _launch_browser(pw, headed=True)
    cookies = None
    try:
        context = _new_stealth_context(browser)
        page = context.new_page()
        auth_url = f"https://filingaccess.serff.com/sfa/home/{state_name}"
        page.goto(auth_url, wait_until="domcontentloaded", timeout=30000)

        # Try auto-clicking in case no CAPTCHA this time
        try:
            begin = page.locator(
                "a[href*='userAgreement.xhtml']:has-text('Begin Search')"
            )
            begin.wait_for(state="visible", timeout=8000)
            begin.click(timeout=5000)
            human_delay(0.5, 1.0)
            accept = page.locator("button:has-text('Accept')")
            accept.wait_for(state="visible", timeout=8000)
            accept.click(timeout=5000, no_wait_after=True)
            try:
                page.wait_for_url("**/filingSearch*", timeout=10000)
            except Exception:
                page.wait_for_load_state("networkidle", timeout=10000)
            if "filingSearch" in page.url:
                cookies = context.cookies()
                print(f"[HEADED] {state_name} authenticated (no CAPTCHA)")
                return cookies
        except Exception:
            pass

        # CAPTCHA likely present — poll for user to complete the flow
        deadline = time.time() + _CAPTCHA_REAUTH_WAIT
        print(
            f"[HEADED] Waiting up to {_CAPTCHA_REAUTH_WAIT}s for you to solve CAPTCHA for {state_name}..."
        )
        while time.time() < deadline:
            try:
                if "filingSearch" in page.url:
                    cookies = context.cookies()
                    print(f"[HEADED] {state_name} authenticated!")
                    return cookies

                begin = page.locator(
                    "a[href*='userAgreement.xhtml']:has-text('Begin Search')"
                )
                if begin.is_visible():
                    begin.click(timeout=5000)
                    human_delay(0.5, 1.0)
                    accept = page.locator("button:has-text('Accept')")
                    try:
                        accept.wait_for(state="visible", timeout=10000)
                        accept.click(timeout=5000, no_wait_after=True)
                        try:
                            page.wait_for_url("**/filingSearch*", timeout=10000)
                        except Exception:
                            page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass
                    if "filingSearch" in page.url:
                        cookies = context.cookies()
                        print(f"[HEADED] {state_name} authenticated!")
                        return cookies
            except Exception:
                pass
            time.sleep(3)

        print(f"[HEADED] Timed out for {state_name}")
        return None
    finally:
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _headed_reauth(state_name):
    """
    Run headed re-auth in a dedicated thread so sync Playwright is not used
    inside an asyncio loop (avoids "Sync API inside asyncio loop" in worker processes).
    Returns cookie list on success, None on timeout.
    """
    result_queue = queue.Queue()

    def run():
        try:
            cookies = _headed_reauth_impl(state_name)
            result_queue.put(("ok", cookies))
        except Exception as e:
            result_queue.put(("error", e))

    thread = threading.Thread(target=run, daemon=False)
    thread.start()
    try:
        status, value = result_queue.get(timeout=_CAPTCHA_REAUTH_WAIT + 60)
    except queue.Empty:
        print("[HEADED] Thread did not return in time")
        return None
    if status == "error":
        print(f"[HEADED] Headed re-auth failed: {value}")
        return None
    return value


def headed_reauth(state_name):
    """
    Public wrapper for _headed_reauth. Opens a visible browser for the user to
    complete Human Verification / CAPTCHA. Returns cookie list on success, None on timeout.
    """
    return _headed_reauth(state_name)


def play_human_verification_alert():
    """Play a system sound to alert the user that manual verification is needed."""
    try:
        if sys.platform == "darwin":
            subprocess.run(
                ["afplay", "/System/Library/Sounds/Glass.aiff"],
                check=False,
                timeout=2,
                capture_output=True,
            )
        else:
            # Windows: winsound.Beep(1000, 500) would need import; fallback to terminal bell
            print("\a", end="", flush=True)
    except Exception:
        print("\a", end="", flush=True)


def apply_cookies_to_browser(browser, cookies, old_context=None):
    """
    Close old context (if any), create a new context, add cookies, and return (context, page).
    Used after headed_reauth() to apply the cookies to the worker's browser.
    """
    if old_context:
        try:
            old_context.close()
        except Exception:
            pass
    time.sleep(random.uniform(1, 2))
    context = _new_stealth_context(browser)
    context.add_cookies(cookies)
    page = context.new_page()
    return context, page


def authenticate(page, state_name, max_retries=3):
    """
    Authenticate for a specific state on SERFF.

    Uses a global lock so only one worker re-authenticates at a time.
    Retries use jittered exponential backoff.

    If retries fail and a verification screen was detected, opens a visible
    browser for the user to solve CAPTCHA, then applies cookies back to the
    caller's context.

    Returns True if successful, False otherwise.
    """
    auth_url = f"https://filingaccess.serff.com/sfa/home/{state_name}"

    lock = _reauth_lock
    lock_acquired = False
    if lock:
        to = _reauth_lock_timeout_sec
        if to and to > 0:
            if not lock.acquire(timeout=to):
                print(
                    f"[LOCK] PID {os.getpid()} timed out after {to}s waiting for re-auth lock "
                    f"(another worker may be hung) — skipping auth for {state_name}"
                )
                return False
        else:
            lock.acquire()
        lock_acquired = True
        print(f"[LOCK] PID {os.getpid()} acquired re-auth lock for {state_name}")

    try:
        verification_screen_seen = False
        for attempt in range(max_retries):
            try:
                page.goto(auth_url, wait_until="domcontentloaded", timeout=30000)
                human_delay(2.0, 4.0)

                if is_server_error(page):
                    delay = BACKOFF_ON_500[min(attempt, len(BACKOFF_ON_500) - 1)]
                    print(
                        f"[500] Auth page returned 500 for {state_name} (attempt {attempt+1}/{max_retries}), waiting {delay}s..."
                    )
                    time.sleep(delay)
                    continue

                begin_btn = page.locator(
                    "a[href*='userAgreement.xhtml']:has-text('Begin Search')"
                )
                begin_btn.wait_for(state="visible", timeout=20000)
                human_delay(0.5, 1.5)
                begin_btn.click(timeout=10000)
                human_delay(1.0, 2.5)

                # Accept is a PrimeFaces button: AJAX submit, not full page nav — do not wait for navigation on click
                accept_btn = page.locator("button:has-text('Accept')")
                accept_btn.wait_for(state="visible", timeout=20000)
                human_delay(0.3, 1.0)
                accept_btn.click(timeout=10000, no_wait_after=True)

                # Wait for redirect to search page (AJAX completes then navigates)
                try:
                    page.wait_for_url("**/filingSearch*", timeout=15000)
                except Exception:
                    try:
                        page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                human_delay(1.0, 2.5)
                print(f"[AUTH] PID {os.getpid()} authenticated for {state_name}")
                return True

            except Exception as e:
                print(
                    f"[ERROR] Auth failed for {state_name} (attempt {attempt+1}/{max_retries}): {str(e).splitlines()[0]}"
                )
                body = None
                try:
                    print(f"[DEBUG] URL: {page.url}")
                    body = page.inner_text("body")
                    print(f"[DEBUG] Body (first 300 chars): {body[:300]}")
                except Exception:
                    print(f"[DEBUG] Could not read page content")

                # Verification/captcha screen in headless — open headed and re-auth immediately
                if body and _is_verification_screen_body(body):
                    verification_screen_seen = True
                if verification_screen_seen:
                    print(
                        "[AUTH] Verification screen detected in headless — opening headed browser for you to complete it."
                    )
                    break

                if attempt < max_retries - 1:
                    lo, hi = _AUTH_BACKOFF[min(attempt, len(_AUTH_BACKOFF) - 1)]
                    wait = random.uniform(lo, hi)
                    print(f"[BACKOFF] Waiting {wait:.0f}s before retry...")
                    time.sleep(wait)

        # Open headed browser when verification/captcha is detected.
        if verification_screen_seen:
            cookies = _headed_reauth(state_name)
            if cookies:
                page.context.add_cookies(cookies)
                # Navigate to search page to confirm auth
                page.goto(
                    "https://filingaccess.serff.com/sfa/search/filingSearch.xhtml",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                return True

        print(
            f"[ERROR] Authentication failed for {state_name} after {max_retries} attempts"
        )
        return False
    finally:
        if lock and lock_acquired:
            lock.release()
            print(f"[LOCK] PID {os.getpid()} released re-auth lock for {state_name}")


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


def new_context_and_reauth(browser, state_name, old_context=None):
    """
    Create a fresh browser context (new identity) and authenticate.
    Closes the old context first to release the server-side session.
    Returns (context, page, auth_ok).
    """
    if old_context:
        try:
            old_context.close()
        except Exception:
            pass
    # Brief pause so the server registers the old session as gone
    time.sleep(random.uniform(2, 5))
    context = _new_stealth_context(browser)
    page = context.new_page()
    auth_ok = authenticate(page, state_name)
    return context, page, auth_ok


# ---------------------------------------------------------------------------
# HTTP session helpers (requests-based, no browser needed)
# ---------------------------------------------------------------------------


def extract_cookies(context):
    """Extract cookies from a Playwright context as a dict for requests."""
    return {c["name"]: c["value"] for c in context.cookies()}


def create_http_session(context):
    """
    Create a requests.Session seeded with cookies from a Playwright context.
    The session uses a matching Firefox user-agent.
    """
    session = _requests.Session()
    session.cookies.update(extract_cookies(context))
    session.headers.update(
        {
            "User-Agent": random.choice(_FIREFOX_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )
    return session


def refresh_http_cookies(context, session):
    """Update an existing requests session with fresh cookies from Playwright."""
    session.cookies.update(extract_cookies(context))


def fetch_page(http_session, url, timeout=30):
    """
    Fetch a page via requests.  Much lighter than a full browser navigation.
    Returns (html, is_expired, is_server_error) or (None, True, False) on failure.

    Notes on auth signalling:
      - 401/403 are treated as `expired=True` so the caller falls back to the
        browser re-auth + navigate path. SERFF occasionally returns 403 to
        plain `requests` traffic while the same cookies still work in a real
        browser session.
    """
    try:
        resp = http_session.get(url, timeout=timeout, allow_redirects=True)
        html = resp.text
        expired = (
            resp.status_code in (401, 403)
            or "Begin Search" in html
            or "userAgreement" in html
            or "Session Expired" in html
        )
        srv_err = resp.status_code >= 500 or "500.xhtml" in resp.url
        if resp.status_code in (401, 403):
            print(
                f"[HTTP] {resp.status_code} on {url} — flagging session expired "
                f"to force browser re-auth fallback"
            )
        return html, expired, srv_err
    except _requests.RequestException as e:
        print(f"[HTTP] Request failed for {url}: {e}")
        return None, True, False


def create_http_session_from_cookies(cookies):
    """
    Create a requests.Session from pre-authed cookies.
    Accepts either a Playwright cookie list ([{name, value, domain, ...}])
    or a plain {name: value} dict.
    """
    session = _requests.Session()
    if isinstance(cookies, list):
        for c in cookies:
            session.cookies.set(
                c["name"],
                c["value"],
                domain=c.get("domain", ""),
                path=c.get("path", "/"),
            )
    else:
        session.cookies.update(cookies)
    session.headers.update(
        {
            "User-Agent": random.choice(_FIREFOX_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )
    return session


def keepalive_session(http_session):
    """
    Send a lightweight HEAD request to the SERFF search page to keep the
    server-side session alive.  Call this periodically between rows.
    """
    try:
        http_session.head(
            "https://filingaccess.serff.com/sfa/search/filingSearch.xhtml",
            timeout=15,
        )
    except Exception:
        pass


_CAPTCHA_WAIT = 60  # seconds to wait for user to solve CAPTCHA


def _poll_for_search_page(page, context, state, deadline):
    """
    Poll the headed browser waiting for the user to complete auth.
    Auto-clicks "Begin Search" / "Accept" if they appear (CAPTCHA solved).
    Returns cookies on success, None on timeout.
    """
    while time.time() < deadline:
        try:
            url = page.url
            if "filingSearch" in url:
                return context.cookies()

            # CAPTCHA solved → "Begin Search" may have appeared, auto-click
            begin = page.locator(
                "a[href*='userAgreement.xhtml']:has-text('Begin Search')"
            )
            if begin.is_visible():
                begin.click(timeout=5000)
                human_delay(0.5, 1.0)
                accept = page.locator("button:has-text('Accept')")
                try:
                    accept.wait_for(state="visible", timeout=10000)
                    accept.click(timeout=5000, no_wait_after=True)
                    try:
                        page.wait_for_url("**/filingSearch*", timeout=10000)
                    except Exception:
                        page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass
                if "filingSearch" in page.url:
                    return context.cookies()
        except Exception:
            pass
        time.sleep(3)
    return None


def pre_authenticate_all_states(states, headed=True):
    """
    Authenticate a list of states sequentially using a single browser.

    Default mode (headed=True):
      1. Open a visible browser, try auto-clicking through auth
      2. If CAPTCHA blocks, user can solve it in the visible window
      3. If user doesn't respond within 90s, fall back to backoff retries

    Headless mode (headed=False):
      Auto-click with jittered backoff retries only.

    Returns {state_name: [full_cookie_dicts]}.
    """
    mode = "headed" if headed else "headless"
    print(f"[PRE-AUTH] Authenticating {len(states)} states ({mode})...")
    all_cookies = {}
    for i, state in enumerate(states, 1):
        print(f"[PRE-AUTH] ({i}/{len(states)}) {state}...", end=" ", flush=True)
        pw = sync_playwright().start()
        browser = _launch_browser(pw, headed=headed)
        context = _new_stealth_context(browser)
        page = context.new_page()
        try:
            # Quick auto-click attempt (2 retries — fast if no CAPTCHA)
            ok = authenticate(page, state, max_retries=2)
            if ok:
                all_cookies[state] = context.cookies()
                print(f"OK ({len(all_cookies[state])} cookies)")
                continue

            if headed:
                # Browser is visible — navigate fresh and let user solve CAPTCHA
                auth_url = f"https://filingaccess.serff.com/sfa/home/{state}"
                page.goto(auth_url, wait_until="domcontentloaded", timeout=30000)
                print(
                    f"\n[CAPTCHA] Solve verification in the browser for {state} (waiting up to {_CAPTCHA_WAIT}s)..."
                )
                deadline = time.time() + _CAPTCHA_WAIT
                cookies = _poll_for_search_page(page, context, state, deadline)
                if cookies:
                    all_cookies[state] = cookies
                    print(f"[CAPTCHA] {state} authenticated!")
                    continue
                print(f"[CAPTCHA] No response, falling back to backoff for {state}...")

            # Backoff fallback — fresh context, more retries
            try:
                context.close()
            except Exception:
                pass
            context = _new_stealth_context(browser)
            page = context.new_page()
            ok = authenticate(page, state)
            if ok:
                all_cookies[state] = context.cookies()
                print(f"OK ({len(all_cookies[state])} cookies) [backoff]")
            else:
                print("FAILED")
        except Exception as e:
            print(f"ERROR: {str(e).splitlines()[0]}")
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
            try:
                pw.stop()
            except Exception:
                pass
        human_delay(1.0, 3.0)

    ok_count = len(all_cookies)
    fail_count = len(states) - ok_count
    print(f"[PRE-AUTH] Done: {ok_count} succeeded, {fail_count} failed")
    return all_cookies


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
            print(f"[500] Server error for {url} (attempt {attempt + 1}/{max_retries})")
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
                page.locator("div.row").first.wait_for(state="attached", timeout=30000)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                return True
            except Exception as e:
                print(f"[WARN] Page load timeout for {url}: {str(e).splitlines()[0]}")
                return False

    print(f"[ERROR] All retries failed for {url}")
    return False


def navigate_via_search(
    page, context, tracking_number, state_name, *, return_reason=False
):
    """
    Navigate to a filing summary page by searching for the SERFF Tracking Number.
    Used for filings with alphanumeric IDs that can't be loaded via direct URL.

    Returns:
      - default: True on success, False on any failure (backward compatible).
      - if return_reason=True: (success: bool, reason: str). Reason is one of:
          "ok"                      success
          "server_error"            search page 500 (transient — worth retrying)
          "search_page_unavailable" search input never attached (transient)
          "search_button_failed"    couldn't click search button (transient)
          "no_results"              search returned empty table (DETERMINISTIC —
                                    do not retry with re-auth; the filing is
                                    not in this state's SERFF instance)
          "summary_load_failed"     clicked row but filing summary timed out
                                    (transient)
    """

    def _result(success, reason):
        if return_reason:
            return success, reason
        return success

    search_url = "https://filingaccess.serff.com/sfa/search/filingSearch.xhtml"
    ensure_single_tab(context, page)
    page.goto(search_url, wait_until="domcontentloaded")

    # Check for 500 error on search page load
    if is_server_error(page):
        print(f"[500] Search page returned 500 for {state_name}, waiting...")
        wait_for_server_recovery(page, state_name, 0)
        page.goto(search_url, wait_until="domcontentloaded")
        if is_server_error(page):
            print(f"[500] Search page still returning 500 after wait")
            return _result(False, "server_error")

    # Wait for search input
    input_locator = page.locator("#simpleSearch\\:serffTrackingNumber")
    try:
        input_locator.wait_for(state="attached", timeout=15000)
    except PlaywrightTimeout:
        # Search page didn't load — likely session expired or 500
        if is_server_error(page):
            print(
                f"[500] Search page 500 error, waiting before re-auth for {state_name}..."
            )
            wait_for_server_recovery(page, state_name, 0)
        else:
            print(
                f"[INFO] Search page not loaded, re-authenticating for {state_name}..."
            )
        authenticate(page, state_name)
        page.goto(search_url, wait_until="domcontentloaded")
        try:
            input_locator.wait_for(state="attached", timeout=15000)
        except PlaywrightTimeout:
            print(f"[ERROR] Search page still not available after re-auth")
            return _result(False, "search_page_unavailable")

    input_locator.fill(str(tracking_number))
    human_delay(0.5, 1.5)

    # Click search button
    try:
        page.locator("#simpleSearch\\:saveBtn").click(timeout=15000)
    except Exception:
        print(f"[ERROR] Could not click search button for {tracking_number}")
        return _result(False, "search_button_failed")

    human_delay(1.0, 2.5)

    # Wait for results table and click the first row
    try:
        page.locator("xpath=//tr[@data-ri='0']").click(timeout=20000)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
    except Exception:
        print(f"[ERROR] No search results found for {tracking_number}")
        return _result(False, "no_results")

    # Wait for the filing summary page to load
    try:
        page.locator("div.row").first.wait_for(state="attached", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        return _result(True, "ok")
    except Exception:
        print(f"[WARN] Filing summary page did not load for {tracking_number}")
        return _result(False, "summary_load_failed")


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
# NAIC / Interstate Insurance Compact (Appian SERFF Filing Access)
# ---------------------------------------------------------------------------

NAIC_SERFF_URL = "https://portals.naic.org/serff-filing-access"


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def is_interstate_insurance_compact_page(html: str) -> bool:
    """True if filing summary HTML indicates an Interstate Insurance Compact filing."""
    if not html:
        return False
    return "interstate insurance compact" in html.lower()


def _naic_soft_click_button(page, name_pat, timeout: int = 30_000):
    """
    Scroll into view, move mouse with steps, then click (no force).
    Intended for Begin Search / Accept to reduce bot-detection friction.
    """
    btn = page.get_by_role("button", name=name_pat).first
    btn.wait_for(state="visible", timeout=timeout)
    btn.scroll_into_view_if_needed(timeout=10_000)
    human_delay(0.4, 1.0)
    box = btn.bounding_box()
    if box:
        x = box["x"] + box["width"] / 2
        y = box["y"] + box["height"] / 2
        page.mouse.move(x, y, steps=10)
        human_delay(0.06, 0.18)
    click_delay = int(random.uniform(80, 180))
    btn.click(timeout=timeout, delay=click_delay)


def _download_zip_naic_on_page(
    page,
    tracking_number,
    zip_file_path,
    *,
    manual_login_seconds: float = 0,
    expect_download_timeout_ms: int = 180_000,
    zip_ready_timeout_s: float = 300.0,
) -> Optional[str]:
    """Core NAIC Appian flow on an existing page (see download_zip_via_naic_portal)."""
    tracking_number = str(tracking_number).strip()
    if not tracking_number:
        return None

    os.makedirs(os.path.dirname(os.path.abspath(zip_file_path)) or ".", exist_ok=True)
    temp_zip = f"{zip_file_path}.part"

    try:
        ensure_single_tab(page.context, page)
        page.goto(NAIC_SERFF_URL, wait_until="domcontentloaded", timeout=60_000)
        human_delay(0.6, 1.5)
        if manual_login_seconds and manual_login_seconds > 0:
            time.sleep(float(manual_login_seconds))

        _naic_soft_click_button(page, re.compile(r"Begin Search", re.I))
        human_delay(1.0, 2.0)
        _naic_soft_click_button(page, re.compile(r"Accept", re.I))
        human_delay(1.0, 2.0)

        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass

        serff_input = page.get_by_label(re.compile(r"SERFF\s+Tracking\s+Number", re.I))
        if serff_input.count() == 0:
            serff_input = page.get_by_placeholder(
                re.compile(r"Search\s+by\s+.*Tracking\s+Number", re.I)
            )
        if serff_input.count() == 0:
            print("[NAIC] SERFF Tracking Number input not found")
            return None
        serff_input.first.wait_for(state="visible", timeout=30_000)
        serff_input.first.fill("")
        serff_input.first.fill(tracking_number)
        human_delay(0.3, 0.7)

        combo = page.get_by_role(
            "combobox", name=re.compile(r"Type\s+of\s+Insurance", re.I)
        )
        combo.wait_for(state="visible", timeout=30_000)
        combo.click()
        human_delay(0.25, 0.45)
        combo.fill("")
        combo.press_sequentially("health", delay=35)
        human_delay(0.35, 0.65)

        health_pat = re.compile(r"health", re.I)
        max_health_picks = 50
        for _ in range(max_health_picks):
            opts = page.get_by_role("option").filter(has_text=health_pat)
            if opts.count() == 0:
                break
            opts.first.click(timeout=10_000)
            human_delay(0.15, 0.35)
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        human_delay(0.2, 0.4)

        search_btn = page.get_by_role("button").filter(
            has_text=re.compile(r"^\s*Search\s*$", re.I)
        )
        if search_btn.count() == 0:
            search_btn = page.locator(
                'button:has(svg[data-owl-icon-name="search"]):has-text("Search")'
            )
        if search_btn.count() == 0:
            print("[NAIC] Search button not found")
            return None
        search_btn.first.wait_for(state="visible", timeout=15_000)
        search_btn.first.click(timeout=15_000)
        human_delay(1.0, 2.0)
        try:
            page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception:
            pass

        rows_match = page.locator("tbody tr").filter(
            has_text=re.compile(re.escape(tracking_number))
        )
        if rows_match.count() > 0:
            result_link = rows_match.first.locator("a.LinkedItem---richtext_link")
        else:
            result_link = page.locator("tbody tr a.LinkedItem---richtext_link").first
        result_link.wait_for(state="visible", timeout=45_000)
        result_link.click(timeout=15_000)
        human_delay(1.0, 2.0)
        try:
            page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception:
            pass

        gen_btn = page.get_by_role(
            "button", name=re.compile(r"Generate\s+Zip\s+to\s+Download", re.I)
        )
        gen_btn.first.wait_for(state="visible", timeout=30_000)
        gen_btn.first.click(timeout=15_000)
        human_delay(0.5, 1.0)

        dl_link = page.get_by_role(
            "link", name=re.compile(r"DOWNLOAD\s+THIS\s+FILING", re.I)
        )
        deadline = time.time() + zip_ready_timeout_s
        href_ok = ""
        while time.time() < deadline:
            if dl_link.count() > 0:
                try:
                    h = dl_link.first.get_attribute("href") or ""
                except Exception:
                    h = ""
                if h.startswith("https://"):
                    href_ok = h
                    break
            time.sleep(0.75)

        if not href_ok:
            print("[NAIC] Timed out waiting for presigned DOWNLOAD THIS FILING link")
            return None

        if os.path.exists(temp_zip):
            try:
                os.remove(temp_zip)
            except OSError:
                pass

        with page.expect_download(timeout=expect_download_timeout_ms) as dl_info:
            dl_link.first.click(timeout=30_000)
        download = dl_info.value
        download.save_as(temp_zip)
        os.replace(temp_zip, zip_file_path)

        if os.path.exists(zip_file_path) and os.path.getsize(zip_file_path) > 0:
            return zip_file_path
        return None

    except PlaywrightTimeout as e:
        print(f"[NAIC] Timeout: {str(e).splitlines()[0]}")
        return None
    except Exception as e:
        print(f"[NAIC] Failed: {str(e).splitlines()[0]}")
        return None
    finally:
        if os.path.exists(temp_zip):
            try:
                if (
                    not os.path.exists(zip_file_path)
                    or os.path.getsize(zip_file_path) == 0
                ):
                    pass  # leave .part for debugging
                else:
                    os.remove(temp_zip)
            except OSError:
                pass


def download_zip_via_naic_portal(
    page,
    tracking_number,
    zip_file_path,
    *,
    manual_login_seconds: float = 0,
    expect_download_timeout_ms: int = 180_000,
    zip_ready_timeout_s: float = 300.0,
    storage_state_path: Optional[str] = None,
    use_chromium: Optional[bool] = None,
) -> Optional[str]:
    """
    On portals.naic.org (Appian), search by SERFF tracking number, filter
    Type of Insurance to options containing 'health', open the first result,
    generate the zip, and save the download to *zip_file_path*.

    Begin Search / Accept use softer mouse movement and delayed clicks (no force).

    **Non-interactive session trust (optional):**
    - ``NAIC_STORAGE_STATE_PATH``: path to Playwright ``storage_state`` JSON.
      When the file exists, NAIC runs in a **new** browser context loaded with
      that state (same browser type as *page*, unless Chromium is requested).
    - ``NAIC_USE_CHROMIUM`` (``1``/``true``/``yes``): run the NAIC flow in a
      separate Chromium instance (optional ``NAIC_CHROME_CHANNEL``, e.g. ``chrome``).
    Kwargs ``storage_state_path`` / ``use_chromium`` override env when not None.

    Returns the saved path on success, or None on failure.
    """
    raw_storage = (
        storage_state_path
        if storage_state_path is not None
        else os.environ.get("NAIC_STORAGE_STATE_PATH", "")
    )
    path_storage = (raw_storage or "").strip()
    if path_storage and not os.path.isfile(path_storage):
        print(f"[NAIC] storage state file not found: {path_storage!r}")
        path_storage = ""

    if use_chromium is None:
        use_chromium = _env_flag("NAIC_USE_CHROMIUM")

    naic_page = page
    cleanup_ctx = None
    cleanup_browser = None
    cleanup_pw = None

    try:
        if use_chromium:
            pw = sync_playwright().start()
            cleanup_pw = pw
            channel = (os.environ.get("NAIC_CHROME_CHANNEL") or "").strip() or None
            headless = not _headed_mode
            ch_args = ["--disable-blink-features=AutomationControlled"]
            if sys.platform == "linux":
                ch_args.extend(["--no-sandbox", "--disable-dev-shm-usage"])
            cleanup_browser = pw.chromium.launch(
                headless=headless, channel=channel, args=ch_args
            )
            ctx_kw = {
                "accept_downloads": True,
                "locale": "en-US",
                "viewport": {"width": 1400, "height": 900},
            }
            if path_storage:
                ctx_kw["storage_state"] = path_storage
            cleanup_ctx = cleanup_browser.new_context(**ctx_kw)
            cleanup_ctx.add_init_script(_STEALTH_SCRIPT_COMMON)
            naic_page = cleanup_ctx.new_page()
        elif path_storage:
            browser = page.context.browser
            vp = page.viewport_size
            cleanup_ctx = browser.new_context(
                accept_downloads=True,
                storage_state=path_storage,
                locale="en-US",
                viewport=vp or {"width": 1280, "height": 720},
            )
            cleanup_ctx.add_init_script(_STEALTH_SCRIPT_COMMON)
            try:
                if browser.browser_type.name == "firefox":
                    cleanup_ctx.add_init_script(_STEALTH_SCRIPT_FIREFOX)
            except Exception:
                pass
            naic_page = cleanup_ctx.new_page()

        return _download_zip_naic_on_page(
            naic_page,
            tracking_number,
            zip_file_path,
            manual_login_seconds=manual_login_seconds,
            expect_download_timeout_ms=expect_download_timeout_ms,
            zip_ready_timeout_s=zip_ready_timeout_s,
        )
    finally:
        if cleanup_ctx is not None:
            try:
                cleanup_ctx.close()
            except Exception:
                pass
        if cleanup_browser is not None:
            try:
                cleanup_browser.close()
            except Exception:
                pass
        if cleanup_pw is not None:
            try:
                cleanup_pw.stop()
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
        print(f"[WARN] ZIP download timed out (120s)")
        return None
    except Exception as e:
        print(f"[WARN] Download failed: {str(e).splitlines()[0]}")
        return None
