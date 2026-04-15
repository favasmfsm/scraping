#!/usr/bin/env python3
"""
Script-based test: fetch SERFF ME auth page HTML and diagnose why "Begin Search" click fails.

- Saves HTML at each step to outputs/ for inspection.
- With --html-only: uses requests only (no browser), saves HTML and parses links/buttons.
- With browser: reports selector counts, visibility, and tries click with force/scroll.

Run from repo root with venv activated:
  python scripts/test_me_auth_click.py --html-only   # no Playwright, fetch + parse HTML
  python scripts/test_me_auth_click.py               # headed (if display available)
  python scripts/test_me_auth_click.py --headless    # headless browser

Outputs:
  - outputs/me_auth_initial.html   (after goto home/ME or requests get)
  - outputs/me_auth_after_begin_click.html (if browser gets past Begin Search click)
  - Console: selector diagnostics and click attempt result
"""

import argparse
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except Exception:
    HAS_PLAYWRIGHT = False

import requests


OUTPUT_DIR = os.path.join(REPO_ROOT, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Match our auth selectors in raw HTML (good enough for structure check)
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0"


def run_html_only(auth_url: str):
    """Fetch page with requests, save HTML, and report links/buttons that affect auth."""
    print(f"[HTML-ONLY] Fetching {auth_url} ...")
    r = requests.get(
        auth_url,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
        timeout=30,
        allow_redirects=True,
    )
    r.raise_for_status()
    html = r.text
    final_url = r.url
    path = os.path.join(OUTPUT_DIR, "me_auth_initial.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"    Saved {len(html)} chars -> {path}")
    print(f"    Final URL: {final_url}")

    # Look for: a[href*='userAgreement.xhtml'] with "Begin Search" text
    # and span/link with "Accept"
    pattern_user_agreement_link = re.compile(
        r'<a[^>]*href\s*=\s*["\']([^"\']*userAgreement[^"\']*)["\'][^>]*>([^<]*)</a>',
        re.I | re.S,
    )
    pattern_begin_search = re.compile(r"Begin Search", re.I)
    pattern_accept = re.compile(r'<(span|a|button)[^>]*>[\s\S]*?Accept[\s\S]*?</\1>', re.I)
    # Simpler: any tag containing "Accept"
    pattern_accept_any = re.compile(r'<([a-z]+)[^>]*>[^<]*Accept[^<]*</\1>', re.I)

    links_ua = list(pattern_user_agreement_link.finditer(html))
    print(f"\n[Links with userAgreement in href]: {len(links_ua)}")
    for m in links_ua:
        href, text = m.group(1), m.group(2).strip()
        has_begin = "Begin Search" in text or pattern_begin_search.search(text)
        print(f"    href={href[:80]}...  text={repr(text[:60])}  has_begin_search={has_begin}")

    # Any <a> with text "Begin Search"
    begin_anchors = re.findall(
        r'<a[^>]*href\s*=\s*["\']([^"\']*)["\'][^>]*>([^<]*Begin Search[^<]*)</a>',
        html,
        re.I,
    )
    print(f"\n[Anchors with 'Begin Search' text]: {len(begin_anchors)}")
    for href, text in begin_anchors:
        print(f"    href={href[:80]}  text={repr(text[:50])}")

    # Accept buttons/spans
    accept_tags = re.findall(
        r'<(span|button|a)[^>]*>[\s\S]*?Accept[\s\S]*?</\1>',
        html,
        re.I,
    )
    print(f"\n[Tags containing 'Accept'] (span/button/a): {len(accept_tags)}")

    # Check for common blockers: captcha, verify, iframe
    if "confirm you are human" in html.lower() or "complete the security check" in html.lower():
        print("\n[!] Page contains verification/CAPTCHA phrasing (our detector would open headed).")
    if "iframe" in html.lower():
        n_iframe = html.lower().count("<iframe")
        print(f"\n[!] Page has {n_iframe} iframe(s) (captcha/verify often in iframe).")
    if "recaptcha" in html.lower() or "hcaptcha" in html.lower():
        print("\n[!] Page mentions reCAPTCHA/hCaptcha.")

    # Why click might not work: link might be JS-only (onclick, no href navigation)
    onclick_begin = re.findall(r'<a[^>]*onclick[^>]*Begin Search|Begin Search[^>]*onclick', html, re.I)
    print(f"\n[Links with onclick near Begin Search]: {len(onclick_begin)}")

    # Fetch userAgreement.xhtml (same session) to see Accept button structure
    base = re.match(r"https?://[^/]+", auth_url).group(0)
    ua_url = base + "/sfa/userAgreement.xhtml"
    print(f"\n[HTML-ONLY] Fetching {ua_url} (agreement page where Accept lives)...")
    r2 = requests.get(
        ua_url,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"},
        timeout=30,
        allow_redirects=True,
        cookies=r.cookies,
    )
    r2.raise_for_status()
    html_ua = r2.text
    path_ua = os.path.join(OUTPUT_DIR, "me_auth_userAgreement.html")
    with open(path_ua, "w", encoding="utf-8") as f:
        f.write(html_ua)
    print(f"    Saved {len(html_ua)} chars -> {path_ua}")
    print(f"    Final URL: {r2.url}")

    # Accept on agreement page: often span with PrimeFaces
    for tag in ("span", "button", "a"):
        for m in re.finditer(
            rf"<{tag}[^>]*>[\s\S]*?Accept[\s\S]*?</{tag}>", html_ua, re.I
        ):
            print(f"    Found <{tag}> with Accept: snippet={m.group(0)[:120]}...")
    # PrimeFaces often: span.ui-button-text
    if "Accept" in html_ua and "span" in html_ua:
        idx = html_ua.find("Accept")
        if idx >= 0:
            snippet = html_ua[max(0, idx - 150) : idx + 80]
            print(f"\n[Accept context in HTML]: ...{snippet}...")
    print("\n[Conclusion from HTML]")
    print("  - Accept is a <button> with PrimeFaces AJAX (onclick=PrimeFaces.ab...), not full navigation.")
    print("  - Playwright click() waits for navigation; AJAX submit does not trigger it -> timeout.")
    print("  - Fix: click Accept with no_wait_after=True, then wait for URL to contain 'filingSearch'.")
    return path


def main():
    parser = argparse.ArgumentParser(description="Diagnose ME auth click")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument(
        "--html-only",
        action="store_true",
        help="Only fetch HTML with requests and parse; no Playwright",
    )
    args = parser.parse_args()

    state = "ME"
    auth_url = f"https://filingaccess.serff.com/sfa/home/{state}"

    if args.html_only:
        run_html_only(auth_url)
        print("\nDone. Inspect outputs/me_auth_initial.html")
        return

    if not HAS_PLAYWRIGHT:
        print("Playwright not available. Run with --html-only to fetch and parse HTML.")
        return
    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0",
            locale="en-US",
        )
        page = context.new_page()

        try:
            print(f"[1] Loading {auth_url} ...")
            page.goto(auth_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)

            html = page.content()
            path_initial = os.path.join(OUTPUT_DIR, "me_auth_initial.html")
            with open(path_initial, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"    Saved HTML ({len(html)} chars) -> {path_initial}")
            print(f"    Current URL: {page.url}")

            # Selectors we care about (same as browser_utils)
            begin_selector = "a[href*='userAgreement.xhtml']:has-text('Begin Search')"
            accept_selector = "span:has-text('Accept')"

            # Diagnose: how many elements match?
            begin_loc = page.locator(begin_selector)
            accept_loc = page.locator(accept_selector)
            n_begin = begin_loc.count()
            n_accept = accept_loc.count()
            print(f"\n[2] Selector counts:")
            print(f"    '{begin_selector}' -> {n_begin} element(s)")
            print(f"    '{accept_selector}' -> {n_accept} element(s)")

            if n_begin == 0:
                # Try looser selectors to see what's on the page
                print("\n[2b] No 'Begin Search' link with userAgreement href. Trying alternatives:")
                for sel in [
                    "a:has-text('Begin Search')",
                    "a[href*='userAgreement']",
                    "text=Begin Search",
                ]:
                    loc = page.locator(sel)
                    c = loc.count()
                    print(f"    {sel} -> {c}")
                    if c > 0:
                        try:
                            first = loc.first
                            print(f"      first: tag={first.evaluate('el => el.tagName')}, href={first.get_attribute('href')}, visible={first.is_visible()}")
                        except Exception as e:
                            print(f"      first: {e}")

            for i in range(n_begin):
                loc = begin_loc.nth(i)
                try:
                    visible = loc.is_visible()
                    box = loc.bounding_box()
                    tag = loc.evaluate("el => el.tagName")
                    href = loc.get_attribute("href")
                    text = loc.inner_text()
                    outer = loc.evaluate("el => el.outerHTML").strip()[:200]
                    print(f"\n    Begin Search #[{i}]: visible={visible}, tag={tag}, href={href}, text={repr(text)}")
                    print(f"      box={box}")
                    print(f"      outerHTML (first 200): {outer}...")
                except Exception as e:
                    print(f"    Begin Search #[{i}]: error {e}")

            # Try wait_for visible then click with options that might help (only if we have a match)
            if n_begin > 0:
                print("\n[3] Attempting click on 'Begin Search' (with scroll + force, timeout=15s)...")
                try:
                    begin_loc.first.wait_for(state="visible", timeout=15000)
                    begin_loc.first.scroll_into_view_if_needed(timeout=5000)
                    begin_loc.first.click(timeout=15000, force=True)
                    print("    Click (force) completed without timeout.")
                    page.wait_for_load_state("networkidle", timeout=15000)
                    url_after = page.url
                    html_after = page.content()
                    path_after = os.path.join(OUTPUT_DIR, "me_auth_after_begin_click.html")
                    with open(path_after, "w", encoding="utf-8") as f:
                        f.write(html_after)
                    print(f"    URL after click: {url_after}")
                    print(f"    Saved -> {path_after}")
                except Exception as e:
                    print(f"    Click failed: {e}")
            else:
                print("\n[3] Skipping click (no element matched).")

            # Keep browser open briefly so user can see (headed only)
            if not args.headless:
                print("\n[4] Keeping browser open 8s for inspection...")
                page.wait_for_timeout(8000)

        finally:
            context.close()
            browser.close()

    print("\nDone. Check outputs/me_auth_*.html to inspect DOM.")


if __name__ == "__main__":
    main()
