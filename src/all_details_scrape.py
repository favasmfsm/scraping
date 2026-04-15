import argparse
import ast
from datetime import datetime
import pandas as pd
import os
import re
import random
import signal
import time
import shutil
from multiprocessing import Pool, Manager, Value, Lock
from tqdm import tqdm
import threading
from bs4 import BeautifulSoup

from browser_utils import (
    create_browser,
    authenticate,
    navigate_via_search,
    is_server_error,
    human_delay,
    set_reauth_lock,
    set_reauth_lock_timeout,
    set_headed_mode,
    new_context_and_reauth,
    create_http_session,
    create_http_session_from_cookies,
    refresh_http_cookies,
    fetch_page,
    keepalive_session,
    pre_authenticate_all_states,
    headed_reauth,
    apply_cookies_to_browser,
    play_human_verification_alert,
    download_zip_via_naic_portal,
    is_interstate_insurance_compact_page,
)

# Throttle settings — longer delays to avoid IP blocks
MIN_DELAY_BETWEEN_ROWS = 0.5  # minimum seconds between requests
MAX_DELAY_BETWEEN_ROWS = 2  # maximum seconds between requests
# Every N rows, take an extra long break to look less bot-like
LONG_PAUSE_EVERY = 200
LONG_PAUSE_MIN = 5
LONG_PAUSE_MAX = 20
# Keepalive ping every N rows to prevent session expiry
KEEPALIVE_EVERY = 12

# Shared counter for total-progress tracking across worker processes
_shared_counter = None
# Shared set of states whose authentication has permanently failed —
# other workers skip chunks for these states immediately.
_failed_states = None
# Pre-authed cookies dict: {state_name: {cookie_name: cookie_value}}
# Set by main process before pool creation; None means workers self-auth.
_preauth_cookies = None
# Whether ZIP download/readability extraction is enabled for this run
_with_zip = False

# Per-row wall-clock limit (worker processes only). Set in _init_worker. 0 = disabled.
_ROW_TIMEOUT_SEC = 0


class RowTimeout(Exception):
    """Raised when ITIMER_REAL fires for a row (Unix/macOS only)."""


def _row_timer_handler(signum, frame):
    raise RowTimeout()


def _row_timer_arm():
    sec = _ROW_TIMEOUT_SEC
    if not sec or sec <= 0:
        return
    if not hasattr(signal, "SIGALRM") or not hasattr(signal, "setitimer"):
        return
    signal.signal(signal.SIGALRM, _row_timer_handler)
    signal.setitimer(signal.ITIMER_REAL, float(sec), 0.0)


def _row_timer_disarm():
    if not _ROW_TIMEOUT_SEC or _ROW_TIMEOUT_SEC <= 0:
        return
    if not hasattr(signal, "setitimer"):
        return
    signal.setitimer(signal.ITIMER_REAL, 0.0, 0.0)


def _apply_row_timeout_skip(df_state, idx, row_tuple, url):
    """Record timeout skip so the run advances; row can be retried on a later run."""
    tracking_number = None
    for col in (
        "SERFF Tracking Number",
        "serf_num",
        "serff_tracking_number",
        "tracking_number",
    ):
        if col in df_state.columns:
            tracking_number = df_state.at[idx, col]
            break
    if tracking_number is None:
        tracking_number = getattr(row_tuple, "serf_num", None)
    if not pd.isna(tracking_number) and tracking_number is not None:
        tracking_number = str(tracking_number).strip()
    url_s = str(url)
    tail = f"{url_s[:80]}…" if len(url_s) > 80 else url_s
    tprint(
        f"[TIMEOUT] PID {os.getpid()} row {idx} tracking={tracking_number!r} "
        f"exceeded {_ROW_TIMEOUT_SEC}s — skipping ({tail})"
    )
    if _with_zip:
        df_state.at[idx, "zip_status"] = "skipped_timeout"
        df_state.at[idx, "zip_size"] = pd.NA
        df_state.at[idx, "zip_attempts"] = 0
        df_state.at[idx, "zip_error"] = f"row wall-clock timeout ({_ROW_TIMEOUT_SEC}s)"


# Log a heartbeat line from the overall progress monitor every N seconds (0 to disable)
_PROGRESS_HEARTBEAT_SEC = 300


def tprint(*args, **kwargs):
    """Print with a local wall-clock timestamp prefix on every line."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]", *args, **kwargs)


def _init_worker(
    counter,
    failed_states,
    reauth_lock,
    preauth_cookies=None,
    headed=True,
    with_zip=False,
    reauth_lock_timeout_sec=600,
    row_timeout_sec=0,
):
    """Pool initializer: store shared objects in each worker."""
    global _shared_counter, _failed_states, _preauth_cookies, _with_zip, _ROW_TIMEOUT_SEC
    _shared_counter = counter
    _failed_states = failed_states
    _preauth_cookies = preauth_cookies
    _with_zip = with_zip
    _ROW_TIMEOUT_SEC = int(row_timeout_sec) if row_timeout_sec else 0
    set_reauth_lock(reauth_lock)
    set_reauth_lock_timeout(reauth_lock_timeout_sec)
    set_headed_mode(headed)


def _monitor_progress(counter, total, stop_event):
    """Monitor thread that displays total progress across all states."""
    pbar = tqdm(total=total, desc="Overall", position=0, leave=True, colour="green")
    completed = 0
    last_heartbeat = time.monotonic()
    while not stop_event.is_set():
        new_val = counter.value
        if new_val > completed:
            pbar.update(new_val - completed)
            completed = new_val
        if _PROGRESS_HEARTBEAT_SEC > 0:
            now = time.monotonic()
            if now - last_heartbeat >= _PROGRESS_HEARTBEAT_SEC:
                tqdm.write(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"[HEARTBEAT] Overall {new_val}/{total} rows completed"
                )
                last_heartbeat = now
        stop_event.wait(1.0)
    # Final update
    new_val = counter.value
    if new_val > completed:
        pbar.update(new_val - completed)
    pbar.close()


def _parse_file_name_column(raw):
    """
    Parse the file_name column (CSV) into a list of filename strings.
    Handles Python list literal strings (e.g. "['a.pdf', 'b.pdf']") via ast.literal_eval.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw]
    s = str(raw).strip()
    if not s or s.lower() == "nan":
        return []
    if s.startswith("["):
        try:
            out = ast.literal_eval(s)
            return [str(x).strip() for x in out] if isinstance(out, list) else [s]
        except (ValueError, SyntaxError):
            return [s]
    return [s]


def _get_form_name_for_file(f, mapping):
    """
    Get form_name for expected filename f from scraped mapping.
    Tries exact match first, then case-insensitive (e.g. .PDF in CSV vs .pdf on page).
    Returns form_name string or None if not found.
    """
    if f in mapping and mapping[f]:
        return mapping[f][0].get("form_name") or ""
    key_lower = f.lower()
    for k, entries in mapping.items():
        if k.lower() == key_lower and entries:
            return entries[0].get("form_name") or ""
    return None


def has_alpha_filing_id(url):
    """
    Check if a SERFF URL has an alphanumeric filing ID (contains letters).
    Numeric-only IDs can be navigated directly; alphanumeric ones need search.
    e.g. filingId=6LJMVY298/00 → True, filingId=132569558 → False
    """
    match = re.search(r"filingId=([^&]+)", url)
    if not match:
        return False
    filing_id = match.group(1)
    return bool(re.search(r"[A-Za-z]", filing_id))


# States to skip entirely (no work units created).
SKIP_STATES = {"WA", "NM", "PA", "MI"}

# Only these states continue to the next row on auth/re-auth failure (full run, skip rows only).
# All other states are marked as failed and the whole state is skipped on first failure.
STATES_CONTINUE_ON_FAILURE = {"ME"}


def build_work_units(df):
    """
    Group rows by state — one work unit per state (no chunking).
    Within each state, numeric filing IDs come first (HTTP-only, fast)
    and alpha IDs last (need Playwright search, slower).
    Returns list of (work_label, state_name, df_for_state) tuples,
    sorted largest-first so big states start processing early.
    """
    units = []
    for state_name, state_df in df.groupby("state"):
        if state_name in SKIP_STATES:
            continue
        state_df = state_df.copy()
        state_df["_is_alpha"] = state_df["page_url"].apply(has_alpha_filing_id)
        state_df = (
            state_df.sort_values("_is_alpha", kind="stable")
            .drop(columns="_is_alpha")
            .reset_index(drop=True)
        )
        units.append((state_name, state_name, state_df))
    units.sort(key=lambda x: len(x[2]), reverse=True)
    return units


def _list_existing_output_files(output_dir):
    import glob

    return sorted(
        glob.glob(os.path.join(output_dir, "state_results_*.csv"))
        + glob.glob(os.path.join(output_dir, "temp_results_*.csv"))
        + glob.glob(os.path.join(output_dir, "backfilled_zip_*.csv"))
        + glob.glob(os.path.join(output_dir, "*backfilled*zip*.csv"))
    )


def load_failed_zip_ids(output_dir="outputs"):
    """
    Return tracking numbers that were previously attempted and failed ZIP download.
    Only rows with zip_status == failed are included.
    """
    match_col = "SERFF Tracking Number"
    failed_ids = set()
    existing_files = _list_existing_output_files(output_dir)

    if not existing_files:
        return failed_ids

    for f in existing_files:
        try:
            tmp_df = pd.read_csv(f)
            if match_col not in tmp_df.columns or "zip_status" not in tmp_df.columns:
                continue

            tmp_df[match_col] = tmp_df[match_col].astype(str)
            is_failed = tmp_df["zip_status"].astype(str).str.lower() == "failed"
            if is_failed.any():
                failed_ids.update(tmp_df.loc[is_failed, match_col].unique())
        except Exception as e:
            tprint(f"[RESUME][WARN] Could not read {f}: {str(e).splitlines()[0]}")

    return failed_ids


PANEL_IDS = {
    "forms": "summaryForm:formAttachmentPanel_content",
    "rate_rule": "summaryForm:rateRuleAttachmentPanel_content",
    "supporting_documentation": "summaryForm:supportingDocumentAttachmentPanel_content",
    "correspondence": "summaryForm:correspondenceAttachmentPanel_content",
}

# Normalized header names we care about → canonical keys
_HEADER_ALIASES = {
    "form name": "form_name",
    "form number": "form_number",
    "document name": "form_name",
    "attachments": "attachments",
}

# Base directory for ZIP downloads when running via Playwright
ZIP_BASE_DIR = "/Users/favasm/Library/CloudStorage/GoogleDrive-favasm@greenberryai.com/Shared drives/MJ/scraping/downloads"

# SERFF ZIP: wait_for visible on #downloadLink, then expect_download after click (per attempt, ms).
_ZIP_DOWNLOAD_LINK_WAIT_MS = (20_000, 30_000)
_ZIP_EXPECT_DOWNLOAD_TIMEOUT_MS = (180_000, 300_000)  # 3 min, then 5 min on retry

# Mapping of filing metadata labels → canonical column keys
_FILING_META_LABELS = {
    "product name": "product_name",
    "type of insurance": "type_of_insurance",
    "sub type of insurance": "sub_type_of_insurance",
    "filing type": "filing_type",
    "serff tracking number": "serff_tracking_number_html",
    "submission date": "submission_date",
    "filing status": "filing_status",
    "serff status": "serff_status",
    "disposition date": "disposition_date",
    "disposition status": "disposition_status",
    "state status": "state_status",
    "state status last changed": "state_status_last_changed",
}


def _parse_panel_rows_bs(soup, panel_id, section_name):
    """
    Parse attachment rows from a panel using BeautifulSoup.
    Works with HTML strings fetched via requests (no browser needed).
    """
    results = []
    panel = soup.find(id=panel_id)
    if not panel:
        return results

    if panel.get_text(strip=True) == "None Available":
        return results

    headers = panel.find_all("div", class_="summaryScheduleItemHeader")
    if not headers:
        return results

    col_map = {}
    for i, h in enumerate(headers):
        raw = h.get_text(strip=True).lower()
        canonical = _HEADER_ALIASES.get(raw)
        if canonical:
            col_map[i] = canonical

    if "attachments" not in col_map.values():
        return results

    output_panel = panel.find("div", class_="ui-outputpanel")
    if not output_panel:
        output_panel = panel

    all_rows = output_panel.find_all("div", class_="row", recursive=False)

    for row_el in all_rows:
        cells = row_el.find_all(
            "div", class_="summaryScheduleItemData", recursive=False
        )
        if not cells:
            continue

        row_data = {}
        attachment_cell = None
        for c, cell in enumerate(cells):
            canonical = col_map.get(c)
            if canonical == "attachments":
                attachment_cell = cell
            elif canonical:
                row_data[canonical] = cell.get_text(strip=True)

        if attachment_cell is None:
            continue

        links = attachment_cell.find_all("a")
        if not links:
            continue

        form_name = row_data.get("form_name", "")
        form_number = row_data.get("form_number")

        for link in links:
            filename = link.get_text(strip=True)
            if filename:
                results.append(
                    {
                        "filename": filename,
                        "form_name": form_name,
                        "form_number": form_number,
                        "section": section_name,
                    }
                )

    return results


def scrape_attachment_mappings_html(html):
    """
    Parse a filing summary page's HTML to build a mapping of
    PDF filename → {form_name, form_number, section}.

    Uses BeautifulSoup — works with HTML fetched via requests or Playwright.
    Returns dict keyed by filename, or empty dict on failure.
    """
    soup = BeautifulSoup(html, "html.parser")

    if not soup.find(id="attachmentsContainer"):
        return {}

    mapping = {}
    for section_name, panel_id in PANEL_IDS.items():
        try:
            rows = _parse_panel_rows_bs(soup, panel_id, section_name)
            for entry in rows:
                fname = entry.pop("filename")
                if fname not in mapping:
                    mapping[fname] = []
                mapping[fname].append(entry)
        except Exception as e:
            tprint(
                f"[WARN] PID {os.getpid()}: Failed to parse {section_name} panel: "
                f"{str(e).splitlines()[0]}"
            )

    return mapping


def parse_filing_metadata_html(html):
    """
    Parse the top-of-page Filing Information / Filing Outcome / Company Information
    block into a flat dict of metadata fields.
    """
    soup = BeautifulSoup(html, "html.parser")
    meta = {}

    root_panel = soup.find("div", id="j_idt28") or soup.find(
        "h2", string=lambda s: s and "Filing Information" in s
    )
    if root_panel and root_panel.name == "h2":
        root_panel = root_panel.find_parent("div", class_="row")

    # Filing Information / Outcome labels + values
    if root_panel:
        for row in root_panel.find_all("div", class_="row"):
            label_el = row.find("label", class_="col-sm-5")
            value_el = row.find("div", class_="col-sm-7")
            if not label_el or not value_el:
                continue
            label_text = label_el.get_text(strip=True).rstrip(":").lower()
            key = _FILING_META_LABELS.get(label_text)
            if not key:
                continue
            value_text = value_el.get_text(separator=" ", strip=True)
            meta[key] = value_text

    # Company Information (single row under the header)
    company_header = soup.find("h2", string=lambda s: s and "Company Information" in s)
    if company_header:
        row = company_header.find_next("div", class_="row")
        # Next sibling row with spans holding the values
        data_row = row.find_next_sibling("div", class_="row") if row else None
        if data_row:
            spans = data_row.find_all("span", class_="col-sm-3")
            code_span = data_row.find("span", class_="col-sm-2")
            addr_span = data_row.find("span", class_="col-sm-4")
            phone_span = data_row.find("span", class_="col-sm-3 text-center")

            if spans:
                meta["company_name"] = spans[0].get_text(strip=True)
            if code_span:
                meta["company_code"] = code_span.get_text(strip=True)
            if addr_span:
                addr_text = addr_span.get_text(separator=" ", strip=True)
                meta["company_address"] = addr_text
            if phone_span:
                meta["company_phone"] = phone_span.get_text(strip=True)

    return meta


def _ensure_browser(pw, browser, context, page, state_name):
    """
    Lazily launch a browser for alpha-ID search (headed or headless per global
    headed mode). If pre-authed cookies exist, loads them into the context.
    Re-uses existing browser if already running.
    Returns (pw, browser, context, page).
    """
    if browser is not None:
        return pw, browser, context, page
    from playwright.sync_api import sync_playwright
    from browser_utils import _launch_browser, _new_stealth_context

    pw = sync_playwright().start()
    # Use global headed mode (set in worker init) so laptop runs can be visible
    browser = _launch_browser(pw, headed=None)
    tprint(
        f"[BROWSER] PID {os.getpid()} launching Firefox for alpha-ID search ({state_name})..."
    )
    context = _new_stealth_context(browser)
    page = context.new_page()

    if _preauth_cookies is not None and state_name in _preauth_cookies:
        context.add_cookies(_preauth_cookies[state_name])
        tprint(
            f"[BROWSER] PID {os.getpid()} loaded pre-authed cookies for {state_name}"
        )
    else:
        authenticate(page, state_name)

    return pw, browser, context, page


def _reauth_and_refresh(browser, context, page, state_name, http_session):
    """
    Re-authenticate with a fresh context and update the HTTP session cookies.
    Closes old context first so the server-side session slot is freed.
    Returns (context, page, http_session, auth_ok).
    """
    context, page, auth_ok = new_context_and_reauth(
        browser, state_name, old_context=context
    )
    if auth_ok and http_session is not None:
        refresh_http_cookies(context, http_session)
    return context, page, http_session, auth_ok


def _zip_output_paths(base_download_dir, state_name, tracking_number):
    """Return (download_root, zip_file_path) for a row."""
    download_root = os.path.join(base_download_dir, state_name, str(tracking_number))
    zip_file_path = os.path.join(download_root, "attachments.zip")
    return download_root, zip_file_path


def _is_valid_zip_file(zip_file_path):
    """A ZIP is considered valid for resume/skip if file exists and size > 0."""
    try:
        return os.path.exists(zip_file_path) and os.path.getsize(zip_file_path) > 0
    except OSError:
        return False


def download_and_process_zip(
    page,
    state_name,
    tracking_number,
    base_download_dir,
    ignore_folders,
    folder_to_column,
):
    """
    Using an already-authenticated Playwright page on a filing summary,
    click the attachment selection buttons and download the ZIP for this filing
    into a state/serff_number folder. It does NOT extract or parse the ZIP.

    Returns a dict with ZIP attempt outcome metadata.
    """
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    if not tracking_number:
        return {
            "zip_status": "skipped_no_tracking",
            "zip_size": pd.NA,
            "zip_attempts": 0,
            "zip_error": "missing tracking number",
        }

    download_root, zip_file_path = _zip_output_paths(
        base_download_dir, state_name, tracking_number
    )
    os.makedirs(download_root, exist_ok=True)

    if _is_valid_zip_file(zip_file_path):
        return {
            "zip_status": "skipped_existing",
            "zip_size": int(os.path.getsize(zip_file_path)),
            "zip_attempts": 0,
            "zip_error": pd.NA,
        }

    try:
        _compact_html = page.content()
    except Exception:
        _compact_html = ""
    if is_interstate_insurance_compact_page(_compact_html):
        os.makedirs(download_root, exist_ok=True)
        naic_path = download_zip_via_naic_portal(
            page, str(tracking_number), zip_file_path
        )
        if naic_path and _is_valid_zip_file(zip_file_path):
            tprint(
                f"[ZIP][NAIC] PID {os.getpid()} {state_name} {tracking_number}: "
                f"saved {zip_file_path}"
            )
            return {
                "zip_status": "success",
                "zip_size": int(os.path.getsize(zip_file_path)),
                "zip_attempts": 1,
                "zip_error": pd.NA,
            }
        err_msg = (
            "naic portal zip failed"
            if not naic_path
            else "naic download empty or invalid"
        )
        tprint(
            f"[ZIP][NAIC] PID {os.getpid()} {state_name} {tracking_number}: {err_msg}"
        )
        return {
            "zip_status": "failed",
            "zip_size": (
                int(os.path.getsize(zip_file_path))
                if os.path.exists(zip_file_path)
                else pd.NA
            ),
            "zip_attempts": 1,
            "zip_error": err_msg,
        }

    select_all_buttons = [
        "formAttachmentSelectAllButton",
        "rateRuleAttachmentSelectAllButton",
        "supportingDocumentAttachmentSelectAllButton",
    ]

    def _select_all_attachments():
        # Click "Select All" buttons where present so every version is included
        for btn_id in select_all_buttons:
            try:
                page.locator(f"#{btn_id}").click(timeout=2_000)
                time.sleep(0.2)
            except PlaywrightTimeoutError:
                pass
            except Exception:
                pass

        # Correspondence has "Select All" instead
        try:
            page.locator("#correspondenceAttachmentSelectAllButton").click(
                timeout=2_000
            )
            time.sleep(0.2)
        except PlaywrightTimeoutError:
            pass
        except Exception:
            pass

    last_error = pd.NA
    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        if _is_valid_zip_file(zip_file_path):
            return {
                "zip_status": "skipped_existing",
                "zip_size": int(os.path.getsize(zip_file_path)),
                "zip_attempts": attempt - 1,
                "zip_error": pd.NA,
            }

        try:
            # Ensure download UI is ready before attempting click.
            link_wait_ms = _ZIP_DOWNLOAD_LINK_WAIT_MS[
                min(attempt - 1, len(_ZIP_DOWNLOAD_LINK_WAIT_MS) - 1)
            ]
            expect_dl_ms = _ZIP_EXPECT_DOWNLOAD_TIMEOUT_MS[
                min(attempt - 1, len(_ZIP_EXPECT_DOWNLOAD_TIMEOUT_MS) - 1)
            ]
            page.locator("[id='summaryForm:downloadLink']").wait_for(
                state="visible", timeout=link_wait_ms
            )
            _select_all_attachments()
            with page.expect_download(timeout=expect_dl_ms) as download_info:
                page.locator("[id='summaryForm:downloadLink']").click()
            download = download_info.value
            temp_zip_path = f"{zip_file_path}.part"
            if os.path.exists(temp_zip_path):
                try:
                    os.remove(temp_zip_path)
                except OSError:
                    pass
            download.save_as(temp_zip_path)
            os.replace(temp_zip_path, zip_file_path)
            if _is_valid_zip_file(zip_file_path):
                tprint(
                    f"[ZIP] PID {os.getpid()} {state_name} {tracking_number}: "
                    f"saved {zip_file_path}"
                )
                return {
                    "zip_status": "success",
                    "zip_size": int(os.path.getsize(zip_file_path)),
                    "zip_attempts": attempt,
                    "zip_error": pd.NA,
                }
            last_error = "download completed but zip is empty"
        except PlaywrightTimeoutError:
            last_error = "download timeout"
        except Exception as e:
            last_error = str(e).splitlines()[0]

        tprint(
            f"[ZIP] PID {os.getpid()} {state_name} {tracking_number}: "
            f"attempt {attempt}/{max_attempts} failed: {last_error} "
            f"(target {zip_file_path})"
        )
        if attempt < max_attempts:
            # Controlled retry: refresh and retry with longer download wait.
            try:
                reload_ms = 45_000 if attempt == 1 else 30_000
                page.reload(wait_until="networkidle", timeout=reload_ms)
            except Exception:
                pass
            time.sleep(2.5 if attempt == 1 else 1.5)

    return {
        "zip_status": "failed",
        "zip_size": (
            int(os.path.getsize(zip_file_path))
            if os.path.exists(zip_file_path)
            else pd.NA
        ),
        "zip_attempts": max_attempts,
        "zip_error": last_error,
    }


def process_state(state_data):
    """
    Process all rows for one state.

    If pre-authed cookies are available (_preauth_cookies), the worker
    skips Playwright auth entirely and uses requests.Session for 94% of
    rows.  A headless browser is only launched on-demand when an alpha
    filing ID requires search navigation (6% of rows).

    Without pre-auth, falls back to the original browser-first flow.
    """
    work_label, state_name, df_state = state_data

    if _failed_states is not None and state_name in _failed_states:
        tprint(
            f"[SKIP] {work_label}: state {state_name} already marked as failed, skipping"
        )
        return None

    df_state = df_state.copy()
    df_state["form_name_mapping"] = None
    if "search_result_url" not in df_state.columns:
        df_state["search_result_url"] = pd.NA
    if "zip_status" not in df_state.columns:
        df_state["zip_status"] = pd.NA
    if "zip_size" not in df_state.columns:
        df_state["zip_size"] = pd.NA
    if "zip_attempts" not in df_state.columns:
        df_state["zip_attempts"] = 0
    if "zip_error" not in df_state.columns:
        df_state["zip_error"] = pd.NA

    # Stagger worker startups
    time.sleep(random.uniform(1, 6))

    # --- Bootstrap HTTP session -----------------------------------------------
    pw = browser = context = page = None
    http_session = None

    has_preauth = _preauth_cookies is not None and state_name in _preauth_cookies

    if has_preauth:
        http_session = create_http_session_from_cookies(_preauth_cookies[state_name])
        tprint(f"[WORKER] PID {os.getpid()} using pre-authed cookies for {state_name}")
    else:
        pw, browser, context, page = create_browser()
        auth_ok = authenticate(page, state_name)
        if not auth_ok:
            context, page, auth_ok = new_context_and_reauth(
                browser, state_name, old_context=context
            )
        if not auth_ok:
            tprint(f"[ERROR] Auth failed for {work_label}")
            if (
                state_name not in STATES_CONTINUE_ON_FAILURE
                and _failed_states is not None
            ):
                _failed_states[state_name] = True
            return None
        http_session = create_http_session(context)

    # --- Process rows ---------------------------------------------------------
    rows_since_keepalive = 0

    try:
        for row in tqdm(
            df_state.itertuples(),
            total=len(df_state),
            desc=f"{work_label} ({len(df_state)} URLs, PID {os.getpid()})",
        ):
            idx = row.Index
            url = row.page_url

            _row_timer_arm()
            try:
                # Use an explicit column lookup for stable folder naming.
                # Positional tuple fields (e.g. "_1") can shift when column order changes.
                tracking_number = None
                for col in (
                    "SERFF Tracking Number",
                    "serf_num",
                    "serff_tracking_number",
                    "tracking_number",
                ):
                    if col in df_state.columns:
                        tracking_number = df_state.at[idx, col]
                        break
                if tracking_number is None:
                    tracking_number = getattr(row, "serf_num", None)

                if not pd.isna(tracking_number):
                    tracking_number = str(tracking_number).strip()
                is_alpha = has_alpha_filing_id(url)
                html = None

                # ---- Path 1: Direct URL via requests (no browser) ----
                if not is_alpha:
                    page_html, expired, srv_err = fetch_page(http_session, url)

                    if srv_err:
                        tprint(
                            f"[500] PID {os.getpid()} row {idx}: Server error via HTTP for {url}"
                        )
                        time.sleep(random.uniform(15, 30))
                        page_html, expired, srv_err = fetch_page(http_session, url)

                    if expired and not srv_err:
                        tprint(
                            f"[HTTP] PID {os.getpid()} row {idx}: Session expired, re-authenticating..."
                        )
                        pw, browser, context, page = _ensure_browser(
                            pw, browser, context, page, state_name
                        )
                        context, page, http_session, auth_ok = _reauth_and_refresh(
                            browser, context, page, state_name, http_session
                        )
                        if auth_ok:
                            page_html, expired, srv_err = fetch_page(http_session, url)
                        else:
                            tprint(
                                f"[ERROR] PID {os.getpid()} Re-auth failed for {state_name}"
                            )
                            if (
                                state_name not in STATES_CONTINUE_ON_FAILURE
                                and _failed_states is not None
                            ):
                                _failed_states[state_name] = True
                                return None
                            continue

                    if page_html and not expired and not srv_err:
                        if "attachmentsContainer" in page_html:
                            html = page_html
                        elif "filingSummary" in url:
                            # Response missing attachmentsContainer (e.g. login/error page) — re-auth and retry once
                            tprint(
                                f"[HTTP] PID {os.getpid()} row {idx}: No attachmentsContainer in response, re-authenticating..."
                            )
                            if page_html:
                                tprint(f"[HTTP] First 300 chars: {page_html[:300]!r}")
                            is_human_verification = (
                                page_html and "Human Verification" in page_html
                            )
                            if is_human_verification:
                                play_human_verification_alert()
                                tprint(
                                    "[HTTP] Human Verification detected — opening headed browser; please complete verification in the visible window."
                                )
                            pw, browser, context, page = _ensure_browser(
                                pw, browser, context, page, state_name
                            )
                            if is_human_verification:
                                cookies = headed_reauth(state_name)
                                if cookies:
                                    context, page = apply_cookies_to_browser(
                                        browser, cookies, old_context=context
                                    )
                                    refresh_http_cookies(context, http_session)
                                    auth_ok = True
                                else:
                                    auth_ok = False
                            else:
                                context, page, http_session, auth_ok = (
                                    _reauth_and_refresh(
                                        browser, context, page, state_name, http_session
                                    )
                                )
                            if auth_ok:
                                page_html, expired, srv_err = fetch_page(
                                    http_session, url
                                )
                                if (
                                    page_html
                                    and not expired
                                    and not srv_err
                                    and "attachmentsContainer" in page_html
                                ):
                                    html = page_html
                            else:
                                tprint(
                                    f"[ERROR] PID {os.getpid()} Re-auth failed for {state_name}"
                                )
                                if (
                                    state_name not in STATES_CONTINUE_ON_FAILURE
                                    and _failed_states is not None
                                ):
                                    _failed_states[state_name] = True
                                    return None
                                continue

                # ---- Path 2: Alpha IDs or failed direct fetch -> Playwright search ----
                if html is None:
                    if not tracking_number or (
                        isinstance(tracking_number, float) and pd.isna(tracking_number)
                    ):
                        tprint(
                            f"[SKIP] PID {os.getpid()} row {idx}: No tracking number for {url}"
                        )
                        continue

                    pw, browser, context, page = _ensure_browser(
                        pw, browser, context, page, state_name
                    )

                    for search_attempt in range(3):
                        if search_attempt > 0:
                            wait = 15 * search_attempt
                            tprint(
                                f"[WARN] PID {os.getpid()} row {idx}: Search attempt {search_attempt+1}/3 for {tracking_number}, waiting {wait}s..."
                            )
                            time.sleep(wait)
                            context, page, http_session, auth_ok = _reauth_and_refresh(
                                browser, context, page, state_name, http_session
                            )
                            if not auth_ok:
                                tprint(
                                    f"[ERROR] PID {os.getpid()} Re-auth failed for {state_name}"
                                )
                                if (
                                    state_name not in STATES_CONTINUE_ON_FAILURE
                                    and _failed_states is not None
                                ):
                                    _failed_states[state_name] = True
                                    return None
                                break  # skip this row, fall through to "if html is None" then continue

                        if navigate_via_search(
                            page, context, tracking_number, state_name
                        ):
                            if not is_server_error(page):
                                df_state.at[idx, "search_result_url"] = page.url
                                html = page.content()
                                break
                            tprint(
                                f"[500] PID {os.getpid()} row {idx}: 500 after search for {tracking_number}"
                            )
                            time.sleep(random.uniform(15, 30))
                        elif is_server_error(page):
                            tprint(
                                f"[500] PID {os.getpid()} row {idx}: 500 during search for {tracking_number}"
                            )
                            time.sleep(random.uniform(15, 30))

                    if html is None:
                        tprint(
                            f"[SKIP] PID {os.getpid()} row {idx}: All nav failed for {tracking_number}"
                        )
                        continue

                # ---- Parse with BeautifulSoup ----
                mapping = scrape_attachment_mappings_html(html)
                meta = parse_filing_metadata_html(html)
                # Restrict to this row's file_name list and build file -> form_name (for these files only)
                expected_files = []
                if "file_name" in df_state.columns:
                    expected_files = _parse_file_name_column(
                        df_state.at[idx, "file_name"]
                    )
                if expected_files:
                    # Subset: only store form names for the files we care about (exact + case-insensitive match)
                    subset = {
                        f: _get_form_name_for_file(f, mapping) for f in expected_files
                    }
                    any_found = any(subset.get(f) is not None for f in expected_files)
                    if not any_found:
                        # One retry: re-auth and re-fetch (whole-run often gets bad responses)
                        tprint(
                            f"[RETRY] PID {os.getpid()} row {idx}: No match, re-auth and re-fetch once..."
                        )
                        pw, browser, context, page = _ensure_browser(
                            pw, browser, context, page, state_name
                        )
                        context, page, http_session, _ = _reauth_and_refresh(
                            browser, context, page, state_name, http_session
                        )
                        if not is_alpha:
                            page_html, expired, srv_err = fetch_page(http_session, url)
                            if (
                                page_html
                                and not expired
                                and not srv_err
                                and "attachmentsContainer" in page_html
                            ):
                                mapping = scrape_attachment_mappings_html(page_html)
                                subset = {
                                    f: _get_form_name_for_file(f, mapping)
                                    for f in expected_files
                                }
                                any_found = any(
                                    subset.get(f) is not None for f in expected_files
                                )
                        else:
                            if navigate_via_search(
                                page, context, tracking_number, state_name
                            ) and not is_server_error(page):
                                df_state.at[idx, "search_result_url"] = page.url
                                mapping = scrape_attachment_mappings_html(
                                    page.content()
                                )
                                subset = {
                                    f: _get_form_name_for_file(f, mapping)
                                    for f in expected_files
                                }
                                any_found = any(
                                    subset.get(f) is not None for f in expected_files
                                )
                    if any_found:
                        df_state.at[idx, "form_name_mapping"] = str(subset)
                    else:
                        # No files found for this row -> NA so it will be rerun (not considered done)
                        page_loaded = html is not None and "Submission Date:" in html
                        tprint(
                            f"[NO_MATCH] PID {os.getpid()} row {idx} {tracking_number}: "
                            f"expected {len(expected_files)} file(s), 0 matched "
                            f"(page_loaded={page_loaded})"
                        )
                        df_state.at[idx, "form_name_mapping"] = pd.NA
                else:
                    # No file_name list: keep full page mapping (backward compat)
                    df_state.at[idx, "form_name_mapping"] = (
                        str(mapping) if mapping else pd.NA
                    )

                # ---- Filing-level metadata into columns ----
                for m_key, m_val in meta.items():
                    if m_key not in df_state.columns:
                        df_state[m_key] = pd.NA
                    df_state.at[idx, m_key] = m_val

                # ---- Optional ZIP download (no parsing, just save ZIP) ----
                if _with_zip:
                    try:
                        if not tracking_number or (
                            isinstance(tracking_number, float)
                            and pd.isna(tracking_number)
                        ):
                            df_state.at[idx, "zip_status"] = "skipped_no_tracking"
                            df_state.at[idx, "zip_size"] = pd.NA
                            df_state.at[idx, "zip_attempts"] = 0
                            df_state.at[idx, "zip_error"] = "missing tracking number"
                            continue

                        _, zip_file_path = _zip_output_paths(
                            ZIP_BASE_DIR, state_name, tracking_number
                        )
                        if _is_valid_zip_file(zip_file_path):
                            df_state.at[idx, "zip_status"] = "skipped_existing"
                            df_state.at[idx, "zip_size"] = int(
                                os.path.getsize(zip_file_path)
                            )
                            df_state.at[idx, "zip_attempts"] = 0
                            df_state.at[idx, "zip_error"] = pd.NA
                            continue

                        pw, browser, context, page = _ensure_browser(
                            pw, browser, context, page, state_name
                        )
                        try:
                            page.goto(url, wait_until="networkidle", timeout=60_000)
                        except Exception as e:
                            tprint(
                                f"[ZIP] PID {os.getpid()} row {idx} {tracking_number}: "
                                f"goto failed: {str(e).splitlines()[0]}"
                            )
                            df_state.at[idx, "zip_status"] = "failed"
                            df_state.at[idx, "zip_size"] = (
                                int(os.path.getsize(zip_file_path))
                                if os.path.exists(zip_file_path)
                                else pd.NA
                            )
                            df_state.at[idx, "zip_attempts"] = 0
                            df_state.at[idx, "zip_error"] = (
                                f"goto failed: {str(e).splitlines()[0]}"
                            )
                        else:
                            zip_info = download_and_process_zip(
                                page,
                                state_name,
                                tracking_number,
                                ZIP_BASE_DIR,
                                {},
                                {},
                            )
                            df_state.at[idx, "zip_status"] = zip_info.get(
                                "zip_status", pd.NA
                            )
                            df_state.at[idx, "zip_size"] = zip_info.get(
                                "zip_size", pd.NA
                            )
                            df_state.at[idx, "zip_attempts"] = zip_info.get(
                                "zip_attempts", 0
                            )
                            df_state.at[idx, "zip_error"] = zip_info.get(
                                "zip_error", pd.NA
                            )
                    except Exception as e:
                        tprint(
                            f"[ZIP] PID {os.getpid()} row {idx} {tracking_number}: "
                            f"unexpected error: {str(e).splitlines()[0]}"
                        )
                        df_state.at[idx, "zip_status"] = "failed"
                        df_state.at[idx, "zip_size"] = pd.NA
                        df_state.at[idx, "zip_attempts"] = max(
                            (
                                int(df_state.at[idx, "zip_attempts"])
                                if pd.notna(df_state.at[idx, "zip_attempts"])
                                else 0
                            ),
                            0,
                        )
                        df_state.at[idx, "zip_error"] = (
                            f"unexpected error: {str(e).splitlines()[0]}"
                        )

            except RowTimeout:
                _apply_row_timeout_skip(df_state, idx, row, url)
            except Exception as e:
                serf = getattr(row, "serf_num", "unknown")
                tprint(
                    f"[ERROR] PID {os.getpid()} row {idx} (serf_num={serf}): {str(e).splitlines()[0]}"
                )
                if browser is not None:
                    try:
                        context, page, http_session, _ = _reauth_and_refresh(
                            browser, context, page, state_name, http_session
                        )
                    except Exception:
                        pass
                continue
            finally:
                _row_timer_disarm()
                if _shared_counter is not None:
                    with _shared_counter.get_lock():
                        _shared_counter.value += 1

                rows_since_keepalive += 1

                if rows_since_keepalive >= KEEPALIVE_EVERY:
                    keepalive_session(http_session)
                    rows_since_keepalive = 0

                delay = random.uniform(MIN_DELAY_BETWEEN_ROWS, MAX_DELAY_BETWEEN_ROWS)
                time.sleep(delay)

                if idx > 0 and idx % LONG_PAUSE_EVERY == 0:
                    pause = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
                    tprint(
                        f"[THROTTLE] PID {os.getpid()} taking {pause:.0f}s breather after {idx} rows..."
                    )
                    time.sleep(pause)

            if idx % 10 == 0 and idx > 0:
                checkpoint_file = f"outputs/temp_results_{work_label}_{os.getpid()}.csv"
                if _with_zip:
                    is_zip_success = (
                        df_state["zip_status"].astype(str).str.lower() == "success"
                    )
                    df_state.loc[is_zip_success].to_csv(checkpoint_file, index=False)
                else:
                    with_mapping = df_state["form_name_mapping"].notna()
                    df_state.loc[with_mapping].to_csv(checkpoint_file, index=False)
    finally:
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
        if pw is not None:
            try:
                pw.stop()
            except Exception:
                pass

    safe_label = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in work_label
    )
    result_file = f"outputs/state_results_{safe_label}_{os.getpid()}.csv"
    df_state.to_csv(result_file, index=False)
    tprint(f"  ✓ Saved: {result_file} ({len(df_state)} rows)")
    return result_file


def load_already_processed(output_dir="outputs", with_zip=False):
    """
    Scan existing result CSVs and return a set of SERFF Tracking Numbers
    that have already been fully processed.
    In ZIP mode, "done" means zip_status == success.
    In non-ZIP mode, "done" means non-null form_name_mapping.
    Also returns a list of the DataFrames so they can be included in the final merge.
    """
    MATCH_COL = "SERFF Tracking Number"
    processed_ids = set()
    processed_dfs = []

    existing_files = _list_existing_output_files(output_dir)

    if not existing_files:
        return processed_ids, processed_dfs

    tprint(
        f"[RESUME] Found {len(existing_files)} existing result files, scanning for already-processed rows..."
    )

    for f in existing_files:
        try:
            tmp_df = pd.read_csv(f)
            if MATCH_COL not in tmp_df.columns:
                continue

            tmp_df[MATCH_COL] = tmp_df[MATCH_COL].astype(str)

            if with_zip:
                if "zip_status" not in tmp_df.columns:
                    continue
                has_data = tmp_df["zip_status"].astype(str).str.lower() == "success"
            else:
                if "form_name_mapping" not in tmp_df.columns:
                    continue
                has_data = tmp_df["form_name_mapping"].notna()
            done_df = tmp_df[has_data]

            if len(done_df) > 0:
                processed_ids.update(done_df[MATCH_COL].unique())
                processed_dfs.append(done_df)
        except Exception as e:
            tprint(f"[RESUME][WARN] Could not read {f}: {str(e).splitlines()[0]}")

    tprint(
        f"[RESUME] {len(processed_ids)} tracking numbers already processed — these will be skipped"
    )
    return processed_ids, processed_dfs


def parse_args():
    parser = argparse.ArgumentParser(description="SERFF filing attachment mapper")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        help="Run without visible browser",
    )
    mode_group.add_argument(
        "--headed",
        dest="headless",
        action="store_false",
        help="Run with visible browser for all browser actions (default)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of parallel worker processes (default: 2)",
    )
    parser.add_argument(
        "--with-zip",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Download ZIP attachments for each filing (default: enabled; use --no-with-zip to disable)",
    )
    parser.add_argument(
        "--reauth-lock-timeout-sec",
        type=int,
        default=600,
        help="Max seconds to wait for the cross-worker re-auth lock (default: 600); 0=wait forever",
    )
    parser.add_argument(
        "--row-timeout-sec",
        type=int,
        default=1800,
        help="Max wall-clock seconds per row on Unix/macOS via SIGALRM (default: 1800); "
        "0=disabled. On timeout the row is skipped (zip_status=skipped_timeout when --with-zip).",
    )
    parser.set_defaults(headless=False)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    n_proc = args.workers
    headed = not args.headless
    tprint(
        f"[CONFIG] reauth_lock_timeout_sec={args.reauth_lock_timeout_sec} "
        f"(0=wait forever), row_timeout_sec={args.row_timeout_sec} "
        f"(0=off; Unix/macOS skips row on expiry)"
    )

    form_df = pd.read_csv("data/form_data.csv")
    df = form_df.copy()
    df["SERFF Tracking Number"] = df["SERFF Tracking Number"].astype(str)

    df["tracking_len"] = df["SERFF Tracking Number"].astype(str).str.len()

    df = df.sort_values(by="tracking_len", ascending=True)

    df = df.drop(columns="tracking_len")

    # ------------------------------------------------------------------
    # RESUME: load already-processed tracking numbers and skip them
    # ------------------------------------------------------------------
    already_done, prev_result_dfs = load_already_processed(
        "outputs", with_zip=args.with_zip
    )

    original_len = len(df)
    if already_done:
        df = df[~df["SERFF Tracking Number"].isin(already_done)].reset_index(drop=True)
        tprint(
            f"[RESUME] Filtered: {original_len} -> {len(df)} rows remaining to process"
        )

    df = df.iloc[::-1].reset_index(drop=True)

    if len(df) == 0:
        tprint("[RESUME] All rows already processed! Combining previous results...")
        final_df = pd.concat(prev_result_dfs, ignore_index=True)
        final_df.to_csv("form_names_mapping.csv", index=False)
        tprint(f"\nResults saved to form_names_mapping.csv ({len(final_df)} rows)")
        exit(0)

    failed_retry_ids = load_failed_zip_ids("outputs") if args.with_zip else set()
    if failed_retry_ids:
        remaining_failed_ids = set(df["SERFF Tracking Number"]).intersection(
            failed_retry_ids
        )
    else:
        remaining_failed_ids = set()

    if remaining_failed_ids:
        df_untried = df[~df["SERFF Tracking Number"].isin(remaining_failed_ids)].copy()
        df_failed_retry = df[
            df["SERFF Tracking Number"].isin(remaining_failed_ids)
        ].copy()
    else:
        df_untried = df
        df_failed_retry = df.iloc[0:0].copy()

    work_units_untried = build_work_units(df_untried) if len(df_untried) else []
    work_units_failed = (
        build_work_units(df_failed_retry) if len(df_failed_retry) else []
    )
    work_units = work_units_untried + work_units_failed
    total_batches = (len(work_units) + n_proc - 1) // n_proc

    if SKIP_STATES:
        tprint(f"[SKIP] Skipping states: {', '.join(sorted(SKIP_STATES))}")
    tprint(f"\n{len(work_units)} states, {n_proc} workers, {total_batches} batches")
    tprint(
        f"Total URLs to process: {len(df)} (skipped {original_len - len(df)} already done)"
    )
    if args.with_zip:
        tprint(
            f"[ORDER] Untried first: {len(df_untried)} rows, failed retries last: {len(df_failed_retry)} rows"
        )
    for rank, (w_label, _s, w_df) in enumerate(work_units, 1):
        tprint(f"  {rank}. {w_label}: {len(w_df)} URLs")
    tprint("=" * 70)

    # Shared objects that persist across all batches
    total_counter = Value("i", 0)
    reauth_lock = Lock()
    manager = Manager()
    failed_states = manager.dict()

    stop_event = threading.Event()
    monitor = threading.Thread(
        target=_monitor_progress,
        args=(total_counter, len(df), stop_event),
        daemon=True,
    )
    monitor.start()

    all_result_files = []

    # ------------------------------------------------------------------
    # Process states in batches — auth only the batch about to run
    # so cookies stay fresh.
    # ------------------------------------------------------------------
    for batch_start in range(0, len(work_units), n_proc):
        batch = work_units[batch_start : batch_start + n_proc]
        batch_num = batch_start // n_proc + 1
        batch_states = [state_name for _label, state_name, _df in batch]

        tprint(f"\n{'='*70}")
        tprint(f"Batch {batch_num}/{total_batches}: {', '.join(batch_states)}")
        tprint(f"{'='*70}")

        preauth_cookies = pre_authenticate_all_states(batch_states, headed=headed)

        for s in batch_states:
            if s not in preauth_cookies and s not in STATES_CONTINUE_ON_FAILURE:
                failed_states[s] = True

        actual_procs = min(n_proc, len(batch))

        with Pool(
            actual_procs,
            initializer=_init_worker,
            initargs=(
                total_counter,
                failed_states,
                reauth_lock,
                dict(preauth_cookies),
                headed,
                args.with_zip,
                args.reauth_lock_timeout_sec,
                args.row_timeout_sec,
            ),
        ) as pool:
            results = list(pool.imap_unordered(process_state, batch))

        all_result_files.extend(results)

    stop_event.set()
    monitor.join()

    # ------------------------------------------------------------------
    # Combine ALL state results with PREVIOUSLY processed results
    # ------------------------------------------------------------------
    all_dfs = list(prev_result_dfs)
    all_state_csvs = []

    for f in all_result_files:
        if f is not None and os.path.exists(f):
            all_dfs.append(pd.read_csv(f))
            all_state_csvs.append(f)

    if not all_dfs:
        tprint("[ERROR] No valid results to combine")
        exit(1)

    final_df = pd.concat(all_dfs, ignore_index=True)
    # For duplicate tracking numbers, keep the row with the richest data:
    # prefer rows with form_name_mapping and/or ZIP-derived columns.
    zip_cols = [
        c
        for c in [
            "form_attachments",
            "supporting_document_attachments",
            "rate_rule_attachments",
            "correspondence_attachments",
            "toc_form_names",
            "readability_text",
        ]
        if c in final_df.columns
    ]
    helper_cols = []
    if "form_name_mapping" in final_df.columns:
        final_df["_has_mapping"] = final_df["form_name_mapping"].notna()
        helper_cols.append("_has_mapping")
    if zip_cols:
        final_df["_has_zip"] = final_df[zip_cols].notna().any(axis=1)
        helper_cols.append("_has_zip")
    if helper_cols:
        final_df = final_df.sort_values(helper_cols, ascending=False)
    final_df = final_df.drop_duplicates(subset="SERFF Tracking Number", keep="first")
    for col in ["_has_mapping", "_has_zip"]:
        if col in final_df.columns:
            final_df = final_df.drop(columns=[col])
    final_df.to_csv("form_names_mapping.csv", index=False)

    tprint(
        f"\nAll done. Results saved to form_names_mapping.csv ({len(final_df)} rows)"
        f"\n  ({len(prev_result_dfs)} previously processed files + {len(all_state_csvs)} new state results merged)"
    )
