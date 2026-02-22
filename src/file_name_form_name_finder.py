import argparse
import ast
import pandas as pd
import os
import re
import random
import time
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
    set_headed_mode,
    new_context_and_reauth,
    create_http_session,
    create_http_session_from_cookies,
    refresh_http_cookies,
    fetch_page,
    keepalive_session,
    pre_authenticate_all_states,
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


def _init_worker(
    counter, failed_states, reauth_lock, preauth_cookies=None, headed=True
):
    """Pool initializer: store shared objects in each worker."""
    global _shared_counter, _failed_states, _preauth_cookies
    _shared_counter = counter
    _failed_states = failed_states
    _preauth_cookies = preauth_cookies
    set_reauth_lock(reauth_lock)
    set_headed_mode(headed)


def _monitor_progress(counter, total, stop_event):
    """Monitor thread that displays total progress across all states."""
    pbar = tqdm(total=total, desc="Overall", position=0, leave=True, colour="green")
    completed = 0
    while not stop_event.is_set():
        new_val = counter.value
        if new_val > completed:
            pbar.update(new_val - completed)
            completed = new_val
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
            print(
                f"[WARN] PID {os.getpid()}: Failed to parse {section_name} panel: "
                f"{str(e).splitlines()[0]}"
            )

    return mapping


def _ensure_browser(pw, browser, context, page, state_name):
    """
    Lazily launch a headless browser for alpha-ID search navigation.
    If pre-authed cookies exist, loads them into the context (no re-auth).
    Re-uses existing browser if already running.
    Returns (pw, browser, context, page).
    """
    if browser is not None:
        return pw, browser, context, page
    from playwright.sync_api import sync_playwright
    from browser_utils import _launch_browser, _new_stealth_context

    print(
        f"[BROWSER] PID {os.getpid()} launching headless Firefox for alpha-ID search ({state_name})..."
    )
    pw = sync_playwright().start()
    browser = _launch_browser(pw, headed=False)
    context = _new_stealth_context(browser)
    page = context.new_page()

    if _preauth_cookies is not None and state_name in _preauth_cookies:
        context.add_cookies(_preauth_cookies[state_name])
        print(f"[BROWSER] PID {os.getpid()} loaded pre-authed cookies for {state_name}")
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
        print(
            f"[SKIP] {work_label}: state {state_name} already marked as failed, skipping"
        )
        return None

    df_state = df_state.copy()
    df_state["form_name_mapping"] = None

    # Stagger worker startups
    time.sleep(random.uniform(1, 6))

    # --- Bootstrap HTTP session -----------------------------------------------
    pw = browser = context = page = None
    http_session = None

    has_preauth = _preauth_cookies is not None and state_name in _preauth_cookies

    if has_preauth:
        http_session = create_http_session_from_cookies(_preauth_cookies[state_name])
        print(f"[WORKER] PID {os.getpid()} using pre-authed cookies for {state_name}")
    else:
        pw, browser, context, page = create_browser()
        auth_ok = authenticate(page, state_name)
        if not auth_ok:
            context, page, auth_ok = new_context_and_reauth(
                browser, state_name, old_context=context
            )
        if not auth_ok:
            print(f"[ERROR] Auth failed for {work_label} — marking state as failed")
            if _failed_states is not None:
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

            try:
                tracking_number = getattr(row, "_1", None)
                is_alpha = has_alpha_filing_id(url)
                html = None

                # ---- Path 1: Direct URL via requests (no browser) ----
                if not is_alpha:
                    page_html, expired, srv_err = fetch_page(http_session, url)

                    if srv_err:
                        print(
                            f"[500] PID {os.getpid()} row {idx}: Server error via HTTP for {url}"
                        )
                        time.sleep(random.uniform(15, 30))
                        page_html, expired, srv_err = fetch_page(http_session, url)

                    if expired and not srv_err:
                        print(
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
                            print(
                                f"[ERROR] PID {os.getpid()} row {idx}: Re-auth failed, skipping row"
                            )
                            continue

                    if page_html and not expired and not srv_err:
                        if (
                            "filingSummary" in url
                            or "attachmentsContainer" in page_html
                        ):
                            html = page_html

                # ---- Path 2: Alpha IDs or failed direct fetch -> Playwright search ----
                if html is None:
                    if not tracking_number or (
                        isinstance(tracking_number, float) and pd.isna(tracking_number)
                    ):
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: No tracking number for {url}"
                        )
                        continue

                    pw, browser, context, page = _ensure_browser(
                        pw, browser, context, page, state_name
                    )

                    for search_attempt in range(3):
                        if search_attempt > 0:
                            wait = 15 * search_attempt
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: Search attempt {search_attempt+1}/3 for {tracking_number}, waiting {wait}s..."
                            )
                            time.sleep(wait)
                            context, page, http_session, _ = _reauth_and_refresh(
                                browser, context, page, state_name, http_session
                            )

                        if navigate_via_search(
                            page, context, tracking_number, state_name
                        ):
                            if not is_server_error(page):
                                html = page.content()
                                break
                            print(
                                f"[500] PID {os.getpid()} row {idx}: 500 after search for {tracking_number}"
                            )
                            time.sleep(random.uniform(15, 30))
                        elif is_server_error(page):
                            print(
                                f"[500] PID {os.getpid()} row {idx}: 500 during search for {tracking_number}"
                            )
                            time.sleep(random.uniform(15, 30))

                    if html is None:
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: All nav failed for {tracking_number}"
                        )
                        continue

                # ---- Parse with BeautifulSoup ----
                mapping = scrape_attachment_mappings_html(html)
                # Restrict to this row's file_name list and build file -> form_name (for these files only)
                expected_files = []
                if "file_name" in df_state.columns:
                    expected_files = _parse_file_name_column(
                        df_state.at[idx, "file_name"]
                    )
                if expected_files:
                    # Subset: only store form names for the files we care about
                    subset = {}
                    for f in expected_files:
                        if f in mapping and mapping[f]:
                            # mapping[f] is list of {form_name, form_number, section}; take first
                            subset[f] = mapping[f][0].get("form_name") or ""
                        else:
                            subset[f] = None
                    any_found = any(subset.get(f) is not None for f in expected_files)
                    if any_found:
                        df_state.at[idx, "form_name_mapping"] = str(subset)
                    else:
                        # No files found for this row -> NA so it will be rerun (not considered done)
                        df_state.at[idx, "form_name_mapping"] = pd.NA
                else:
                    # No file_name list: keep full page mapping (backward compat)
                    df_state.at[idx, "form_name_mapping"] = (
                        str(mapping) if mapping else pd.NA
                    )

            except Exception as e:
                serf = getattr(row, "serf_num", "unknown")
                print(
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
                    print(
                        f"[THROTTLE] PID {os.getpid()} taking {pause:.0f}s breather after {idx} rows..."
                    )
                    time.sleep(pause)

            if idx % 100 == 0 and idx > 0:
                checkpoint_file = f"outputs/temp_results_{work_label}_{os.getpid()}.csv"
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
    result_file = f"outputs/state_results_{safe_label}.csv"
    df_state.to_csv(result_file, index=False)
    print(f"  ✓ Saved: {result_file} ({len(df_state)} rows)")
    return result_file


def load_already_processed(output_dir="outputs"):
    """
    Scan existing result CSVs and return a set of SERFF Tracking Numbers
    that have already been fully processed (non-null form_name_mapping).
    Also returns a list of the DataFrames so they can be included in the final merge.
    """
    import glob

    MATCH_COL = "SERFF Tracking Number"
    EXTRACTION_COLS = ["form_name_mapping"]

    processed_ids = set()
    processed_dfs = []

    existing_files = sorted(
        glob.glob(os.path.join(output_dir, "state_results_*.csv"))
        + glob.glob(os.path.join(output_dir, "temp_results_*.csv"))
    )

    if not existing_files:
        return processed_ids, processed_dfs

    print(
        f"[RESUME] Found {len(existing_files)} existing result files, scanning for already-processed rows..."
    )

    for f in existing_files:
        try:
            tmp_df = pd.read_csv(f)
            if MATCH_COL not in tmp_df.columns:
                continue

            tmp_df[MATCH_COL] = tmp_df[MATCH_COL].astype(str)

            available_cols = [c for c in EXTRACTION_COLS if c in tmp_df.columns]
            if not available_cols:
                continue

            has_data = tmp_df[available_cols].notna().any(axis=1)
            done_df = tmp_df[has_data]

            if len(done_df) > 0:
                processed_ids.update(done_df[MATCH_COL].unique())
                processed_dfs.append(done_df)
        except Exception as e:
            print(f"[RESUME][WARN] Could not read {f}: {str(e).splitlines()[0]}")

    print(
        f"[RESUME] {len(processed_ids)} tracking numbers already processed — these will be skipped"
    )
    return processed_ids, processed_dfs


def parse_args():
    parser = argparse.ArgumentParser(description="SERFF filing attachment mapper")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run pre-auth without a visible browser (fully unattended, uses backoff only)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel worker processes (default: 10)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    n_proc = args.workers
    headed = not args.headless

    form_df = pd.read_csv("data/final_batch_22_feb.csv")
    df = form_df.copy()
    df["SERFF Tracking Number"] = df["SERFF Tracking Number"].astype(str)

    # ------------------------------------------------------------------
    # RESUME: load already-processed tracking numbers and skip them
    # ------------------------------------------------------------------
    already_done, prev_result_dfs = load_already_processed("outputs")

    original_len = len(df)
    if already_done:
        df = df[~df["SERFF Tracking Number"].isin(already_done)].reset_index(drop=True)
        print(
            f"[RESUME] Filtered: {original_len} -> {len(df)} rows remaining to process"
        )

    df = df.iloc[::-1].reset_index(drop=True)

    if len(df) == 0:
        print("[RESUME] All rows already processed! Combining previous results...")
        final_df = pd.concat(prev_result_dfs, ignore_index=True)
        final_df.to_csv("form_names_mapping.csv", index=False)
        print(f"\nResults saved to form_names_mapping.csv ({len(final_df)} rows)")
        exit(0)

    work_units = build_work_units(df)
    total_batches = (len(work_units) + n_proc - 1) // n_proc

    print(f"\n{len(work_units)} states, {n_proc} workers, {total_batches} batches")
    print(
        f"Total URLs to process: {len(df)} (skipped {original_len - len(df)} already done)"
    )
    for rank, (w_label, _s, w_df) in enumerate(work_units, 1):
        print(f"  {rank}. {w_label}: {len(w_df)} URLs")
    print("=" * 70)

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
        batch_states = [s for s, _, _ in batch]

        print(f"\n{'='*70}")
        print(f"Batch {batch_num}/{total_batches}: {', '.join(batch_states)}")
        print(f"{'='*70}")

        preauth_cookies = pre_authenticate_all_states(batch_states, headed=headed)

        for s in batch_states:
            if s not in preauth_cookies:
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
        print("[ERROR] No valid results to combine")
        exit(1)

    final_df = pd.concat(all_dfs, ignore_index=True)
    # For duplicate tracking numbers, keep the row with form_name_mapping (so NA reruns don't overwrite good results)
    if "form_name_mapping" in final_df.columns:
        final_df["_has_mapping"] = final_df["form_name_mapping"].notna()
        final_df = final_df.sort_values("_has_mapping", ascending=False)
    final_df = final_df.drop_duplicates(subset="SERFF Tracking Number", keep="first")
    if "_has_mapping" in final_df.columns:
        final_df = final_df.drop(columns=["_has_mapping"])
    final_df.to_csv("form_names_mapping.csv", index=False)

    print(
        f"\nAll done. Results saved to form_names_mapping.csv ({len(final_df)} rows)"
        f"\n  ({len(prev_result_dfs)} previously processed files + {len(all_state_csvs)} new state results merged)"
    )
