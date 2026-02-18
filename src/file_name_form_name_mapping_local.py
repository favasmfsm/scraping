import pandas as pd
import numpy as np
import os
import re
import random
import time
from urllib.parse import unquote
from multiprocessing import Pool, Manager, cpu_count, Value
from tqdm import tqdm
import threading

from browser_utils import (
    create_browser,
    authenticate,
    restart_browser,
    navigate_via_search,
    is_session_expired,
    is_server_error,
    wait_for_server_recovery,
    BACKOFF_ON_500,
)

# Throttle settings — be gentle on the SERFF server
MIN_DELAY_BETWEEN_ROWS = 2  # minimum seconds between requests
MAX_DELAY_BETWEEN_ROWS = 5  # maximum seconds between requests

# Shared counter for total-progress tracking across worker processes
_shared_counter = None
# Shared set of states whose authentication has permanently failed —
# other workers skip chunks for these states immediately.
_failed_states = None


def _init_worker(counter, failed_states):
    """Pool initializer: store shared objects in each worker."""
    global _shared_counter, _failed_states
    _shared_counter = counter
    _failed_states = failed_states


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
    Returns list of (work_label, state_name, df_for_state) tuples,
    sorted largest-first so big states start processing early.
    """
    units = []
    for state_name, state_df in df.groupby("state"):
        state_df = state_df.reset_index(drop=True)
        units.append((state_name, state_name, state_df))
    # Largest states first for better load balancing
    units.sort(key=lambda x: len(x[2]), reverse=True)
    return units


def _try_direct_nav(page, url, idx, state_name):
    """
    Attempt direct URL navigation. Returns True if we landed on the filing
    summary page, False otherwise (session expired, redirected, 500, timeout).
    Handles 500 errors with backoff retries.
    """
    page.goto(url, wait_until="domcontentloaded")

    # Check for 500 server error first
    if is_server_error(page):
        print(
            f"[500] PID {os.getpid()} row {idx}: Server error on direct nav for {url}"
        )
        for err_attempt in range(len(BACKOFF_ON_500)):
            if wait_for_server_recovery(page, state_name, err_attempt):
                page.goto(url, wait_until="domcontentloaded")
                if not is_server_error(page) and "filingSummary" in page.url:
                    return True
        print(
            f"[500] PID {os.getpid()} row {idx}: Server still returning 500 after retries, falling back to search..."
        )
        return False

    if is_session_expired(page):
        print(
            f"[WARN] PID {os.getpid()} row {idx}: Session expired on direct nav for {url}, falling back to search..."
        )
        return False

    try:
        page.locator("div.row").first.wait_for(state="attached", timeout=15000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        if "filingSummary" in page.url:
            return True
        else:
            print(
                f"[WARN] PID {os.getpid()} row {idx}: Redirected to {page.url}, falling back to search..."
            )
            return False
    except Exception:
        print(
            f"[WARN] PID {os.getpid()} row {idx}: Direct nav failed for {url}, falling back to search..."
        )
        return False


def _try_direct_nav_fallback(page, url):
    """
    After a browser restart + failed search nav, try loading the URL directly
    and wait for page content. Returns True on success.
    """
    page.goto(url, wait_until="domcontentloaded")
    try:
        page.locator("div.row").first.wait_for(state="attached", timeout=15000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        return True
    except Exception:
        return False


def _re_navigate(page, context, tracking_number, is_alpha, url, state_name):
    """
    Re-navigate after a browser restart: try search first, then direct URL.
    Returns True if we successfully landed on the filing summary page.
    """
    if tracking_number and not (
        isinstance(tracking_number, float) and pd.isna(tracking_number)
    ):
        if navigate_via_search(page, context, tracking_number, state_name):
            return True
    if not is_alpha:
        return _try_direct_nav_fallback(page, url)
    return False


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


def _parse_panel_rows(page, panel_id, section_name):
    """
    Parse attachment rows from a single panel on the filing summary page.
    Dynamically detects column layout from the header row.

    Returns list of dicts:
      [{"filename": "...", "form_name": "...", "form_number": "...", "section": "..."}]
    """
    results = []
    panel = page.locator(f"[id='{panel_id}']")

    if panel.count() == 0:
        return results

    content_text = panel.inner_text().strip()
    if content_text == "None Available":
        return results

    # Discover column layout from the header row
    headers = panel.locator("div.summaryScheduleItemHeader")
    header_count = headers.count()
    if header_count == 0:
        return results

    col_map = {}
    for i in range(header_count):
        raw = headers.nth(i).inner_text().strip().lower()
        canonical = _HEADER_ALIASES.get(raw)
        if canonical:
            col_map[i] = canonical

    if "attachments" not in col_map.values():
        return results

    # Get all top-level .row divs inside the panel's output-panel wrapper
    output_panel = panel.locator("div.ui-outputpanel")
    if output_panel.count() == 0:
        output_panel = panel

    all_rows = output_panel.locator(":scope > div.row")
    row_count = all_rows.count()

    for r in range(row_count):
        row_el = all_rows.nth(r)
        cells = row_el.locator(":scope > div.summaryScheduleItemData")
        cell_count = cells.count()

        if cell_count == 0:
            continue

        row_data = {}
        attachment_cell = None
        for c in range(cell_count):
            canonical = col_map.get(c)
            if canonical == "attachments":
                attachment_cell = cells.nth(c)
            elif canonical:
                row_data[canonical] = cells.nth(c).inner_text().strip()

        if attachment_cell is None:
            continue

        links = attachment_cell.locator("a")
        link_count = links.count()
        if link_count == 0:
            continue

        form_name = row_data.get("form_name", "")
        form_number = row_data.get("form_number")

        for li in range(link_count):
            filename = links.nth(li).inner_text().strip()
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


def scrape_attachment_mappings(page):
    """
    Scrape the filing summary page to build a mapping of
    PDF filename → {form_name, form_number, section}.

    Returns dict keyed by filename, or empty dict on failure.
    """
    try:
        page.locator("#attachmentsContainer").wait_for(state="attached", timeout=15000)
    except Exception:
        return {}

    mapping = {}
    for section_name, panel_id in PANEL_IDS.items():
        try:
            rows = _parse_panel_rows(page, panel_id, section_name)
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


def process_state(state_data):
    """
    Process all rows for one state with a single browser.
    state_data is a tuple: (work_label, state_name, df_for_state)
    The worker owns this state end-to-end, checkpointing every 100 rows.
    """
    work_label, state_name, df_state = state_data

    # Fast-skip: another worker already proved this state is unreachable
    if _failed_states is not None and state_name in _failed_states:
        print(
            f"[SKIP] {work_label}: state {state_name} already marked as failed, skipping"
        )
        return None

    df_state = df_state.copy()
    df_state["form_name_mapping"] = None

    pw, browser, context, page = create_browser()

    try:
        # Authenticate with retries (3 attempts with backoff)
        AUTH_RETRIES = 3
        auth_ok = False
        for attempt in range(1, AUTH_RETRIES + 1):
            # Check again in case another worker marked it while we were retrying
            if _failed_states is not None and state_name in _failed_states:
                print(
                    f"[SKIP] {work_label}: state {state_name} marked as failed by another worker"
                )
                return None
            auth_ok = authenticate(page, state_name)
            if auth_ok:
                break
            if attempt < AUTH_RETRIES:
                wait = attempt * 5  # 5s, 10s backoff
                print(
                    f"[RETRY] {work_label}: auth attempt {attempt}/{AUTH_RETRIES} failed, retrying in {wait}s..."
                )
                time.sleep(wait)

        if not auth_ok:
            print(
                f"[ERROR] All {AUTH_RETRIES} auth attempts failed for {work_label} — marking state {state_name} as failed"
            )
            if _failed_states is not None:
                _failed_states[state_name] = True
            return None

        # Process all URLs for this state
        for row in tqdm(
            df_state.itertuples(),
            total=len(df_state),
            desc=f"{work_label} ({len(df_state)} URLs, PID {os.getpid()})",
        ):
            idx = row.Index
            url = row.page_url

            try:
                # --- Navigation ---
                # "SERFF Tracking Number" column becomes _7 in itertuples (spaces in name)
                tracking_number = getattr(row, "_7", None)
                is_alpha = has_alpha_filing_id(url)
                nav_ok = False

                # Step A: Try direct URL navigation first (skip for alpha IDs)
                if not is_alpha:
                    nav_ok = _try_direct_nav(page, url, idx, state_name)

                # Step B: If direct nav failed (or alpha ID), try search-based navigation
                if not nav_ok:
                    if not tracking_number or (
                        isinstance(tracking_number, float) and pd.isna(tracking_number)
                    ):
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: No SERFF Tracking Number, cannot search for {url}"
                        )
                        continue

                    # Try search-based nav with fresh browser on failure (3 attempts)
                    for search_attempt in range(3):
                        if search_attempt == 1:
                            # Second attempt: restart browser first
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: Search nav failed, restarting browser for retry..."
                            )
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )
                        elif search_attempt == 2:
                            # Third attempt: wait longer in case of server overload
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: Search nav failed again, waiting 30s then retrying..."
                            )
                            time.sleep(30)
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )

                        if navigate_via_search(
                            page, context, tracking_number, state_name
                        ):
                            # Check if search landed on a 500 page
                            if is_server_error(page):
                                print(
                                    f"[500] PID {os.getpid()} row {idx}: Server error after search nav for {tracking_number}"
                                )
                                wait_for_server_recovery(
                                    page, state_name, search_attempt
                                )
                                continue
                            nav_ok = True
                            break
                        # Check if the failure was due to a 500 error
                        if is_server_error(page):
                            print(
                                f"[500] PID {os.getpid()} row {idx}: Server error during search for {tracking_number}, waiting before retry..."
                            )
                            wait_for_server_recovery(page, state_name, search_attempt)
                        else:
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: Search nav attempt {search_attempt + 1}/3 failed for {tracking_number}"
                            )

                    if not nav_ok:
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: All nav methods failed for {url} ({tracking_number}), restarting browser..."
                        )
                        browser, context, page, _ = restart_browser(
                            pw, browser, state_name
                        )
                        continue

                mapping = scrape_attachment_mappings(page)
                if mapping:
                    df_state.at[idx, "form_name_mapping"] = str(mapping)

            except Exception as e:
                serf = getattr(row, "serf_num", "unknown")
                print(
                    f"[ERROR] PID {os.getpid()} row {idx} (serf_num={serf}): Unexpected error for {url}: {str(e).splitlines()[0]}, restarting browser..."
                )
                browser, context, page, _ = restart_browser(pw, browser, state_name)
                # Continue to next row — don't let one bad row crash the chunk
                continue
            finally:
                # Increment the total-progress counter (runs on success, skip, or error)
                if _shared_counter is not None:
                    with _shared_counter.get_lock():
                        _shared_counter.value += 1
                # Throttle: random delay between rows to avoid hammering the server
                delay = random.uniform(MIN_DELAY_BETWEEN_ROWS, MAX_DELAY_BETWEEN_ROWS)
                time.sleep(delay)

            # Checkpoint save every 100 rows
            if idx % 100 == 0 and idx > 0:
                checkpoint_file = f"outputs/temp_results_{work_label}_{os.getpid()}.csv"
                df_state.to_csv(checkpoint_file, index=False)
    finally:
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass

    # Save final results for this work unit
    safe_label = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in work_label
    )
    result_file = f"outputs/state_results_{safe_label}.csv"
    df_state.to_csv(result_file, index=False)
    print(f"  ✓ Saved: {result_file} ({len(df_state)} rows)")
    return result_file


def load_already_processed(output_dir="outputs"):
    """
    Scan existing temp_results_*.csv files and return a set of serf_num values
    that have already been fully processed (i.e., have at least one extraction column populated).
    Also returns a list of the DataFrames so they can be included in the final merge.
    """
    import glob

    EXTRACTION_COLS = ["form_name_mapping"]

    processed_serf_nums = set()
    processed_dfs = []

    # Look for both per-state CSVs (state_results_*) and chunk checkpoints (temp_results_*)
    existing_files = sorted(
        glob.glob(os.path.join(output_dir, "state_results_*.csv"))
        + glob.glob(os.path.join(output_dir, "temp_results_*.csv"))
    )

    if not existing_files:
        return processed_serf_nums, processed_dfs

    print(
        f"[RESUME] Found {len(existing_files)} existing result files, scanning for already-processed rows..."
    )

    for f in existing_files:
        try:
            tmp_df = pd.read_csv(f)
            if "serf_num" not in tmp_df.columns:
                continue

            # Find which extraction columns exist in this file
            available_cols = [c for c in EXTRACTION_COLS if c in tmp_df.columns]
            if not available_cols:
                continue

            # Rows that have at least one non-null extraction value are "done"
            has_data = tmp_df[available_cols].notna().any(axis=1)
            done_df = tmp_df[has_data]

            if len(done_df) > 0:
                processed_serf_nums.update(done_df["serf_num"].unique())
                processed_dfs.append(done_df)
        except Exception as e:
            print(f"[RESUME][WARN] Could not read {f}: {str(e).splitlines()[0]}")

    print(
        f"[RESUME] {len(processed_serf_nums)} serf_nums already processed — these will be skipped"
    )
    return processed_serf_nums, processed_dfs


if __name__ == "__main__":
    form_df = pd.read_csv("data/batch_1.csv")
    df = form_df.copy()

    # ------------------------------------------------------------------
    # RESUME: load already-processed serf_nums and skip them
    # ------------------------------------------------------------------
    already_done_serf_nums, prev_result_dfs = load_already_processed("outputs")

    original_len = len(df)
    if already_done_serf_nums:
        df = df[~df["serf_num"].isin(already_done_serf_nums)].reset_index(drop=True)
        print(
            f"[RESUME] Filtered: {original_len} → {len(df)} rows remaining to process"
        )

    if len(df) == 0:
        print("[RESUME] All rows already processed! Combining previous results...")
        final_df = pd.concat(prev_result_dfs, ignore_index=True)
        final_df.to_csv("form_names_readability_text.csv", index=False)
        print(
            f"\n✅ Results saved to form_names_readability_text.csv ({len(final_df)} rows)"
        )
        exit(0)

    n_proc = 6  # Single process — Playwright + multiprocessing causes EPIPE on macOS

    # Group data by state — one work unit per state, largest first
    work_units = build_work_units(df)

    # ------------------------------------------------------------------
    # Each core gets one state at a time. Largest states start first.
    # When a core finishes a state, it picks up the next unassigned
    # state automatically (via imap_unordered).
    # ------------------------------------------------------------------
    print(f"Processing {len(work_units)} states with {n_proc} processes")
    print(
        f"Total URLs to process: {len(df)} (skipped {original_len - len(df)} already done)"
    )
    for rank, (w_label, _s, w_df) in enumerate(work_units, 1):
        print(f"  {rank}. {w_label}: {len(w_df)} URLs")
    print("=" * 70)

    actual_procs = min(n_proc, len(work_units))

    # Shared counter for total progress across all workers
    total_counter = Value("i", 0)
    # Shared dict to track states whose auth permanently failed
    manager = Manager()
    failed_states = manager.dict()

    stop_event = threading.Event()
    monitor = threading.Thread(
        target=_monitor_progress,
        args=(total_counter, len(df), stop_event),
        daemon=True,
    )
    monitor.start()

    with Pool(
        actual_procs, initializer=_init_worker, initargs=(total_counter, failed_states)
    ) as pool:
        # imap_unordered feeds states (largest first); each worker picks
        # the next state as soon as it finishes one
        state_result_files = list(pool.imap_unordered(process_state, work_units))

    stop_event.set()
    monitor.join()

    # ------------------------------------------------------------------
    # Combine ALL state results with PREVIOUSLY processed results
    # ------------------------------------------------------------------
    all_dfs = list(prev_result_dfs)  # copy the list
    all_state_csvs = []

    for f in state_result_files:
        if f is not None and os.path.exists(f):
            all_dfs.append(pd.read_csv(f))
            all_state_csvs.append(f)

    if not all_dfs:
        print("[ERROR] No valid results to combine")
        exit(1)

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Deduplicate by serf_num — keep the last (most recent) entry
    final_df = final_df.drop_duplicates(subset="serf_num", keep="last")

    final_df.to_csv("form_names_mapping.csv", index=False)

    print(
        f"\n✅ All done. Results saved to form_names_readability_text.csv ({len(final_df)} rows)"
        f"\n   ({len(prev_result_dfs)} previously processed files + {len(all_state_csvs)} new state results merged)"
    )
