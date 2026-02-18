"""
SERFF filing scraper — AWS EC2 version (form-name mapping).

Scrapes the filing summary page to build filename -> form_name mappings
from the attachment panels (Forms, Rate/Rule, Supporting Docs, Correspondence).
Does NOT download ZIPs — reads panel HTML directly.

AWS-specific features:
  - Periodic /tmp cleanup for stale Chromium temp dirs
  - Disk space monitoring (warns if free space drops below threshold)
  - CDP cache clearing on browser restarts (via browser_utils_aws)
  - Configurable process count (N_PROC) for different EC2 instance sizes
  - CHUNK_SIZE splitting for better load balancing across cores
"""

import math
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

from browser_utils_aws import (
    create_browser,
    authenticate,
    restart_browser,
    navigate_via_search,
    is_session_expired,
    is_server_error,
    wait_for_server_recovery,
    cleanup_tmp_chromium_dirs,
    BACKOFF_ON_500,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_CSV = "data/old_data_from_name_fetch.csv"
# INPUT_CSV = "data/test_sample_29.csv"

# Number of parallel browser workers (tune for your EC2 instance)
#   t3.medium  -> 2
#   c5.xlarge  -> 4
#   c5.2xlarge -> 6-8
#   c5.4xlarge -> 14
N_PROC = min(cpu_count(), 29)

# Throttle settings — be gentle on the SERFF server
MIN_DELAY_BETWEEN_ROWS = 2  # minimum seconds between requests
MAX_DELAY_BETWEEN_ROWS = 5  # maximum seconds between requests

# Disk space warning threshold (MB)
DISK_WARN_THRESHOLD_MB = 500

# Rows per work unit — controls chunk granularity.
# Smaller = better load balance across cores but more browser auth overhead.
CHUNK_SIZE = 1000

# Stagger range (seconds) — each worker sleeps a random amount in
# [0, AUTH_STAGGER_MAX_SEC] before its first authentication call so that
# workers don't all hit the auth endpoint at the same instant.
AUTH_STAGGER_MAX_SEC = 30

# ---------------------------------------------------------------------------
# Shared state for multiprocessing
# ---------------------------------------------------------------------------

_shared_counter = None
_failed_states = None
_startup_order = None


def _init_worker(counter, failed_states, startup_order):
    """Pool initializer: store shared objects in each worker."""
    global _shared_counter, _failed_states, _startup_order
    _shared_counter = counter
    _failed_states = failed_states
    _startup_order = startup_order


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
    new_val = counter.value
    if new_val > completed:
        pbar.update(new_val - completed)
    pbar.close()


# ---------------------------------------------------------------------------
# AWS-specific helpers
# ---------------------------------------------------------------------------


def check_disk_space(path="/"):
    """Check free disk space and warn if below threshold. Returns free MB."""
    try:
        stat = os.statvfs(path)
        free_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
        if free_mb < DISK_WARN_THRESHOLD_MB:
            print(
                f"[DISK] WARNING: Only {free_mb:.0f} MB free on {path} "
                f"(threshold: {DISK_WARN_THRESHOLD_MB} MB)"
            )
        return free_mb
    except Exception as e:
        print(f"[DISK] Could not check disk space: {str(e).splitlines()[0]}")
        return float("inf")


# ---------------------------------------------------------------------------
# URL / filing helpers
# ---------------------------------------------------------------------------


def has_alpha_filing_id(url):
    """
    Check if a SERFF URL has an alphanumeric filing ID (contains letters).
    Numeric-only IDs can be navigated directly; alphanumeric ones need search.
    """
    match = re.search(r"filingId=([^&]+)", url)
    if not match:
        return False
    filing_id = match.group(1)
    return bool(re.search(r"[A-Za-z]", filing_id))


# ---------------------------------------------------------------------------
# Work-unit construction
# ---------------------------------------------------------------------------


def build_work_units(df):
    """
    Split rows into equal-sized single-state chunks of at most CHUNK_SIZE rows.

    Each state is split independently, so every chunk belongs to exactly one
    state (preserving the single-auth-per-worker model).  The pool's
    imap_unordered distributes chunks to cores — when a core finishes a
    chunk it immediately picks up the next one.

    Returns list of (work_label, state_name, df_chunk) tuples,
    sorted largest-chunk-first.
    """
    units = []
    for state_name, state_df in df.groupby("state"):
        state_df = state_df.reset_index(drop=True)
        n_chunks = max(1, math.ceil(len(state_df) / CHUNK_SIZE))
        chunks = np.array_split(state_df, n_chunks)
        if n_chunks == 1:
            units.append((state_name, state_name, chunks[0].reset_index(drop=True)))
        else:
            for i, chunk_df in enumerate(chunks, 1):
                label = f"{state_name}_chunk{i}of{n_chunks}"
                units.append((label, state_name, chunk_df.reset_index(drop=True)))
    units.sort(key=lambda x: len(x[2]), reverse=True)
    return units


# ---------------------------------------------------------------------------
# Navigation helpers (per-row)
# ---------------------------------------------------------------------------


def _try_direct_nav(page, url, idx, state_name):
    """
    Attempt direct URL navigation. Returns True if we landed on the filing
    summary page, False otherwise (session expired, redirected, 500, timeout).
    """
    page.goto(url, wait_until="domcontentloaded")

    if is_server_error(page):
        print(
            f"[500] PID {os.getpid()} row {idx}: "
            f"Server error on direct nav for {url}"
        )
        for err_attempt in range(len(BACKOFF_ON_500)):
            if wait_for_server_recovery(page, state_name, err_attempt):
                page.goto(url, wait_until="domcontentloaded")
                if not is_server_error(page) and "filingSummary" in page.url:
                    return True
        print(
            f"[500] PID {os.getpid()} row {idx}: "
            f"Server still returning 500 after retries, falling back to search..."
        )
        return False

    if is_session_expired(page):
        print(
            f"[WARN] PID {os.getpid()} row {idx}: "
            f"Session expired on direct nav for {url}, falling back to search..."
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
                f"[WARN] PID {os.getpid()} row {idx}: "
                f"Redirected to {page.url}, falling back to search..."
            )
            return False
    except Exception:
        print(
            f"[WARN] PID {os.getpid()} row {idx}: "
            f"Direct nav failed for {url}, falling back to search..."
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


# ---------------------------------------------------------------------------
# Panel scraping
# ---------------------------------------------------------------------------


PANEL_IDS = {
    "forms": "summaryForm:formAttachmentPanel_content",
    "rate_rule": "summaryForm:rateRuleAttachmentPanel_content",
    "supporting_documentation": "summaryForm:supportingDocumentAttachmentPanel_content",
    "correspondence": "summaryForm:correspondenceAttachmentPanel_content",
}

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
    PDF filename -> {form_name, form_number, section}.

    Returns dict keyed by filename, or empty dict on failure.
    """
    try:
        page.locator("#attachmentsContainer").wait_for(
            state="attached", timeout=15000
        )
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
                f"[WARN] PID {os.getpid()}: Failed to parse "
                f"{section_name} panel: {str(e).splitlines()[0]}"
            )

    return mapping


# ---------------------------------------------------------------------------
# Main per-state worker
# ---------------------------------------------------------------------------


def process_state(state_data):
    """
    Process all rows for one state/chunk with a single browser.
    state_data is a tuple: (work_label, state_name, df_for_state)
    The worker owns this chunk end-to-end, checkpointing every 100 rows.
    """
    work_label, state_name, df_state = state_data

    if _failed_states is not None and state_name in _failed_states:
        print(
            f"[SKIP] {work_label}: state {state_name} already marked as "
            f"failed, skipping"
        )
        return None

    df_state = df_state.copy()
    df_state["form_name_mapping"] = None

    pw, browser, context, page = create_browser()

    # Stagger initial auth: each worker gets a sequential order number and
    # sleeps proportionally so requests are spread over AUTH_STAGGER_MAX_SEC.
    if _startup_order is not None:
        with _startup_order.get_lock():
            order = _startup_order.value
            _startup_order.value += 1
        stagger = (order / max(N_PROC, 1)) * AUTH_STAGGER_MAX_SEC
        stagger += random.uniform(0, 2)
        print(
            f"[STAGGER] {work_label}: worker #{order} waiting "
            f"{stagger:.1f}s before auth"
        )
        time.sleep(stagger)

    try:
        AUTH_RETRIES = 3
        auth_ok = False
        for attempt in range(1, AUTH_RETRIES + 1):
            if _failed_states is not None and state_name in _failed_states:
                print(
                    f"[SKIP] {work_label}: state {state_name} marked as "
                    f"failed by another worker"
                )
                return None
            auth_ok = authenticate(page, state_name)
            if auth_ok:
                break
            if attempt < AUTH_RETRIES:
                wait = attempt * 5
                print(
                    f"[RETRY] {work_label}: auth attempt {attempt}/{AUTH_RETRIES} "
                    f"failed, retrying in {wait}s..."
                )
                time.sleep(wait)

        if not auth_ok:
            print(
                f"[ERROR] All {AUTH_RETRIES} auth attempts failed for "
                f"{work_label} — marking state {state_name} as failed"
            )
            if _failed_states is not None:
                _failed_states[state_name] = True
            return None

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
                nav_ok = False

                if not is_alpha:
                    nav_ok = _try_direct_nav(page, url, idx, state_name)

                if not nav_ok:
                    if not tracking_number or (
                        isinstance(tracking_number, float)
                        and pd.isna(tracking_number)
                    ):
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: "
                            f"No SERFF Tracking Number, cannot search for {url}"
                        )
                        continue

                    for search_attempt in range(3):
                        if search_attempt == 1:
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: "
                                f"Search nav failed, restarting browser for retry..."
                            )
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )
                        elif search_attempt == 2:
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: "
                                f"Search nav failed again, waiting 30s then retrying..."
                            )
                            time.sleep(30)
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )

                        if navigate_via_search(
                            page, context, tracking_number, state_name
                        ):
                            if is_server_error(page):
                                print(
                                    f"[500] PID {os.getpid()} row {idx}: "
                                    f"Server error after search nav for "
                                    f"{tracking_number}"
                                )
                                wait_for_server_recovery(
                                    page, state_name, search_attempt
                                )
                                continue
                            nav_ok = True
                            break
                        if is_server_error(page):
                            print(
                                f"[500] PID {os.getpid()} row {idx}: "
                                f"Server error during search for "
                                f"{tracking_number}, waiting before retry..."
                            )
                            wait_for_server_recovery(
                                page, state_name, search_attempt
                            )
                        else:
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: "
                                f"Search nav attempt {search_attempt + 1}/3 "
                                f"failed for {tracking_number}"
                            )

                    if not nav_ok:
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: "
                            f"All nav methods failed for {url} "
                            f"({tracking_number}), restarting browser..."
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
                    f"[ERROR] PID {os.getpid()} row {idx} (serf_num={serf}): "
                    f"Unexpected error for {url}: "
                    f"{str(e).splitlines()[0]}, restarting browser..."
                )
                browser, context, page, _ = restart_browser(
                    pw, browser, state_name
                )
                continue
            finally:
                if _shared_counter is not None:
                    with _shared_counter.get_lock():
                        _shared_counter.value += 1
                delay = random.uniform(MIN_DELAY_BETWEEN_ROWS, MAX_DELAY_BETWEEN_ROWS)
                time.sleep(delay)

            # Checkpoint save every 100 rows
            if idx % 100 == 0 and idx > 0:
                checkpoint_file = (
                    f"outputs/temp_results_{work_label}_{os.getpid()}.csv"
                )
                df_state.to_csv(checkpoint_file, index=False)
                # AWS: periodic cleanup of stale Chromium temp dirs
                cleanup_tmp_chromium_dirs(max_age_hours=1)
                # AWS: check disk space
                check_disk_space("/")
    finally:
        try:
            browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass
        # AWS: cleanup stale Chromium temp dirs on worker exit
        cleanup_tmp_chromium_dirs(max_age_hours=0)

    safe_label = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in work_label
    )
    result_file = f"outputs/state_results_{safe_label}.csv"
    df_state.to_csv(result_file, index=False)
    print(f"  -> Saved: {result_file} ({len(df_state)} rows)")
    return result_file


# ---------------------------------------------------------------------------
# Resume logic
# ---------------------------------------------------------------------------


def load_already_processed(output_dir="outputs"):
    """
    Scan existing result files and return a set of serf_num values that have
    already been fully processed. Also returns a list of the DataFrames so
    they can be included in the final merge.

    Sources checked:
    1. processed_serf_nums.csv (plain list of serf_nums, in output_dir or subdirs)
    2. state_results_*.csv and temp_results_*.csv in output_dir and subdirs
    """
    import glob

    EXTRACTION_COLS = ["form_name_mapping"]

    processed_serf_nums = set()
    processed_dfs = []

    # --- 1. Load serf_nums from any processed_serf_nums.csv files ---
    serf_num_files = sorted(
        glob.glob(os.path.join(output_dir, "processed_serf_nums.csv"))
        + glob.glob(
            os.path.join(output_dir, "**", "processed_serf_nums.csv"),
            recursive=True,
        )
    )
    for f in serf_num_files:
        try:
            sn_df = pd.read_csv(f)
            if "serf_num" in sn_df.columns:
                new_nums = set(sn_df["serf_num"].dropna().unique())
                processed_serf_nums.update(new_nums)
                print(f"[RESUME] Loaded {len(new_nums)} serf_nums from {f}")
        except Exception as e:
            print(
                f"[RESUME][WARN] Could not read {f}: "
                f"{str(e).splitlines()[0]}"
            )

    # --- 2. Scan state_results / temp_results CSVs (including subdirs) ---
    existing_files = sorted(
        set(
            glob.glob(os.path.join(output_dir, "state_results_*.csv"))
            + glob.glob(os.path.join(output_dir, "temp_results_*.csv"))
            + glob.glob(
                os.path.join(output_dir, "**", "state_results_*.csv"),
                recursive=True,
            )
            + glob.glob(
                os.path.join(output_dir, "**", "temp_results_*.csv"),
                recursive=True,
            )
        )
    )

    if existing_files:
        print(
            f"[RESUME] Found {len(existing_files)} existing result files, "
            f"scanning for already-processed rows..."
        )

    for f in existing_files:
        try:
            tmp_df = pd.read_csv(f)
            if "serf_num" not in tmp_df.columns:
                continue

            available_cols = [c for c in EXTRACTION_COLS if c in tmp_df.columns]
            if not available_cols:
                continue

            has_data = tmp_df[available_cols].notna().any(axis=1)
            done_df = tmp_df[has_data]

            if len(done_df) > 0:
                processed_serf_nums.update(done_df["serf_num"].unique())
                processed_dfs.append(done_df)
        except Exception as e:
            print(
                f"[RESUME][WARN] Could not read {f}: "
                f"{str(e).splitlines()[0]}"
            )

    print(
        f"[RESUME] {len(processed_serf_nums)} serf_nums already processed "
        f"-- these will be skipped"
    )
    return processed_serf_nums, processed_dfs


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    # AWS: initial cleanup of stale Chromium temp dirs before starting
    cleanup_tmp_chromium_dirs(max_age_hours=0)

    # AWS: check disk space before starting
    free_mb = check_disk_space("/")
    print(f"[DISK] Free disk space: {free_mb:.0f} MB")

    form_df = pd.read_csv(INPUT_CSV)
    df = form_df.copy()

    # ------------------------------------------------------------------
    # RESUME: load already-processed serf_nums and skip them
    # ------------------------------------------------------------------
    already_done_serf_nums, prev_result_dfs = load_already_processed("outputs")

    original_len = len(df)
    if already_done_serf_nums:
        df = df[~df["serf_num"].isin(already_done_serf_nums)].reset_index(
            drop=True
        )
        print(
            f"[RESUME] Filtered: {original_len} -> {len(df)} rows remaining "
            f"to process"
        )

    if len(df) == 0:
        print(
            "[RESUME] All rows already processed! Combining previous results..."
        )
        final_df = pd.concat(prev_result_dfs, ignore_index=True)
        final_df.to_csv("form_names_mapping.csv", index=False)
        print(
            f"\nResults saved to form_names_mapping.csv "
            f"({len(final_df)} rows)"
        )
        exit(0)

    n_proc = N_PROC

    # Split rows into equal-sized chunks (at most CHUNK_SIZE rows each)
    work_units = build_work_units(df)

    # ------------------------------------------------------------------
    n_states = df["state"].nunique()
    print(
        f"Processing {n_states} states split into {len(work_units)} "
        f"chunks (~{CHUNK_SIZE} rows each) across {n_proc} cores"
    )
    print(
        f"Total URLs to process: {len(df)} "
        f"(skipped {original_len - len(df)} already done)"
    )
    from collections import defaultdict
    _state_chunks = defaultdict(list)
    for w_label, _s, w_df in work_units:
        _state_chunks[_s].append((w_label, len(w_df)))
    for rank, (state, chunks) in enumerate(
        sorted(
            _state_chunks.items(),
            key=lambda x: sum(c[1] for c in x[1]),
            reverse=True,
        ),
        1,
    ):
        total_rows = sum(c[1] for c in chunks)
        if len(chunks) == 1:
            print(f"  {rank}. {state}: {total_rows} URLs (1 chunk)")
        else:
            chunk_sizes = ", ".join(f"{c[1]}" for c in chunks)
            print(
                f"  {rank}. {state}: {total_rows} URLs "
                f"-> {len(chunks)} chunks [{chunk_sizes}]"
            )
    print("=" * 70)

    actual_procs = min(n_proc, len(work_units))

    total_counter = Value("i", 0)
    startup_order = Value("i", 0)
    manager = Manager()
    failed_states = manager.dict()

    stop_event = threading.Event()
    monitor = threading.Thread(
        target=_monitor_progress,
        args=(total_counter, len(df), stop_event),
        daemon=True,
    )
    monitor.start()

    os.makedirs("outputs", exist_ok=True)

    with Pool(
        actual_procs,
        initializer=_init_worker,
        initargs=(total_counter, failed_states, startup_order),
    ) as pool:
        state_result_files = list(
            pool.imap_unordered(process_state, work_units)
        )

    stop_event.set()
    monitor.join()

    # ------------------------------------------------------------------
    # Combine ALL state results with PREVIOUSLY processed results
    # ------------------------------------------------------------------
    all_dfs = list(prev_result_dfs)
    all_state_csvs = []

    for f in state_result_files:
        if f is not None and os.path.exists(f):
            all_dfs.append(pd.read_csv(f))
            all_state_csvs.append(f)

    if not all_dfs:
        print("[ERROR] No valid results to combine")
        exit(1)

    final_df = pd.concat(all_dfs, ignore_index=True)

    final_df = final_df.drop_duplicates(subset="serf_num", keep="last")

    final_df.to_csv("form_names_mapping.csv", index=False)

    # AWS: final cleanup
    cleanup_tmp_chromium_dirs(max_age_hours=0)

    print(
        f"\nAll done. Results saved to form_names_mapping.csv "
        f"({len(final_df)} rows)"
        f"\n   ({len(prev_result_dfs)} previously processed + "
        f"{len(all_state_csvs)} new states merged)"
    )
