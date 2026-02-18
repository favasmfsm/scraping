"""
SERFF filing scraper — AWS EC2 version.

Standalone script — imports ONLY from browser_utils_aws (never browser_utils).
Downloads ZIP files from SERFF, extracts attachment info, TOC form names,
and readability text from PDFs.

AWS-specific features:
  - Periodic /tmp cleanup for stale Chromium temp dirs
  - Disk space monitoring (warns if free space drops below threshold)
  - CDP cache clearing on browser restarts
  - Configurable process count (N_PROC) for different EC2 instance sizes
"""

import pandas as pd
import numpy as np
import os
import re
import random
import shutil
import time
import zipfile
from urllib.parse import unquote
from multiprocessing import Pool, Manager, cpu_count, Value
from tqdm import tqdm
import threading
import fitz  # PyMuPDF

from browser_utils_aws import (
    create_browser,
    authenticate,
    restart_browser,
    navigate_via_search,
    is_session_expired,
    is_server_error,
    wait_for_server_recovery,
    click_select_buttons,
    wait_for_ajax,
    download_zip,
    cleanup_tmp_chromium_dirs,
    BACKOFF_ON_500,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Input CSV — change this when switching between batches
INPUT_CSV = "data/remaining_batch_a.csv"

# Number of parallel browser workers (tune for your EC2 instance)
#   t3.medium  → 2
#   c5.xlarge  → 4
#   c5.2xlarge → 6-8
N_PROC = min(cpu_count(), 4)

# Throttle settings — be gentle on the SERFF server
MIN_DELAY_BETWEEN_ROWS = 2  # minimum seconds between requests
MAX_DELAY_BETWEEN_ROWS = 5  # maximum seconds between requests

# Disk space warning threshold (MB). Warn if free space drops below this.
DISK_WARN_THRESHOLD_MB = 500

BASE_DIR = "downloads"

# ---------------------------------------------------------------------------
# Shared state for multiprocessing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# AWS-specific helpers
# ---------------------------------------------------------------------------


def check_disk_space(path="/"):
    """
    Check free disk space and warn if below threshold.
    Returns free space in MB.
    """
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
    e.g. filingId=6LJMVY298/00 -> True, filingId=132569558 -> False
    """
    match = re.search(r"filingId=([^&]+)", url)
    if not match:
        return False
    filing_id = match.group(1)
    return bool(re.search(r"[A-Za-z]", filing_id))


def is_form_number(text):
    """
    Check if a line looks like a form number rather than a form name.
    Form numbers typically have patterns like:
    - SML-LTD-MP-CO (uppercase with dashes)
    - 0013252XX 01/2015 (numbers with XX pattern)
    - 0006017XX 06/2010
    """
    if re.match(r"^[A-Z0-9\-]+$", text) and "-" in text:
        return True
    if re.match(r"^\d+XX\s+\d{2}/\d{4}$", text):
        return True
    if re.match(r"^\d+XX$", text):
        return True
    return False


# ---------------------------------------------------------------------------
# PDF / ZIP extraction helpers
# ---------------------------------------------------------------------------


def extract_toc_form_names(root_pdf_path):
    """
    Extract form name mappings from the Table of Contents on page 1 of the root PDF.
    Returns dict: {filename: form_name}

    The TOC structure is:
    - Form Attachments section: Form Name | Form Number | filename.pdf
    - Supporting Document section: Document Name | filename.pdf
    """
    toc_mapping = {}

    try:
        pdf = fitz.open(root_pdf_path)
        page = pdf[0]
        text = page.get_text()
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        for i, line in enumerate(lines):
            if line.endswith(".pdf"):
                filename = line
                for j in range(i - 1, max(0, i - 5), -1):
                    prev_line = lines[j]
                    if (
                        not prev_line.endswith(".pdf")
                        and "Attachments" not in prev_line
                        and "(ex." not in prev_line
                        and prev_line
                        not in [
                            "Form Attachments",
                            "Supporting Document",
                            "User Usage Agreement",
                        ]
                        and not prev_line.startswith("SERFF Tracking")
                        and not prev_line.startswith("State Tracking")
                        and not prev_line.startswith("Company Tracking")
                        and not is_form_number(prev_line)
                    ):
                        toc_mapping[filename] = prev_line
                        break

        pdf.close()
    except Exception as e:
        print(
            f"[WARN] Could not extract TOC from {root_pdf_path}: "
            f"{str(e).splitlines()[0]}"
        )

    return toc_mapping


def process_subfolder_files(subfolder_path):
    """
    Process all files in a subfolder and return file info dict.
    Returns dict: {filename: page_count (for PDFs) or file_extension (for non-PDFs)}
    """
    file_info = {}

    try:
        for filename in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, filename)

            if not os.path.isfile(file_path):
                continue

            if filename.lower().endswith(".pdf"):
                try:
                    pdf = fitz.open(file_path)
                    file_info[filename] = len(pdf)
                    pdf.close()
                except Exception as e:
                    print(
                        f"[WARN] Could not read PDF {filename}: "
                        f"{str(e).splitlines()[0]}"
                    )
                    file_info[filename] = 0
            else:
                ext = os.path.splitext(filename)[1].lstrip(".")
                file_info[filename] = ext if ext else "unknown"
    except Exception as e:
        print(
            f"[ERROR] Failed to process subfolder {subfolder_path}: "
            f"{str(e).splitlines()[0]}"
        )

    return file_info


def extract_readability_text(extract_dir, ignore_folders):
    """
    Extract text from PDFs with 'read' or 'flesch' in the filename.
    Returns dict: {filename: extracted_text}
    """
    readability_texts = {}

    for subfolder in os.listdir(extract_dir):
        subfolder_path = os.path.join(extract_dir, subfolder)

        if subfolder in ignore_folders or not os.path.isdir(subfolder_path):
            continue

        for filename in os.listdir(subfolder_path):
            name_lower = filename.lower()

            if (
                "read" in name_lower or "flesch" in name_lower
            ) and filename.lower().endswith(".pdf"):
                file_path = os.path.join(subfolder_path, filename)
                try:
                    pdf = fitz.open(file_path)
                    text = ""
                    for page in pdf:
                        text += page.get_text() or ""
                    pdf.close()
                    readability_texts[filename] = text
                except Exception as e:
                    print(
                        f"[WARN] Could not extract text from {filename}: "
                        f"{str(e).splitlines()[0]}"
                    )
                    readability_texts[filename] = None

    return readability_texts


# ---------------------------------------------------------------------------
# Work-unit construction
# ---------------------------------------------------------------------------


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
    units.sort(key=lambda x: len(x[2]), reverse=True)
    return units


# ---------------------------------------------------------------------------
# Navigation helpers (per-row)
# ---------------------------------------------------------------------------


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
# Main per-state worker
# ---------------------------------------------------------------------------


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
            f"[SKIP] {work_label}: state {state_name} already marked as failed, "
            f"skipping"
        )
        return None

    df_state = df_state.copy()
    # Initialize new columns for ZIP-based extraction
    df_state["form_attachments"] = [None] * len(df_state)
    df_state["supporting_document_attachments"] = [None] * len(df_state)
    df_state["rate_rule_attachments"] = [None] * len(df_state)
    df_state["correspondence_attachments"] = [None] * len(df_state)
    df_state["toc_form_names"] = [None] * len(df_state)
    df_state["readability_text"] = [None] * len(df_state)

    # Create a single download directory per process (reused for all URLs)
    download_path = os.path.join(BASE_DIR, f"proc_{os.getpid()}", work_label)
    download_path = os.path.abspath(download_path)
    os.makedirs(download_path, exist_ok=True)

    pw, browser, context, page = create_browser()

    try:
        # Authenticate with retries (3 attempts with backoff)
        AUTH_RETRIES = 3
        auth_ok = False
        for attempt in range(1, AUTH_RETRIES + 1):
            # Check again in case another worker marked it while we were retrying
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
                wait = attempt * 5  # 5s, 10s backoff
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

        # Folders to ignore when processing ZIP contents
        IGNORE_FOLDERS = {"User Usage Agreement Attachments"}

        # Column mapping: normalized subfolder name -> DataFrame column name
        FOLDER_TO_COLUMN = {
            "form_attachments": "form_attachments",
            "supporting_document_attachments": "supporting_document_attachments",
            "rate_rule_attachments": "rate_rule_attachments",
            "correspondence_attachments": "correspondence_attachments",
        }

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
                tracking_number = getattr(row, "_7", None)
                is_alpha = has_alpha_filing_id(url)
                nav_ok = False

                # Step A: Try direct URL navigation first (skip for alpha IDs)
                if not is_alpha:
                    nav_ok = _try_direct_nav(page, url, idx, state_name)

                # Step B: If direct nav failed (or alpha ID), try search
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

                    # Try search-based nav with fresh browser on failure (3 attempts)
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

                # --- Session check + Select buttons + Download (up to 3 retries) ---
                zip_file_path = None
                for _page_attempt in range(3):

                    # Check for 500 error AFTER navigation
                    if is_server_error(page):
                        print(
                            f"[500] PID {os.getpid()} row {idx}: "
                            f"Server error before download "
                            f"(attempt {_page_attempt + 1}) for {url}"
                        )
                        if _page_attempt < 2:
                            wait_for_server_recovery(
                                page, state_name, _page_attempt
                            )
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )
                            if _re_navigate(
                                page, context, tracking_number,
                                is_alpha, url, state_name,
                            ):
                                continue
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: "
                            f"Server error persists for {url}, skipping"
                        )
                        break

                    # Check for session expired AFTER navigation
                    if is_session_expired(page):
                        if _page_attempt < 2:
                            print(
                                f"[WARN] PID {os.getpid()} row {idx}: "
                                f"Session expired (attempt {_page_attempt + 1}) "
                                f"for {url}, restarting browser..."
                            )
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )
                            if _re_navigate(
                                page, context, tracking_number,
                                is_alpha, url, state_name,
                            ):
                                continue
                            print(
                                f"[SKIP] PID {os.getpid()} row {idx}: "
                                f"Re-navigation failed after session restart "
                                f"for {url}"
                            )
                            break
                        else:
                            print(
                                f"[SKIP] PID {os.getpid()} row {idx}: "
                                f"Session still expired after "
                                f"{_page_attempt + 1} attempts for {url}, "
                                f"skipping"
                            )
                            break

                    # Step 2: Click "Select Current Version Only" buttons
                    click_select_buttons(page)

                    # Step 3: Wait for PrimeFaces AJAX, then check session
                    wait_for_ajax(page)

                    # Re-check session after Select clicks
                    if is_session_expired(page):
                        print(
                            f"[WARN] PID {os.getpid()} row {idx}: "
                            f"Session expired after Select clicks "
                            f"(attempt {_page_attempt + 1}) for {url}"
                        )
                        if _page_attempt < 2:
                            browser, context, page, _ = restart_browser(
                                pw, browser, state_name
                            )
                            if _re_navigate(
                                page, context, tracking_number,
                                is_alpha, url, state_name,
                            ):
                                continue
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: "
                            f"Session expired after Select clicks, "
                            f"giving up for {url}"
                        )
                        break

                    # Step 4: Download ZIP via Playwright's expect_download()
                    zip_file_path = download_zip(page, download_path)
                    if zip_file_path:
                        break  # success

                    # Download failed — retry with fresh browser
                    if _page_attempt < 2:
                        print(
                            f"[WARN] PID {os.getpid()} row {idx}: "
                            f"Download failed (attempt {_page_attempt + 1}) "
                            f"for {url}, restarting browser and retrying..."
                        )
                        browser, context, page, _ = restart_browser(
                            pw, browser, state_name
                        )
                        if _re_navigate(
                            page, context, tracking_number,
                            is_alpha, url, state_name,
                        ):
                            continue
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: "
                            f"Re-navigation failed for {url}"
                        )
                        break
                    else:
                        print(
                            f"[SKIP] PID {os.getpid()} row {idx}: "
                            f"Download failed after {_page_attempt + 1} "
                            f"attempts for {url}, skipping"
                        )
                        break

                if not zip_file_path:
                    continue

                # Step 5: Extract ZIP (handle Windows-style backslash paths)
                extract_dir = os.path.join(download_path, f"extracted_{idx}")
                try:
                    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                        for member in zip_ref.namelist():
                            normalized_path = member.replace("\\", "/")
                            target_path = os.path.join(
                                extract_dir, normalized_path
                            )

                            parent_dir = os.path.dirname(target_path)
                            if parent_dir:
                                os.makedirs(parent_dir, exist_ok=True)

                            if normalized_path.endswith("/"):
                                continue

                            with zip_ref.open(member) as src, open(
                                target_path, "wb"
                            ) as dst:
                                dst.write(src.read())
                except Exception as e:
                    print(
                        f"[ERROR] PID {os.getpid()} row {idx}: "
                        f"Failed to extract ZIP for {url}: "
                        f"{str(e).splitlines()[0]}"
                    )
                    try:
                        os.remove(zip_file_path)
                    except Exception:
                        pass
                    continue

                # Find the root folder inside extracted ZIP
                extracted_contents = os.listdir(extract_dir)
                root_folder = extract_dir

                if len(extracted_contents) == 1 and os.path.isdir(
                    os.path.join(extract_dir, extracted_contents[0])
                ):
                    root_folder = os.path.join(
                        extract_dir, extracted_contents[0]
                    )

                # Step 6: Process subfolders and create columns
                for subfolder in os.listdir(root_folder):
                    subfolder_path = os.path.join(root_folder, subfolder)

                    if subfolder in IGNORE_FOLDERS or not os.path.isdir(
                        subfolder_path
                    ):
                        continue

                    normalized = (
                        subfolder.lower()
                        .replace("-", "_")
                        .replace(" ", "_")
                    )
                    col_name = FOLDER_TO_COLUMN.get(normalized, normalized)

                    if col_name not in df_state.columns:
                        df_state[col_name] = None

                    file_info = process_subfolder_files(subfolder_path)

                    if file_info:
                        df_state.at[idx, col_name] = str(file_info)

                # Step 7: Extract TOC form names from root PDF
                for item in os.listdir(root_folder):
                    item_path = os.path.join(root_folder, item)
                    if item.endswith(".pdf") and os.path.isfile(item_path):
                        toc_mapping = extract_toc_form_names(item_path)
                        if toc_mapping:
                            df_state.at[idx, "toc_form_names"] = str(
                                toc_mapping
                            )
                        break  # Only process the first (root) PDF

                # Step 8: Extract readability text
                readability_texts = extract_readability_text(
                    root_folder, IGNORE_FOLDERS
                )
                if readability_texts:
                    df_state.at[idx, "readability_text"] = str(
                        readability_texts
                    )

                # Step 9: Cleanup — delete extracted folder and ZIP
                try:
                    shutil.rmtree(extract_dir, ignore_errors=True)
                except Exception:
                    pass

                try:
                    if zip_file_path and os.path.exists(zip_file_path):
                        os.remove(zip_file_path)
                except Exception:
                    pass

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
                # Increment the total-progress counter
                if _shared_counter is not None:
                    with _shared_counter.get_lock():
                        _shared_counter.value += 1
                # Throttle: random delay between rows
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
        # Clean up the download directory for this process
        try:
            if os.path.exists(download_path):
                shutil.rmtree(download_path, ignore_errors=True)
            proc_folder = os.path.dirname(download_path)
            if os.path.exists(proc_folder) and not os.listdir(proc_folder):
                os.rmdir(proc_folder)
        except Exception as e:
            print(
                f"[ERROR] Failed to delete download directory "
                f"{download_path}: {str(e)}"
            )
        # AWS: final cleanup of stale Chromium temp dirs
        cleanup_tmp_chromium_dirs(max_age_hours=0)

    # Save final results for this work unit
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

    EXTRACTION_COLS = [
        "form_attachments",
        "supporting_document_attachments",
        "rate_rule_attachments",
        "correspondence_attachments",
        "toc_form_names",
        "readability_text",
    ]

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

    # Filter out rows with alphanumeric filing IDs (handle separately later)
    alpha_mask = df["page_url"].apply(has_alpha_filing_id)
    alpha_count = alpha_mask.sum()
    if alpha_count > 0:
        df = df[~alpha_mask].reset_index(drop=True)
        print(
            f"[FILTER] Skipped {alpha_count} rows with alpha filing IDs, "
            f"{len(df)} remaining"
        )

    if len(df) == 0:
        print(
            "[RESUME] All rows already processed! Combining previous results..."
        )
        final_df = pd.concat(prev_result_dfs, ignore_index=True)
        final_df.to_csv("form_names_readability_text.csv", index=False)
        print(
            f"\nResults saved to form_names_readability_text.csv "
            f"({len(final_df)} rows)"
        )
        exit(0)

    n_proc = N_PROC

    # Group data by state — one work unit per state, largest first
    work_units = build_work_units(df)

    # ------------------------------------------------------------------
    # Each core gets one state at a time. Largest states start first.
    # When a core finishes a state, it picks up the next unassigned
    # state automatically (via imap_unordered).
    # ------------------------------------------------------------------
    print(f"Processing {len(work_units)} states with {n_proc} processes")
    print(
        f"Total URLs to process: {len(df)} "
        f"(skipped {original_len - len(df)} already done)"
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

    os.makedirs("outputs", exist_ok=True)

    with Pool(
        actual_procs,
        initializer=_init_worker,
        initargs=(total_counter, failed_states),
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

    # Deduplicate by serf_num — keep the last (most recent) entry
    final_df = final_df.drop_duplicates(subset="serf_num", keep="last")

    final_df.to_csv("form_names_readability_text.csv", index=False)

    # AWS: final cleanup
    cleanup_tmp_chromium_dirs(max_age_hours=0)

    print(
        f"\nAll done. Results saved to form_names_readability_text.csv "
        f"({len(final_df)} rows)"
        f"\n   ({len(prev_result_dfs)} previously processed + "
        f"{len(all_state_csvs)} new states merged)"
    )
