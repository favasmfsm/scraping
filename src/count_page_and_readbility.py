import pandas as pd
import numpy as np
import os
import time
import shutil
import tempfile
from urllib.parse import unquote
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import fitz  # PyMuPDF

from zip_utils import (
    extract_toc_form_names,
    extract_readability_text,
    process_subfolder_files,
)

BASE_DIR = "downloads"


def cleanup_chrome_cache(current_user_data_dir=None):
    """
    Clean up Chrome cache directories from various locations.

    Args:
        current_user_data_dir: Path to the current Chrome user data directory to preserve
    """
    cleaned_count = 0
    cleaned_size = 0

    # Locations where Chrome cache might be stored
    cache_locations = [
        "/tmp",  # Direct temp directories
        "/tmp/snap-private-tmp/snap.chromium/tmp",  # Snap Chrome cache location
    ]

    for base_dir in cache_locations:
        if not os.path.exists(base_dir):
            continue

        try:
            # Look for chrome_cache_* directories
            for item in os.listdir(base_dir):
                item_path = os.path.join(base_dir, item)

                # Skip if this is the current user data directory
                if current_user_data_dir and item_path == current_user_data_dir:
                    continue

                # Check if it's a Chrome cache directory
                if item.startswith("chrome_cache_") and os.path.isdir(item_path):
                    try:
                        # Calculate size before deletion
                        try:
                            size = sum(
                                os.path.getsize(os.path.join(dirpath, filename))
                                for dirpath, dirnames, filenames in os.walk(item_path)
                                for filename in filenames
                            )
                            cleaned_size += size
                        except:
                            pass

                        shutil.rmtree(item_path, ignore_errors=True)
                        cleaned_count += 1
                        print(f"[CLEANUP] Removed Chrome cache directory: {item_path}")
                    except Exception as e:
                        print(f"[CLEANUP] Failed to remove {item_path}: {str(e)}")
        except Exception as e:
            print(f"[CLEANUP] Error accessing {base_dir}: {str(e)}")

    if cleaned_count > 0:
        size_mb = cleaned_size / (1024 * 1024)
        print(
            f"[CLEANUP] Cleaned {cleaned_count} cache directories (~{size_mb:.1f} MB)"
        )


BASE_DIR = "downloads"


def process_state(state_data):
    """
    Process all rows for a single state chunk and save progress immediately.
    state_data is a tuple: (state_name, df_for_state, chunk_idx, num_chunks)
    """
    state_name, df_state, chunk_idx, num_chunks = state_data

    # Temporary Chrome user data directory for isolated cache
    user_data_dir = tempfile.mkdtemp(prefix="chrome_cache_")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-data-dir={user_data_dir}")  # isolate cache
    options.add_experimental_option(
        "prefs",
        {
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        },
    )
    chrome_binary = os.environ.get("CHROME_BINARY")
    if chrome_binary:
        options.binary_location = chrome_binary

    # Use a pre-downloaded ChromeDriver path if provided to avoid race conditions
    driver_path = os.environ.get("CHROMEDRIVER_PATH")
    if driver_path and os.path.isfile(driver_path) and os.access(driver_path, os.X_OK):
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        # Use webdriver-manager to automatically download compatible ChromeDriver
        # This will download the correct version matching your Chrome browser
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    df_state = df_state.copy()
    # Initialize new columns for ZIP-based extraction
    df_state["form_attachments"] = [None] * len(df_state)
    df_state["supporting_document_attachments"] = [None] * len(df_state)
    df_state["rate_rule_attachments"] = [None] * len(df_state)
    df_state["correspondence_attachments"] = [None] * len(df_state)
    df_state["toc_form_names"] = [None] * len(df_state)
    df_state["readability_text"] = [None] * len(df_state)

    # Create a single download directory per process (reused for all URLs)
    download_path = os.path.join(BASE_DIR, f"proc_{os.getpid()}", state_name)
    download_path = os.path.abspath(download_path)
    os.makedirs(download_path, exist_ok=True)
    print(f"[PATH] Download directory: {download_path}")
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": download_path},
    )

    try:
        # Authenticate once for this state
        auth_url = f"https://filingaccess.serff.com/sfa/home/{state_name}"
        driver.get(auth_url)
        try:
            begin = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//a[contains(@href, 'userAgreement.xhtml') and normalize-space()='Begin Search']",
                    )
                )
            )
            begin.click()
            accept = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//span[normalize-space()='Accept']")
                )
            )
            accept.click()
            # Wait for session to be established after accepting agreement
            time.sleep(2)
        except Exception as e:
            pass

        # Folders to ignore when processing ZIP contents
        IGNORE_FOLDERS = {"User Usage Agreement Attachments"}

        # Column mapping: subfolder name -> DataFrame column name
        FOLDER_TO_COLUMN = {
            "Form Attachments": "form_attachments",
            "Supporting Document Attachments": "supporting_document_attachments",
            "Rate Rule Attachments": "rate_rule_attachments",
            "Correspondence Attachments": "correspondence_attachments",
        }

        # Process all URLs for this state chunk
        for row in tqdm(
            df_state.itertuples(),
            total=len(df_state),
            desc=f"State: {state_name} Chunk {chunk_idx+1}/{num_chunks} (PID {os.getpid()})",
        ):
            idx = row.Index
            url = row.page_url

            # Navigate and wait for page load
            driver.get(url)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row"))
                )
                # Additional wait for dynamic content to load
                time.sleep(2)
            except Exception as e:
                print(f"[WARN] Page load timeout for {url}")
                continue

            # Step 2: Click "Select Current Version Only" buttons
            select_current_buttons = [
                "formAttachmentSelectCurrentButton",
                "rateRuleAttachmentSelectCurrentButton",
                "supportingDocumentAttachmentSelectCurrentButton",
            ]
            for btn_id in select_current_buttons:
                try:
                    btn = driver.find_element(By.ID, btn_id)
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.3)
                except Exception:
                    pass  # Button may not exist if panel is empty

            # For Correspondence, click "Select All" (no "Select Current Version Only" button)
            try:
                corr_btn = driver.find_element(
                    By.ID, "correspondenceAttachmentSelectAllButton"
                )
                driver.execute_script("arguments[0].click();", corr_btn)
                time.sleep(0.3)
            except Exception:
                pass

            # Step 3: Click "Download Zip File" button
            time.sleep(0.5)  # Wait for checkboxes to be checked

            # Snapshot files before download
            try:
                before_files = set(os.listdir(download_path))
            except Exception:
                before_files = set()

            try:
                download_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "summaryForm:downloadLink"))
                )
                driver.execute_script("arguments[0].click();", download_btn)
            except Exception as e:
                print(f"[WARN] Could not click download button for {url}: {e}")
                continue

            # Step 4: Wait for ZIP download
            max_wait = 60
            wait_interval = 0.5
            waited = 0.0
            zip_file_path = None

            while waited < max_wait:
                try:
                    current_files = set(os.listdir(download_path))
                except Exception:
                    current_files = set()

                new_files = current_files - before_files
                # Look for completed .zip files (not .crdownload)
                zip_files = [
                    f
                    for f in new_files
                    if f.endswith(".zip")
                    and not os.path.exists(
                        os.path.join(download_path, f + ".crdownload")
                    )
                ]

                if zip_files:
                    zip_file_path = os.path.join(download_path, zip_files[0])
                    break

                time.sleep(wait_interval)
                waited += wait_interval

            if not zip_file_path:
                print(f"[WARN] ZIP download timeout for {url}")
                continue

            # Step 5: Extract ZIP (handle Windows-style backslash paths)
            extract_dir = os.path.join(download_path, f"extracted_{idx}")
            try:
                with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                    for member in zip_ref.namelist():
                        # Convert Windows backslashes to forward slashes for proper extraction
                        normalized_path = member.replace("\\", "/")
                        target_path = os.path.join(extract_dir, normalized_path)

                        # Create parent directories if needed
                        parent_dir = os.path.dirname(target_path)
                        if parent_dir:
                            os.makedirs(parent_dir, exist_ok=True)

                        # Skip directory entries (end with /)
                        if normalized_path.endswith("/"):
                            continue

                        # Extract file content
                        with zip_ref.open(member) as src, open(
                            target_path, "wb"
                        ) as dst:
                            dst.write(src.read())
            except Exception as e:
                print(f"[ERROR] Failed to extract ZIP for {url}: {e}")
                # Cleanup ZIP file
                try:
                    os.remove(zip_file_path)
                except Exception:
                    pass
                continue

            # Find the root folder inside extracted ZIP (usually matches tracking number)
            extracted_contents = os.listdir(extract_dir)
            root_folder = extract_dir

            # If there's a single folder inside, that's the root
            if len(extracted_contents) == 1 and os.path.isdir(
                os.path.join(extract_dir, extracted_contents[0])
            ):
                root_folder = os.path.join(extract_dir, extracted_contents[0])

            # Step 6: Process subfolders and create columns
            for subfolder in os.listdir(root_folder):
                subfolder_path = os.path.join(root_folder, subfolder)

                # Skip ignored folders and files
                if subfolder in IGNORE_FOLDERS or not os.path.isdir(subfolder_path):
                    continue

                # Get column name from mapping, or create sanitized name
                if subfolder in FOLDER_TO_COLUMN:
                    col_name = FOLDER_TO_COLUMN[subfolder]
                else:
                    col_name = subfolder.lower().replace(" ", "_")

                # Process files in subfolder
                file_info = process_subfolder_files(subfolder_path)

                if file_info:
                    df_state.at[idx, col_name] = file_info

            # Step 7: Extract TOC form names from root PDF (OPTIONAL)
            for item in os.listdir(root_folder):
                item_path = os.path.join(root_folder, item)
                if item.endswith(".pdf") and os.path.isfile(item_path):
                    toc_mapping = extract_toc_form_names(item_path)
                    if toc_mapping:
                        df_state.at[idx, "toc_form_names"] = toc_mapping
                    break  # Only process the first (root) PDF

            # Step 8: Extract readability text
            readability_texts = extract_readability_text(root_folder, IGNORE_FOLDERS)
            if readability_texts:
                df_state.at[idx, "readability_text"] = readability_texts

            # Step 9: Cleanup - delete extracted folder and ZIP
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception:
                pass

            try:
                if zip_file_path and os.path.exists(zip_file_path):
                    os.remove(zip_file_path)
            except Exception:
                pass

            # Checkpoint save every 10 rows
            if idx % 10 == 0 and idx > 0:
                checkpoint_file = f"outputs/temp_results_{state_name}_chunk{chunk_idx+1}_{os.getpid()}.csv"
                df_state.to_csv(checkpoint_file, index=False)
                # Clean up old Chrome cache directories on each checkpoint save
                cleanup_chrome_cache(current_user_data_dir=user_data_dir)
    finally:
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)  # clean Chrome cache
        # Clean up any remaining Chrome cache directories
        cleanup_chrome_cache(current_user_data_dir=user_data_dir)
        # Clean up the single download directory for this process
        try:
            if os.path.exists(download_path):
                shutil.rmtree(download_path, ignore_errors=True)
                print(f"[DELETE] Deleted download directory: {download_path}")
            # Also clean up the parent proc_ folder if it's empty
            proc_folder = os.path.dirname(download_path)
            if os.path.exists(proc_folder) and not os.listdir(proc_folder):
                os.rmdir(proc_folder)
        except Exception as e:
            print(
                f"[ERROR] Failed to delete download directory {download_path}: {str(e)}"
            )

    # Save progress for this state chunk immediately
    # Sanitize state name for filename (remove special characters)
    safe_state_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in state_name
    )
    partial_file = (
        f"outputs/temp_results_{safe_state_name}_chunk{chunk_idx+1}_{os.getpid()}.csv"
    )
    df_state.to_csv(partial_file, index=False)
    return partial_file


if __name__ == "__main__":
    form_df = pd.read_csv("data/batch_1.csv")
    df = form_df.sample(20)

    # Group data by state
    states = df.groupby("state")
    state_groups = []
    chunk_size = 10

    # Split each state into chunks of 1000 rows
    for state_name, state_df in states:
        state_df = state_df.reset_index(drop=True)
        # Split into chunks of 1000 rows
        num_chunks = (len(state_df) + chunk_size - 1) // chunk_size  # Ceiling division
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min((chunk_idx + 1) * chunk_size, len(state_df))
            chunk_df = state_df.iloc[start_idx:end_idx].reset_index(drop=True)
            state_groups.append((state_name, chunk_df, chunk_idx, num_chunks))

    # Sort by number of rows (largest first)
    state_groups.sort(key=lambda x: len(x[1]), reverse=True)

    # Prefer system chromedriver on ARM64 (avoids wrong-arch downloads)
    for system_path in (
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
    ):
        if os.path.exists(system_path):
            os.environ["CHROMEDRIVER_PATH"] = system_path
            break

    n_proc = min(cpu_count(), 7, len(state_groups))  # Don't exceed number of states

    with Pool(n_proc) as pool:
        partial_files = list(pool.map(process_state, state_groups))

    # Combine results from all states
    result_dfs = [pd.read_csv(f) for f in partial_files]
    final_df = pd.concat(result_dfs, ignore_index=True)
    final_df.to_csv("form_names_readability_text.csv", index=False)

    # Clean up temporary files
    for f in partial_files:
        try:
            os.remove(f)
        except Exception as e:
            pass

    print(
        f"\n✅ All states completed. Results saved to form_names_readability_text.csv ({len(final_df)} rows)"
    )
