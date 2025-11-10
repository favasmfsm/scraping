import pandas as pd
import numpy as np
import os
import time
import shutil
import tempfile
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from textstat import flesch_reading_ease
import PyPDF2
import fitz  # PyMuPDF

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
        print(f"[CLEANUP] Cleaned {cleaned_count} cache directories (~{size_mb:.1f} MB)")


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
        # Fallback to Selenium Manager (lets Selenium pick compatible driver/arch)
        driver = webdriver.Chrome(options=options)
    df_state = df_state.copy()
    df_state["form_name"] = [[] for _ in range(len(df_state))]
    df_state["submission_date"] = pd.NA
    df_state["flesch_reading_ease"] = [[] for _ in range(len(df_state))]

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
        except Exception as e:
            pass

        # Process all URLs for this state chunk
        for row in tqdm(
            df_state.itertuples(),
            total=len(df_state),
            desc=f"State: {state_name} Chunk {chunk_idx+1}/{num_chunks} (PID {os.getpid()})",
        ):
            idx = row.Index
            url = row.page_url

            # Navigate and extract
            driver.get(url)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row"))
                )
            except Exception as e:
                continue

            try:
                submission_label = driver.find_element(
                    By.XPATH, "//label[contains(text(), 'Submission Date')]"
                )
                submission_date = submission_label.find_element(
                    By.XPATH, "../div"
                ).text.strip()
            except Exception as e:
                submission_date = None
            df_state.at[idx, "submission_date"] = submission_date

            row_divs = driver.find_elements(By.CSS_SELECTOR, "div.row")
            for row_idx, row_div in enumerate(row_divs):
                try:
                    # Try to find form name element with better error handling
                    try:
                        form_name_elem = row_div.find_element(
                            By.CSS_SELECTOR, "div.col-lg-4.summaryScheduleItemData"
                        )
                        form_name = form_name_elem.text.strip()
                    except Exception as e:
                        continue

                    # Try to find download link
                    try:
                        link = row_div.find_element(
                            By.CSS_SELECTOR, "a[id*='downloadAttachment_']"
                        )
                    except Exception as e:
                        continue

                    driver.execute_script(
                        "arguments[0].removeAttribute('target');", link
                    )
                    # Snapshot existing PDFs before clicking
                    try:
                        before_pdfs = {
                            f
                            for f in os.listdir(download_path)
                            if f.lower().endswith(".pdf")
                        }
                    except Exception:
                        before_pdfs = set()

                    driver.execute_script("arguments[0].click();", link)
                    # Wait for a new PDF to appear (poll every 0.5s up to 15s)
                    max_wait = 15
                    wait_interval = 0.5
                    waited = 0.0
                    actual_file_path = None
                    while waited < max_wait and actual_file_path is None:
                        try:
                            current_files = os.listdir(download_path)
                        except Exception:
                            current_files = []
                        # Candidate PDFs excluding the ones seen before
                        candidate_pdfs = [
                            f
                            for f in current_files
                            if f.lower().endswith(".pdf") and f not in before_pdfs
                        ]
                        # Filter out any still-downloading files (.crdownload counterpart)
                        candidate_pdfs = [
                            f
                            for f in candidate_pdfs
                            if not os.path.exists(
                                os.path.join(download_path, f + ".crdownload")
                            )
                        ]
                        if candidate_pdfs:
                            # Pick the most recent by mtime
                            candidate_paths = [
                                os.path.join(download_path, f) for f in candidate_pdfs
                            ]
                            actual_file_path = max(
                                candidate_paths, key=lambda p: os.path.getmtime(p)
                            )
                            break
                        time.sleep(wait_interval)
                        waited += wait_interval

                    df_state.at[idx, "form_name"].append(form_name)

                    if not actual_file_path:
                        print(f"[PATH] File not found in: {download_path}")
                        # Investigate: list files in download directory
                        if os.path.exists(download_path):
                            files_in_dir = os.listdir(download_path)
                            if files_in_dir:
                                print(f"[PATH] Files in directory: {files_in_dir}")
                            else:
                                print(f"[PATH] Directory is empty")
                        else:
                            print(f"[PATH] Download directory does not exist")
                        flesch_score = None
                    else:
                        # Use the actual file path that was found
                        # Extract text using PyMuPDF
                        try:
                            text = ""
                            pdf = fitz.open(actual_file_path)
                            for page in pdf:
                                text += page.get_text() or ""
                            pdf.close()
                            flesch_score = flesch_reading_ease(text)
                        except Exception as e:
                            flesch_score = None
                            print(f"[ERROR] Failed to process PDF: {str(e)}")

                        # delete file
                        try:
                            if os.path.exists(actual_file_path):
                                os.remove(actual_file_path)

                        except Exception as e:

                            # Try to delete again after a short delay (file might be locked)
                            try:
                                time.sleep(0.5)
                                if os.path.exists(actual_file_path):
                                    os.remove(actual_file_path)
                                    print(
                                        f"[DELETE] Successfully deleted on retry: {actual_file_path}"
                                    )
                            except Exception as e2:
                                print(
                                    f"[ERROR] Failed to delete PDF on retry {actual_file_path}: {str(e2)}"
                                )

                    df_state.at[idx, "flesch_reading_ease"].append(flesch_score)
                except Exception as e:
                    continue

            if idx % 200 == 0 and idx > 0:
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
        except Exception as e:
            print(f"[ERROR] Failed to delete download directory {download_path}: {str(e)}")

    # Save progress for this state chunk immediately
    # Sanitize state name for filename (remove special characters)
    safe_state_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in state_name
    )
    partial_file = f"outputs/temp_results_{safe_state_name}_chunk{chunk_idx+1}_{os.getpid()}.csv"
    df_state.to_csv(partial_file, index=False)
    return partial_file


if __name__ == "__main__":
    form_df = pd.read_csv("data/form_data_full.csv")
    to_extract = pd.read_csv("data/to_extract_v2.csv")
    df = form_df[form_df.serf_num.isin(to_extract.serf_num)]

    # Group data by state
    states = df.groupby("state")
    state_groups = []
    chunk_size = 1000
    
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
    for system_path in ("/usr/bin/chromedriver", "/usr/lib/chromium-browser/chromedriver"):
        if os.path.exists(system_path):
            os.environ["CHROMEDRIVER_PATH"] = system_path
            break

    n_proc = min(cpu_count(), 7, len(state_groups))  # Don't exceed number of states

    with Pool(n_proc) as pool:
        partial_files = list(pool.map(process_state, state_groups))

    # Combine results from all states
    result_dfs = [pd.read_csv(f) for f in partial_files]
    final_df = pd.concat(result_dfs, ignore_index=True)
    final_df.to_csv("form_names_submission_date.csv", index=False)

    # Clean up temporary files
    for f in partial_files:
        try:
            os.remove(f)
        except Exception as e:
            pass

    print(
        f"\n✅ All states completed. Results saved to form_names_submission_date.csv ({len(final_df)} rows)"
    )
