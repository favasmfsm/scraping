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
from webdriver_manager.chrome import ChromeDriverManager
from textstat import flesch_reading_ease
import PyPDF2

BASE_DIR = "downloads"


def process_state(state_data):
    """
    Process all rows for a single state and save progress immediately.
    state_data is a tuple: (state_name, df_for_state)
    """
    state_name, df_state = state_data

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

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    df_state = df_state.copy()
    df_state["form_name"] = [[] for _ in range(len(df_state))]
    df_state["submission_date"] = pd.NA
    df_state["flesch_reading_ease"] = [[] for _ in range(len(df_state))]

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

        # Process all URLs for this state
        for idx, row in tqdm(
            df_state.iterrows(),
            total=len(df_state),
            desc=f"State: {state_name} (PID {os.getpid()})",
        ):
            url = row["page_url"]
            # Dynamic download path - use absolute path
            folder_name = url.split("=")[-1].strip()
            download_path = os.path.join(
                BASE_DIR, f"proc_{os.getpid()}", state_name, folder_name
            )
            # Convert to absolute path
            download_path = os.path.abspath(download_path)
            print(f"[PATH] Download directory: {download_path}")
            os.makedirs(download_path, exist_ok=True)
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": download_path},
            )

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
                        # Extract text
                        try:
                            with open(actual_file_path, "rb") as f:
                                reader = PyPDF2.PdfReader(f)
                                text = " ".join(
                                    page.extract_text() for page in reader.pages
                                )
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

            if idx % 5 == 0:
                checkpoint_file = f"outputs/temp_results_{state_name}_{os.getpid()}.csv"
                df_state.to_csv(checkpoint_file, index=False)
    finally:
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)  # clean Chrome cache

    # Save progress for this state immediately
    # Sanitize state name for filename (remove special characters)
    safe_state_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in state_name
    )
    partial_file = f"outputs/temp_results_{safe_state_name}_{os.getpid()}.csv"
    df_state.to_csv(partial_file, index=False)
    return partial_file


if __name__ == "__main__":
    form_df = pd.read_csv("data/to_fetch_states.csv")
    df = form_df.copy()

    # Group data by state
    states = df.groupby("state")
    state_groups = [
        (state_name, state_df.reset_index(drop=True)) for state_name, state_df in states
    ]

    # For M1 MacBook Air: Each Chrome process uses ~1-1.5GB RAM
    # M1 has 8 CPU cores, but memory is the limiting factor
    # Safe values: 8GB RAM -> 2-3 processes, 16GB RAM -> 4-6 processes
    # Using 4 as a safe default for M1 MacBook Air (works for both 8GB and 16GB models)
    # You can increase to 6 if you have 16GB RAM and want faster processing
    n_proc = min(cpu_count(), 2, len(state_groups))  # Don't exceed number of states
    # For 16GB M1 MacBook Air, you can safely use: n_proc = min(cpu_count(), 6, len(state_groups))

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
