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


def process_state(state_data):
    """
    Process all rows for a single state chunk and save progress immediately.
    state_data is a tuple: (state_name, df_for_state, chunk_idx, total_chunks)
    """
    state_name, df_state, chunk_idx, total_chunks = state_data

    # Temporary Chrome user data directory for isolated cache
    user_data_dir = tempfile.mkdtemp(prefix="chrome_cache_")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-data-dir={user_data_dir}")  # isolate cache
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
        except:
            pass

        # Process all URLs for this state chunk
        desc = f"State: {state_name}"
        if total_chunks > 1:
            desc += f" chunk {chunk_idx+1}/{total_chunks}"
        desc += f" (PID {os.getpid()})"
        
        for idx, row in tqdm(
            df_state.iterrows(),
            total=len(df_state),
            desc=desc,
        ):
            url = row["page_url"]

            # Navigate and extract
            driver.get(url)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row"))
                )
            except:
                continue

            try:
                submission_label = driver.find_element(
                    By.XPATH, "//label[contains(text(), 'Submission Date')]"
                )
                submission_date = submission_label.find_element(
                    By.XPATH, "../div"
                ).text.strip()
            except:
                submission_date = None
            df_state.at[idx, "submission_date"] = submission_date

            for row_div in driver.find_elements(By.CSS_SELECTOR, "div.row"):
                try:
                    form_name = row_div.find_element(
                        By.CSS_SELECTOR, "div.col-lg-4.summaryScheduleItemData"
                    ).text.strip()
                    df_state.at[idx, "form_name"].append(form_name)
                except:
                    continue
    finally:
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)  # clean Chrome cache

    # Save progress for this state chunk immediately
    # Sanitize state name for filename (remove special characters)
    safe_state_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in state_name
    )
    if total_chunks > 1:
        partial_file = f"temp_results_{safe_state_name}_chunk{chunk_idx+1}of{total_chunks}_{os.getpid()}.csv"
        print(f"✅ State '{state_name}' chunk {chunk_idx+1}/{total_chunks} completed and saved to {partial_file}")
    else:
        partial_file = f"temp_results_{safe_state_name}_{os.getpid()}.csv"
        print(f"✅ State '{state_name}' completed and saved to {partial_file}")
    df_state.to_csv(partial_file, index=False)
    return partial_file


if __name__ == "__main__":
    form_df = pd.read_csv("data/to_fetch_states.csv")
    df = form_df.copy()

    

    # Group data by state
    states = df.groupby("state")
    state_groups_raw = [
        (state_name, state_df.reset_index(drop=True)) for state_name, state_df in states
    ]

    print(f"Found {len(state_groups_raw)} unique states to process")
    for state_name, state_df in state_groups_raw:
        print(f"  - {state_name}: {len(state_df)} rows")
    
    # Find the smallest state size (chunk size)
    min_size = min(len(state_df) for _, state_df in state_groups_raw)
    print(f"\nSmallest state size: {min_size} rows (using as chunk size)")
    
    # Split states into chunks based on the smallest size
    state_groups = []
    for state_name, state_df in state_groups_raw:
        state_size = len(state_df)
        num_chunks = (state_size + min_size - 1) // min_size  # Ceiling division
        
        if num_chunks == 1:
            # State fits in one chunk
            state_groups.append((state_name, state_df, 0, 1))
        else:
            # Split state into multiple chunks
            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * min_size
                end_idx = min((chunk_idx + 1) * min_size, state_size)
                chunk_df = state_df.iloc[start_idx:end_idx].reset_index(drop=True)
                state_groups.append((state_name, chunk_df, chunk_idx, num_chunks))
            print(f"  - {state_name}: split into {num_chunks} chunks")
    
    print(f"\nTotal chunks to process: {len(state_groups)}")

    # For M1 MacBook Air: Each Chrome process uses ~1-1.5GB RAM
    # M1 has 8 CPU cores, but memory is the limiting factor
    # Safe values: 8GB RAM -> 2-3 processes, 16GB RAM -> 4-6 processes
    # Using 4 as a safe default for M1 MacBook Air (works for both 8GB and 16GB models)
    # You can increase to 6 if you have 16GB RAM and want faster processing
    n_proc = min(cpu_count(), 8, len(state_groups))  # Don't exceed number of chunks
    # For 16GB M1 MacBook Air, you can safely use: n_proc = min(cpu_count(), 6, len(state_groups))

    # Prefer system chromedriver on ARM64 (avoids wrong-arch downloads)
    for system_path in ("/usr/bin/chromedriver", "/usr/lib/chromium-browser/chromedriver"):
        if os.path.exists(system_path):
            os.environ["CHROMEDRIVER_PATH"] = system_path
            break

    print(f"\nUsing {n_proc} processes (CPU cores available: {cpu_count()})")
    print(f"Each process will handle one chunk and save progress immediately.\n")
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
        except:
            pass

    print("\n✅ All states completed. Extracted form names and submission dates.")
    print(
        f"✅ Final results saved to form_names_submission_date.csv ({len(final_df)} rows)"
    )
