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

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
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

        # Process all URLs for this state
        for idx, row in tqdm(
            df_state.iterrows(),
            total=len(df_state),
            desc=f"State: {state_name} (PID {os.getpid()})",
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

    # Save progress for this state immediately
    # Sanitize state name for filename (remove special characters)
    safe_state_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_" for c in state_name
    )
    partial_file = f"temp_results_{safe_state_name}_{os.getpid()}.csv"
    df_state.to_csv(partial_file, index=False)
    print(f"✅ State '{state_name}' completed and saved to {partial_file}")
    return partial_file


if __name__ == "__main__":
    form_df = pd.read_csv("../data/to_fetch_states.csv")
    df = form_df.copy()

    # Group data by state
    states = df.groupby("state")
    state_groups = [
        (state_name, state_df.reset_index(drop=True)) for state_name, state_df in states
    ]

    print(f"Found {len(state_groups)} unique states to process")
    for state_name, state_df in state_groups:
        print(f"  - {state_name}: {len(state_df)} rows")

    # For M1 MacBook Air: Each Chrome process uses ~1-1.5GB RAM
    # M1 has 8 CPU cores, but memory is the limiting factor
    # Safe values: 8GB RAM -> 2-3 processes, 16GB RAM -> 4-6 processes
    # Using 4 as a safe default for M1 MacBook Air (works for both 8GB and 16GB models)
    # You can increase to 6 if you have 16GB RAM and want faster processing
    n_proc = min(cpu_count(), 6, len(state_groups))  # Don't exceed number of states
    # For 16GB M1 MacBook Air, you can safely use: n_proc = min(cpu_count(), 6, len(state_groups))

    print(f"\nUsing {n_proc} processes (CPU cores available: {cpu_count()})")
    print(f"Each process will handle one state and save progress immediately.\n")
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
