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

BASE_DIR = "/Users/favasm/Library/CloudStorage/GoogleDrive-favasm@annolive.com/Shared drives/MJ/scraping/downloads"

def process_chunk(df_chunk):
    # --- Temporary Chrome user data directory for clean cache ---
    user_data_dir = tempfile.mkdtemp(prefix="chrome_cache_")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-data-dir={user_data_dir}")  # isolate cache
    options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    })

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    df_chunk = df_chunk.copy()
    df_chunk['attachments'] = [[] for _ in range(len(df_chunk))]
    df_chunk['form_name'] = [[] for _ in range(len(df_chunk))]
    df_chunk['submission_date'] = pd.NA
    state_old = ""

    try:
        for idx, row in tqdm(df_chunk.iterrows(), total=len(df_chunk), desc=f"PID {os.getpid()}"):
            url = row['page_url']
            folder_name = url.split("=")[-1].strip()
            state = row['state']

            # Reauthenticate when state changes
            if state != state_old:
                auth_url = f"https://filingaccess.serff.com/sfa/home/{state}"
                print(auth_url)
                driver.get(auth_url)
                try:
                    begin = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, 'userAgreement.xhtml') and normalize-space()='Begin Search']"))
                    )
                    begin.click()
                    accept = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[normalize-space()='Accept']"))
                    )
                    accept.click()
                    time.sleep(2)
                except:
                    pass
                state_old = state

            # Dynamic download path
            # download_path = os.path.join(BASE_DIR, state, folder_name)
            # os.makedirs(download_path, exist_ok=True)
            # driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            #     "behavior": "allow",
            #     "downloadPath": download_path
            # })

            # Navigate and extract
            driver.get(url)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.row"))
                )
            except:
                continue

            try:
                submission_label = driver.find_element(By.XPATH, "//label[contains(text(), 'Submission Date')]")
                submission_date = submission_label.find_element(By.XPATH, "../div").text.strip()
            except:
                submission_date = None
            df_chunk.at[idx, 'submission_date'] = submission_date

            for row_div in driver.find_elements(By.CSS_SELECTOR, "div.row"):
                try:
                    form_name = row_div.find_element(By.CSS_SELECTOR, "div.col-lg-4.summaryScheduleItemData").text.strip()
                    # link = row_div.find_element(By.CSS_SELECTOR, "a[id*='downloadAttachment_']")
                    # driver.execute_script("arguments[0].removeAttribute('target');", link)
                    # driver.execute_script("arguments[0].click();", link)
                    # time.sleep(2)
                    # df_chunk.at[idx, 'attachments'].append(form_name)
                    df_chunk.at[idx, 'form_name'].append(form_name)
                except:
                    continue
            time.sleep(2)
    finally:
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)  # clean Chrome cache

    # Save partial progress for this chunk
    partial_file = f"temp_results_{os.getpid()}.csv"
    df_chunk.to_csv(partial_file, index=False)
    return partial_file


if __name__ == "__main__":
    form_df = pd.read_csv("form_data.csv")
    df = form_df.copy()
    n_proc = min(cpu_count(), 4)
    chunks = np.array_split(df, n_proc)

    with Pool(n_proc) as pool:
        partial_files = pool.map(process_chunk, chunks)

    # Combine results
    result_dfs = [pd.read_csv(f) for f in partial_files]
    final_df = pd.concat(result_dfs, ignore_index=True)
    final_df.to_csv("downloaded_parallel_clean.csv", index=False)
    print("✅ All processes done with cache cleared each cycle.")