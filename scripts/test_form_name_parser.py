#!/usr/bin/env python3
"""
Test script for form-name-from-filename parsing.

Randomly samples N rows from data/final_batch_22_feb.csv, fetches each
filing summary page (HTTP or Playwright), saves HTML for inspection,
runs scrape_attachment_mappings_html(), and compares mapping keys to
the row's file_name list.

Usage (from repo root, with venv activated):
  python scripts/test_form_name_parser.py --size 3 [--seed 42]
  python scripts/test_form_name_parser.py --size 20 [--seed 123]
  python scripts/test_form_name_parser.py --dry-run --size 5   # no browser, just show sample

Note: Full run requires Playwright (Firefox) and network; run from your terminal if the
IDE sandbox cannot launch the browser.

Outputs:
  - outputs/debug_serff_<state>_<filing_id>.html (one per row)
  - Console: per-row result and final summary
"""

import argparse
import ast
import os
import re
import random
import time
import sys

# Run from repo root; ensure src is on path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import pandas as pd

from file_name_form_name_finder import (
    has_alpha_filing_id,
    scrape_attachment_mappings_html,
)
from browser_utils import (
    create_browser,
    authenticate,
    navigate_via_search,
    is_server_error,
    create_http_session,
    fetch_page,
    human_delay,
)


def parse_file_name(raw):
    """Parse file_name column into a list of strings. Handles CSV string that looks like a list."""
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


def filing_id_from_url(url):
    """Extract a short filing id from page_url for use in debug filenames."""
    m = re.search(r"filingId=([^&]+)", url)
    return m.group(1).replace("/", "_")[:40] if m else "unknown"


def main():
    parser = argparse.ArgumentParser(description="Test form name parser on sampled SERFF rows")
    parser.add_argument("--size", type=int, default=3, help="Number of rows to sample (default 3)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--csv", type=str, default="data/final_batch_22_feb.csv", help="Path to CSV")
    parser.add_argument("--out-dir", type=str, default="outputs", help="Directory for debug HTML")
    parser.add_argument("--dry-run", action="store_true", help="Only sample and print rows; do not fetch or parse")
    args = parser.parse_args()

    csv_path = os.path.join(REPO_ROOT, args.csv)
    out_dir = os.path.join(REPO_ROOT, args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    if not os.path.isfile(csv_path):
        print(f"[ERROR] CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    if len(df) == 0:
        print("[ERROR] CSV is empty")
        sys.exit(1)

    # Column names: SERFF Tracking Number, state, page_url, auth_url, file_name
    tracking_col = "SERFF Tracking Number"
    state_col = "state"
    url_col = "page_url"
    file_col = "file_name"
    if tracking_col not in df.columns or state_col not in df.columns or url_col not in df.columns or file_col not in df.columns:
        print(f"[ERROR] Expected columns: {tracking_col}, {state_col}, {url_col}, {file_col}")
        sys.exit(1)

    n = min(args.size, len(df))
    if args.seed is not None:
        random.seed(args.seed)
        df = df.sample(n=n, random_state=args.seed).reset_index(drop=True)
    else:
        df = df.sample(n=n).reset_index(drop=True)

    print(f"Sampled {n} rows from {csv_path}")
    if args.dry_run:
        print("DRY RUN: no browser, no fetch, no parser.")
        for idx, row in df.iterrows():
            tracking = str(row[tracking_col]).strip()
            state_name = str(row[state_col]).strip()
            url = str(row[url_col]).strip()
            expected_files = parse_file_name(row[file_col])
            is_alpha = has_alpha_filing_id(url)
            print(f"  [{idx+1}] {tracking} | {state_name} | alpha={is_alpha} | files={len(expected_files)}")
        print("Done (dry run).")
        return 0
    print(f"Debug HTML will be saved to {out_dir}/")
    print()

    pw = browser = context = page = None
    http_session = None
    current_state = None

    results = []

    try:
        for idx, row in df.iterrows():
            tracking = str(row[tracking_col]).strip()
            state_name = str(row[state_col]).strip()
            url = str(row[url_col]).strip()
            expected_files = parse_file_name(row[file_col])

            print(f"[{idx+1}/{n}] {tracking} ({state_name}) — {len(expected_files)} files expected")

            # Auth when state changes
            if state_name != current_state:
                if pw is not None:
                    try:
                        browser.close()
                        pw.stop()
                    except Exception:
                        pass
                    pw = browser = context = page = None
                    http_session = None

                pw = create_browser()
                pw, browser, context, page = pw[0], pw[1], pw[2], pw[3]
                if not authenticate(page, state_name):
                    print(f"  SKIP: Auth failed for {state_name}")
                    results.append({"tracking": tracking, "state": state_name, "ok": False, "reason": "auth_failed", "expected": expected_files, "mapping_keys": [], "missing": expected_files})
                    continue
                http_session = create_http_session(context)
                current_state = state_name
                human_delay(1.0, 2.0)

            html = None
            is_alpha = has_alpha_filing_id(url)

            if not is_alpha:
                page_html, expired, srv_err = fetch_page(http_session, url)
                if page_html and not expired and not srv_err and ("filingSummary" in url or "attachmentsContainer" in page_html):
                    html = page_html
            else:
                if navigate_via_search(page, context, tracking, state_name) and not is_server_error(page):
                    html = page.content()

            if html is None:
                print("  SKIP: Could not load page HTML")
                results.append({"tracking": tracking, "state": state_name, "ok": False, "reason": "no_html", "expected": expected_files, "mapping_keys": [], "missing": expected_files})
                human_delay(0.5, 1.5)
                continue

            # Save debug HTML
            fid = filing_id_from_url(url)
            safe_fid = "".join(c if c.isalnum() or c in "._-" else "_" for c in fid)
            html_path = os.path.join(out_dir, f"debug_serff_{state_name}_{safe_fid}.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  Saved: {os.path.basename(html_path)}")

            mapping = scrape_attachment_mappings_html(html)
            mapping_keys = list(mapping.keys()) if mapping else []

            missing = [f for f in expected_files if f not in mapping]
            found = [f for f in expected_files if f in mapping]
            ok = len(missing) == 0 and len(expected_files) > 0
            if len(expected_files) == 0:
                ok = len(mapping_keys) >= 0  # no expected files to check

            results.append({
                "tracking": tracking,
                "state": state_name,
                "ok": ok,
                "reason": "ok" if ok else "missing_files",
                "expected": expected_files,
                "mapping_keys": mapping_keys,
                "found": found,
                "missing": missing,
            })

            if missing:
                print(f"  Parser found {len(mapping_keys)} filenames; expected {len(expected_files)}. Missing: {missing[:5]}{'...' if len(missing) > 5 else ''}")
            else:
                print(f"  OK: all {len(expected_files)} expected files found in mapping ({len(mapping_keys)} total in page)")
            human_delay(0.8, 1.8)

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

    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    ok_count = sum(1 for r in results if r["ok"])
    no_html = sum(1 for r in results if r["reason"] == "no_html")
    auth_fail = sum(1 for r in results if r["reason"] == "auth_failed")
    missing = sum(1 for r in results if r["reason"] == "missing_files")
    print(f"Rows processed:     {len(results)}")
    print(f"All files matched:  {ok_count}")
    print(f"Missing some files: {missing}")
    print(f"No HTML loaded:     {no_html}")
    print(f"Auth failed:        {auth_fail}")

    if results:
        total_expected = sum(len(r["expected"]) for r in results)
        total_found = sum(len(r["found"]) for r in results)
        total_missing = sum(len(r["missing"]) for r in results)
        print(f"Total expected files: {total_expected}")
        print(f"Total found in mapping: {total_found}")
        print(f"Total missing: {total_missing}")
        if total_expected:
            pct = 100.0 * total_found / total_expected
            print(f"Match rate: {pct:.1f}%")

    print()
    if ok_count == len(results) and len(results) > 0:
        print("SUCCESS: All rows had all expected files in the parser output.")
    elif ok_count >= len(results) * 0.8:
        print("PARTIAL: Most rows OK; review missing files above.")
    else:
        print("CHECK: Many rows had missing files or no HTML — inspect debug HTML and parser.")
    return 0 if ok_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
