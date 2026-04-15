import argparse
import glob
import os
from datetime import datetime

import pandas as pd


DEFAULT_ZIP_BASE_DIR = (
    "/Users/favasm/Library/CloudStorage/GoogleDrive-favasm@greenberryai.com/"
    "Shared drives/MJ/scraping/downloads"
)


def _first_existing_column(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _zip_path(zip_base_dir, state_name, tracking_number):
    return os.path.join(zip_base_dir, str(state_name), str(tracking_number), "attachments.zip")


def _zip_size_if_valid(path):
    try:
        if os.path.exists(path):
            size = os.path.getsize(path)
            return size if size > 0 else None
    except OSError:
        return None
    return None


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Combine existing output CSVs and backfill zip_status columns based on "
            "attachments.zip existence (size > 0)."
        )
    )
    parser.add_argument(
        "--outputs-dir",
        default="outputs",
        help="Directory containing state_results_*.csv / temp_results_*.csv (default: outputs)",
    )
    parser.add_argument(
        "--zip-base-dir",
        default=DEFAULT_ZIP_BASE_DIR,
        help="Base directory where ZIPs are stored by state/tracking (default: project setting)",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help=(
            "Optional output CSV path. If omitted, writes to "
            "outputs/state_results_backfilled_zip_<timestamp>.csv"
        ),
    )
    args = parser.parse_args()

    pattern_state = os.path.join(args.outputs_dir, "state_results_*.csv")
    pattern_temp = os.path.join(args.outputs_dir, "temp_results_*.csv")
    files = sorted(glob.glob(pattern_state) + glob.glob(pattern_temp))

    if not files:
        print(f"[INFO] No matching files found in {args.outputs_dir}")
        return

    dfs = []
    skipped_files = 0
    for path in files:
        try:
            df = pd.read_csv(path)
            df["_source_file"] = os.path.basename(path)
            dfs.append(df)
        except Exception as exc:
            skipped_files += 1
            print(f"[WARN] Could not read {path}: {str(exc).splitlines()[0]}")

    if not dfs:
        print("[ERROR] No readable CSV files found.")
        return

    combined = pd.concat(dfs, ignore_index=True)

    state_col = _first_existing_column(combined, ["state", "State"])
    tracking_col = _first_existing_column(
        combined,
        [
            "SERFF Tracking Number",
            "serff_tracking_number",
            "serf_num",
            "tracking_number",
        ],
    )
    mapping_col = _first_existing_column(
        combined,
        ["form_name_mapping", "mapping", "file_name_form_name_mapping"],
    )

    if state_col is None or tracking_col is None:
        print(
            "[ERROR] Required columns missing. Need state + tracking columns in combined data."
        )
        print(f"       state_col={state_col}, tracking_col={tracking_col}")
        return

    # Ensure target columns exist
    if "zip_status" not in combined.columns:
        combined["zip_status"] = pd.NA
    if "zip_size" not in combined.columns:
        combined["zip_size"] = pd.NA
    if "zip_attempts" not in combined.columns:
        combined["zip_attempts"] = pd.NA
    if "zip_error" not in combined.columns:
        combined["zip_error"] = pd.NA

    total_rows = len(combined)
    marked_success = 0
    marked_failed = 0
    already_success = 0

    for idx, row in combined.iterrows():
        state = row.get(state_col)
        tracking = row.get(tracking_col)
        if pd.isna(state) or pd.isna(tracking):
            continue

        status = str(row.get("zip_status")).strip().lower()
        if status == "success":
            already_success += 1
            continue

        zip_path = _zip_path(args.zip_base_dir, state, str(tracking).strip())
        zip_size = _zip_size_if_valid(zip_path)
        if zip_size is None:
            has_mapping = False
            if mapping_col is not None:
                mapping_val = row.get(mapping_col)
                has_mapping = pd.notna(mapping_val) and str(mapping_val).strip().lower() not in {
                    "",
                    "none",
                    "nan",
                    "<na>",
                }
            is_empty_status = status in {"", "none", "nan", "<na>"}
            if has_mapping and is_empty_status:
                combined.at[idx, "zip_status"] = "failed"
                marked_failed += 1
            continue

        combined.at[idx, "zip_status"] = "success"
        combined.at[idx, "zip_size"] = int(zip_size)
        combined.at[idx, "zip_attempts"] = 0
        combined.at[idx, "zip_error"] = pd.NA
        marked_success += 1

    # Keep most useful row per tracking number, prefer successful ZIP rows.
    combined["_zip_success_rank"] = combined["zip_status"].astype(str).str.lower().eq(
        "success"
    )
    combined = combined.sort_values("_zip_success_rank", ascending=False)
    combined = combined.drop_duplicates(subset=[tracking_col], keep="first")
    combined = combined.drop(columns=["_zip_success_rank"])

    if args.output_file:
        output_path = args.output_file
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(
            args.outputs_dir, f"state_results_backfilled_zip_{ts}.csv"
        )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined.to_csv(output_path, index=False)

    print(f"[DONE] Read files: {len(dfs)} (skipped unreadable: {skipped_files})")
    print(f"[DONE] Rows scanned: {total_rows}")
    print(f"[DONE] Already success rows: {already_success}")
    print(f"[DONE] Newly marked success by ZIP existence: {marked_success}")
    print(f"[DONE] Newly marked failed by missing ZIP + mapping: {marked_failed}")
    print(f"[DONE] Deduped output rows: {len(combined)}")
    print(f"[DONE] Wrote: {output_path}")


if __name__ == "__main__":
    main()
