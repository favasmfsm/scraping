import os
import re
import shutil
import zipfile
from typing import Dict, Set, Optional

import fitz  # PyMuPDF


def is_form_number(text: str) -> bool:
    """
    Heuristic check for whether a line looks like a form number rather than a form name.
    Reused by TOC parsing so it lives here for both scripts.
    """
    if re.match(r"^[A-Z0-9\\-]+$", text) and "-" in text:
        return True

    if re.match(r"^\\d+XX\\s+\\d{2}/\\d{4}$", text):
        return True

    if re.match(r"^\\d+XX$", text):
        return True

    return False


def extract_toc_form_names(root_pdf_path: str) -> Dict[str, str]:
    """
    Extract form name mappings from the Table of Contents on page 1 of the root PDF.
    Returns dict: {filename: form_name}
    """
    toc_mapping: Dict[str, str] = {}

    try:
        pdf = fitz.open(root_pdf_path)
        try:
            page = pdf[0]
            text = page.get_text()
        finally:
            pdf.close()

        lines = [l.strip() for l in text.split("\\n") if l.strip()]

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
    except Exception as e:
        print(f"[WARN] Could not extract TOC from {root_pdf_path}: {e}")

    return toc_mapping


def process_subfolder_files(subfolder_path: str) -> Dict[str, int]:
    """
    Process all files in a subfolder and return file info dict.
    Returns dict: {filename: page_count (for PDFs) or file_extension (for non-PDFs)}.
    """
    file_info: Dict[str, int] = {}

    try:
        for filename in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, filename)

            if not os.path.isfile(file_path):
                continue

            if filename.lower().endswith(".pdf"):
                try:
                    pdf = fitz.open(file_path)
                    try:
                        file_info[filename] = len(pdf)
                    finally:
                        pdf.close()
                except Exception as e:
                    print(f"[WARN] Could not read PDF {filename}: {e}")
                    file_info[filename] = 0
            else:
                ext = os.path.splitext(filename)[1].lstrip(".")
                file_info[filename] = ext if ext else "unknown"
    except Exception as e:
        print(f"[ERROR] Failed to process subfolder {subfolder_path}: {e}")

    return file_info


def extract_readability_text(
    root_dir: str, ignore_folders: Optional[Set[str]] = None
) -> Dict[str, Optional[str]]:
    """
    Extract text from PDFs with 'read' or 'flesch' in the filename under root_dir.
    Returns dict: {filename: extracted_text}.
    """
    if ignore_folders is None:
        ignore_folders = set()

    readability_texts: Dict[str, Optional[str]] = {}

    for subfolder in os.listdir(root_dir):
        subfolder_path = os.path.join(root_dir, subfolder)

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
                    try:
                        text_parts = []
                        for page in pdf:
                            page_text = page.get_text() or ""
                            text_parts.append(page_text)
                        readability_texts[filename] = "".join(text_parts)
                    finally:
                        pdf.close()
                except Exception as e:
                    print(f"[WARN] Could not extract text from {filename}: {e}")
                    readability_texts[filename] = None

    return readability_texts


def extract_zip_to_dir(zip_file_path: str, extract_dir: str) -> bool:
    """
    Extract the given ZIP file into extract_dir, normalizing Windows-style
    backslash paths and skipping directory-only entries.
    Returns True on success, False on failure.
    """
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            for member in zip_ref.namelist():
                normalized_path = member.replace("\\\\", "/")
                if normalized_path.endswith("/"):
                    continue
                target_path = os.path.join(extract_dir, normalized_path)
                parent_dir = os.path.dirname(target_path)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
                with zip_ref.open(member) as src, open(target_path, "wb") as dst:
                    dst.write(src.read())
        return True
    except Exception as e:
        print(f"[ERROR] Failed to extract ZIP {zip_file_path}: {e}")
        try:
            os.remove(zip_file_path)
        except Exception:
            pass
        try:
            shutil.rmtree(extract_dir, ignore_errors=True)
        except Exception:
            pass
        return False
