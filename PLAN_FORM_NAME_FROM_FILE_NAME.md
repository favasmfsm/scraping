# Plan: Find Form Name for Each File (from final_batch_22_feb.csv)

## Goal

- **Input:** `data/final_batch_22_feb.csv` with columns: `SERFF Tracking Number`, `state`, `page_url`, `auth_url`, `file_name`.
- **file_name:** Contains a **list of PDF filenames**. Stored as a string that looks like a Python list (e.g. `"['GA80081CERTEND.pdf', 'MPA Actuarial Memorandum.pdf']"`), so it may need `ast.literal_eval()` when parsing.
- **Output:** For each row, after loading the filing summary page, get the **form name** (and optionally form_number, section) for **each file** in that row’s `file_name` list.

Current flow already loads the page and calls `scrape_attachment_mappings_html(html)` to build a mapping; the part that “didn’t really work” is the **BeautifulSoup-based** parser `_parse_panel_rows_bs` (and thus the mapping is often empty or wrong). We need to **test on real data**, fix or replace the parser, then **use the mapping to look up form name per file**.

---

## 1. Data and parsing

- **Load CSV:** `pd.read_csv("data/final_batch_22_feb.csv")`.
- **Parse `file_name` into a list:**
  - If `file_name` is already a list (e.g. from pandas), use as-is.
  - If it’s a string that looks like a list (starts with `[`), use `ast.literal_eval(file_name)` (with try/except; on failure keep original or `[]`).
- **Tracking number:** Use `SERFF Tracking Number` (current code uses `row._1` in itertuples, which is the first column — correct).

---

## 2. Test dataset (from final_batch_22_feb.csv)

Pick a **small, reproducible set** for debugging:

- **Option A – Same state, 2–3 rows:** e.g. first 2–3 rows where `state == "IA"` (or any single state that has both `page_url` and `file_name`).
- **Option B – Mixed:** 1 row with **numeric** `filingId` (direct URL) + 1 row with **alpha** `filingId` (search), to test both HTTP and Playwright paths.
- **Option C – Small variety:** 3–5 rows from different states, with 1–2 files each.

Suggested minimal test: **3 rows** from the top of the CSV (e.g. rows 0, 1, 2), so we have known `page_url` and `file_name` lists to compare against the scraped mapping.

---

## 3. Why the current parser may not work (hypotheses to test)

`scrape_attachment_mappings_html` + `_parse_panel_rows_bs` assume:

1. **`#attachmentsContainer`** exists in the HTML.
2. **Panel IDs** match: `summaryForm:formAttachmentPanel_content`, etc.
3. **Structure:** `div.summaryScheduleItemHeader` for headers, `div.summaryScheduleItemData` for cells, and an “attachments” column whose links are the filenames.
4. **Header text** matches `_HEADER_ALIASES` (e.g. “form name”, “form number”, “attachments”, “document name”).

Possible failures:

- SERFF HTML changed: different IDs/classes or structure.
- **JS-rendered content:** if the attachment table is loaded dynamically, `requests`/initial HTML might not contain it (Playwright would).
- **Different column labels** (e.g. “Attachment” vs “Attachments”).
- **No “attachments” column** in the panel (parser returns early if `"attachments" not in col_map.values()`).

---

## 4. Testing steps (to run before implementation)

### 4.1 Save HTML for inspection

- For each row in the **test dataset** (e.g. 3 rows):
  - Use the **same navigation logic** as in production (direct URL via `fetch_page` for numeric IDs, or Playwright search for alpha IDs).
  - Get the **final** HTML after the page has loaded (e.g. `page.content()` in Playwright, or the HTML returned by `fetch_page`).
  - Save to a file, e.g. `outputs/debug_serff_<filingId_or_tracking>.html` (one file per row).
- Manually open the HTML and check:
  - Is `id="attachmentsContainer"` present?
  - Are the four panel IDs present? (e.g. `summaryForm:formAttachmentPanel_content`).
  - What are the **exact** header texts in the first panel? (e.g. “Form Name”, “Form Number”, “Attachments”.)
  - Are filenames inside `<a>` tags under a div with class `summaryScheduleItemData`?

### 4.2 Run current parser on saved HTML

- For each saved HTML file, call `scrape_attachment_mappings_html(html)` and print the result.
- Compare with the **expected** filenames from the CSV `file_name` for that row:
  - Do the keys of the mapping match the filenames in `file_name` (exact string match)?
  - Is the mapping empty? If so, which check in the parser failed (e.g. no container, no “attachments” column)?

### 4.3 Decide fix or alternative

- **If structure matches but alias/class is wrong:** adjust `_HEADER_ALIASES` or class names in `_parse_panel_rows_bs`.
- **If structure is different:** update selectors in `_parse_panel_rows_bs` to match the real DOM (using the saved HTML as reference).
- **If content is only in JS-rendered page:** prefer using **Playwright** to get HTML (or use the AWS-style flow that uses `page.locator` in `file_name_form_name_mapping_aws.py`) for those pages, and keep requests for static pages that already contain the table.
- **If filenames appear in a different pattern** (e.g. in a different panel or table), add a fallback path (e.g. regex or a second parser) and test again on the same HTML.

---

## 5. Lookup: file_name → form name (after mapping works)

Once `scrape_attachment_mappings_html(html)` returns a mapping `m` (filename → list of `{form_name, form_number, section}`):

- For each row, parse `file_name` → list `files`.
- For each `f` in `files`:
  - Look up `m.get(f)` (or try normalized key if filenames differ by spacing/case).
  - If present, take e.g. `form_name` from the first entry (or merge multiple if one file appears in multiple sections).
  - If absent, store `None` or `""` and optionally log.
- **Output shape** (choose one for implementation):
  - **A.** New column `form_name_mapping` = dict from filename → form_name (or list of form_name) for that row’s `file_name` list only.
  - **B.** Explode: one row per (SERFF Tracking Number, file_name) with columns `file_name`, `form_name`, `form_number`, `section`.

---

## 6. Implementation order (after tests pass)

1. **Data loading:** In the script that reads `final_batch_22_feb.csv`, add safe parsing of `file_name` → list (`ast.literal_eval` with fallback).
2. **Parser:** Fix or replace `_parse_panel_rows_bs` (and optionally `scrape_attachment_mappings_html`) based on test findings; keep saving debug HTML until mapping matches expectations for the test rows.
3. **Lookup:** After getting `mapping` for a row, compute per-file form names from the row’s `file_name` list and store in the chosen output shape.
4. **Resume/output:** Keep existing resume logic by SERFF Tracking Number; ensure the new per-file form name output is written to the state result CSVs and the final merged CSV.

---

## 7. Test script (implemented)

**Location:** `scripts/test_form_name_parser.py`

**Usage (from repo root, with venv activated):**

```bash
# Dry run: sample only, no browser (no auth/fetch)
python scripts/test_form_name_parser.py --dry-run --size 5 --seed 42

# 3 random rows: fetch HTML, save to outputs/, run parser, compare to file_name
python scripts/test_form_name_parser.py --size 3 --seed 42

# 20 random rows: confirm success
python scripts/test_form_name_parser.py --size 20 --seed 123
```

The script: samples N rows, parses `file_name` with `ast.literal_eval`, authenticates per state, fetches each page (HTTP or Playwright by `has_alpha_filing_id`), saves HTML to `outputs/debug_serff_<state>_<filingId>.html`, runs `scrape_attachment_mappings_html(html)`, and compares mapping keys to the row’s file list. Summary prints match rate and success/failure.

**Note:** Full run requires Playwright (Firefox) and network; run from your terminal if the IDE sandbox cannot launch the browser.

---

## Summary

| Step | Action |
|------|--------|
| 1 | Define test set: 3–5 rows from `final_batch_22_feb.csv` (e.g. first 3 rows). |
| 2 | Parse `file_name` with `ast.literal_eval` where needed. |
| 3 | For each test row, load page (requests or Playwright), save HTML to `outputs/debug_serff_*.html`. |
| 4 | Inspect HTML: confirm `#attachmentsContainer`, panel IDs, header texts, and where filenames live. |
| 5 | Run `scrape_attachment_mappings_html(html)` on saved HTML; compare mapping keys to `file_name` list. |
| 6 | Fix `_parse_panel_rows_bs` (or switch to Playwright parsing) based on findings. |
| 7 | Implement lookup: for each file in row’s `file_name`, get form name from mapping and store in chosen output format. |

Once step 5 shows the mapping matching the expected filenames for the test rows, implementation (steps 6–7) can proceed with confidence.
