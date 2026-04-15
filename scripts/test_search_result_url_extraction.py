import os
import sys
import pandas as pd

# Ensure local src/ directory is importable when running as a script
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
for p in (ROOT_DIR, SRC_DIR):
    if p not in sys.path:
        sys.path.insert(0, p)

from src import all_details_scrape as mod


def setup_fake_environment():
    """
    Monkeypatch network/browser-dependent functions in all_details_scrape so we can
    run process_state on a tiny synthetic DataFrame without hitting SERFF.
    """

    # 1) Force HTTP path to "fail" so the code falls back to the search path
    def fake_fetch_page(http_session, url, timeout=30):
        # (html=None, expired=True, srv_err=False) -> triggers search fallback
        return None, True, False

    # 2) Minimal fake Playwright-like objects
    class FakePage:
        def __init__(self):
            # will be overwritten by fake_navigate_via_search
            self.url = "about:blank"

        def content(self):
            # Minimal HTML that satisfies both scrape_attachment_mappings_html
            # and parse_filing_metadata_html (attachmentsContainer + Submission Date)
            return """
            <html>
              <body>
                <div id="attachmentsContainer"></div>
                <div id="j_idt28">
                  <div class="row">
                    <label class="col-sm-5">Submission Date:</label>
                    <div class="col-sm-7">2025-01-01</div>
                  </div>
                </div>
              </body>
            </html>
            """

    class FakeContext:
        def add_cookies(self, cookies):
            pass

        def cookies(self):
            # Return an empty cookie jar structure similar to Playwright
            return []

    # 3) Fake browser bootstrap: return our fake page/context
    def fake_ensure_browser(pw, browser, context, page, state_name):
        fake_page = FakePage()
        fake_context = FakeContext()
        return None, None, fake_context, fake_page

    # 4) Fake re-auth: just return the same context/page and http_session
    def fake_reauth_and_refresh(browser, context, page, state_name, http_session):
        return context, page, http_session, True

    # 5) Fake search navigation: set a deterministic URL and "succeed"
    def fake_navigate_via_search(page, context, tracking_number, state_name):
        page.url = (
            f"https://fake.serff.test/filingSummary.xhtml?serff={tracking_number}"
        )
        return True

    # Apply monkeypatches onto the imported module
    def fake_create_browser():
        # Match signature of browser_utils.create_browser used in process_state
        fake_page = FakePage()
        fake_context = FakeContext()
        return None, None, fake_context, fake_page

    def fake_authenticate(page, state_name):
        return True

    mod.create_browser = fake_create_browser
    mod.authenticate = fake_authenticate
    mod.fetch_page = fake_fetch_page
    mod._ensure_browser = fake_ensure_browser
    mod._reauth_and_refresh = fake_reauth_and_refresh
    mod.navigate_via_search = fake_navigate_via_search

    # Make sure the outputs directory exists for the CSV that process_state writes
    os.makedirs("outputs", exist_ok=True)


def build_test_work_unit():
    """
    Build a minimal DataFrame with a single alpha-ID row so process_state
    will go down the search path and set search_result_url.
    """
    # Column order is important because process_state reads tracking_number
    # via getattr(row, "_1", None). For columns with non-identifier names,
    # pandas.itertuples() uses positional names like _1, _2, ...
    # By placing "SERFF Tracking Number" first, it becomes _1.
    df = pd.DataFrame(
        {
            "SERFF Tracking Number": ["TEST-ALPHA-123"],
            "state": ["MI"],
            # Alpha filingId -> has_alpha_filing_id(url) == True
            "page_url": [
                "https://filingaccess.serff.com/sfa/search/filingSummary.xhtml?filingId=6LJMVY298/00"
            ],
        }
    )
    work_units = mod.build_work_units(df)
    assert len(work_units) == 1, "Expected exactly one work unit for the test"
    return work_units[0]


def main():
    setup_fake_environment()
    state_data = build_test_work_unit()

    print("Running process_state on synthetic alpha-ID row...")
    result_file = mod.process_state(state_data)

    print(f"\nprocess_state returned: {result_file}")
    if result_file and os.path.exists(result_file):
        out_df = pd.read_csv(result_file)
        cols_to_show = [
            c
            for c in [
                "state",
                "SERFF Tracking Number",
                "page_url",
                "search_result_url",
                "form_name_mapping",
            ]
            if c in out_df.columns
        ]
        print("\nResult rows (subset of columns):")
        print(out_df[cols_to_show])
    else:
        print("No result file was produced by process_state.")


if __name__ == "__main__":
    main()

