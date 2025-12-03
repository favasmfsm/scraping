# SERFF Insurance Filing Data Scraping and Analysis Process Documentation

## Overview

This document describes a two-phase web scraping and data analysis pipeline designed to extract insurance filing information from the SERFF (System for Electronic Rate and Form Filing) website and compute readability scores for associated PDF documents. The process was executed on a 16-core AWS EC2 instance to leverage parallel processing capabilities.

---

## Phase 1: Tabular Data Extraction

### Objective
Extract structured tabular data from the SERFF filing access website for each state, including insurance company information, filing details, and tracking numbers.

### Implementation
**File:** `notebooks/csv_data.ipynb`

### Process Flow

1. **Initialization**
   - Sets up Selenium WebDriver with Chrome browser
   - Configures browser to start maximized
   - Navigates to state-specific SERFF homepage: `https://filingaccess.serff.com/sfa/home/{state}`

2. **Manual Filter Configuration**
   - Provides a 20-second window for manual filter setup on the website
   - Allows user to configure search criteria before automated scraping begins

3. **Automated Pagination Scraping**
   - Extracts data from paginated tables automatically
   - Implements intelligent pagination detection and navigation
   - Saves progress incrementally to CSV files

### Key Features

- **Auto-Pagination**: Automatically detects and clicks "Next" button until all pages are scraped
- **Progress Tracking**: Saves data after each page to prevent data loss
- **Page Number Tracking**: Records which page each row was extracted from
- **Error Handling**: Gracefully handles pagination failures and extraction errors
- **Incremental Saving**: Continuously updates CSV file with new data

### Data Extracted

The following fields are captured for each filing record:
- Company Name
- NAIC Company Code
- Insurance Product Name
- Sub Type Of Insurance
- Filing Type
- Filing Status
- SERFF Tracking Number
- Page Number (for tracking purposes)

### Output
- CSV file per state: `insurance_filings_auto_pagination_{state}.csv`
- Contains all scraped rows with incremental saves during execution

---

## Phase 2: PDF Download and Flesch Reading Ease Score Computation

### Objective
For each SERFF tracking number extracted in Phase 1, download associated PDF attachments, extract text, and compute Flesch Reading Ease scores to assess document readability.

### Implementation
**File:** `src/flesch_serf_woth_alpha.py`

### Architecture

#### Parallel Processing Strategy
- **Multi-processing**: Uses Python's `multiprocessing.Pool` for parallel execution
- **State-based Chunking**: Data is grouped by state and split into chunks of 50 rows
- **Process Isolation**: Each worker process maintains its own Chrome instance and download directory
- **Resource Management**: Limits concurrent processes to 2 or available CPU cores (whichever is smaller)

#### Process Flow

1. **Data Preparation**
   - Reads input CSV file: `data/to_extract_final_batch.csv`
   - Groups data by state
   - Splits each state into chunks of 50 rows
   - Creates task groups for parallel processing

2. **Worker Process Execution** (`process_state` function)
   Each worker process:
   
   a. **Browser Setup**
      - Creates isolated Chrome user data directory for cache management
      - Configures headless Chrome browser
      - Sets up dedicated download directory per process
      - Configures automatic PDF download behavior
   
   b. **Authentication**
      - Navigates to state-specific SERFF homepage
      - Accepts user agreement automatically
      - Maintains session for all requests within the process
   
   c. **For Each Row in Chunk:**
      - Navigates to filing search page
      - Enters SERFF Tracking Number
      - Clicks search and selects first result
      - Extracts submission date from filing details page
      - Iterates through all form attachments:
         * Extracts form name
         * Downloads PDF attachment
         * Waits for download completion (up to 15 seconds)
         * Extracts text using PyMuPDF library
         * Computes Flesch Reading Ease score using `textstat` library
         * Deletes PDF file immediately after processing
         * Stores form name and Flesch score in lists

3. **Progress Checkpointing**
   - Saves progress every 20 rows to temporary CSV files
   - Prevents data loss in case of interruptions
   - Cleans up Chrome cache directories periodically

4. **Result Aggregation**
   - Combines all partial results from parallel processes
   - Merges into single final CSV file
   - Cleans up temporary files and directories

### Key Technical Features

#### Download Management
- **File Detection**: Monitors download directory for new PDF files
- **Download Completion Detection**: Filters out incomplete downloads (`.crdownload` files)
- **File Selection**: Selects most recently modified PDF when multiple candidates exist
- **Immediate Cleanup**: Deletes PDFs after text extraction to manage disk space

#### Text Extraction
- **Library**: Uses PyMuPDF (fitz) for robust PDF text extraction
- **Page-by-Page Processing**: Iterates through all pages to extract complete text
- **Error Handling**: Gracefully handles corrupted or unreadable PDFs

#### Flesch Reading Ease Score
- **Library**: `textstat.flesch_reading_ease()`
- **Purpose**: Measures readability on a scale of 0-100 (higher = easier to read)
- **Storage**: Stored as list per row (one score per PDF attachment)

#### Resource Management
- **Chrome Cache Cleanup**: Automatically removes temporary Chrome cache directories
- **Download Directory Cleanup**: Removes process-specific download directories after completion
- **Memory Efficiency**: Processes and deletes files immediately to minimize disk usage

### Data Structure

**Input CSV Columns:**
- All columns from Phase 1
- `state` column (for grouping)

**Output CSV Columns:**
- All original columns from input
- `form_name`: List of form names for each filing
- `submission_date`: Submission date extracted from filing details
- `flesch_reading_ease`: List of Flesch scores (one per PDF attachment)

### Output Files

- **Temporary Files**: `outputs/temp_results_{state}_chunk{chunk_idx}_{pid}.csv`
  - Created during processing for checkpointing
  - One file per state chunk per process
  
- **Final Output**: `form_names_submission_date.csv`
  - Combined results from all states and processes
  - Contains all extracted data with form names and Flesch scores

---

## Technical Stack

### Libraries and Tools
- **Selenium WebDriver**: Browser automation
- **Pandas**: Data manipulation and CSV handling
- **PyMuPDF (fitz)**: PDF text extraction
- **textstat**: Flesch Reading Ease score computation
- **multiprocessing**: Parallel processing
- **tqdm**: Progress bars
- **Chrome/ChromeDriver**: Headless browser execution

### System Requirements
- Python 3.x
- Chrome browser
- ChromeDriver (compatible with Chrome version)
- Sufficient disk space for temporary downloads
- Network connectivity to SERFF website

### Execution Environment
- **Platform**: AWS EC2 instance
- **Cores**: 16-core system
- **Parallelism**: Up to 2 concurrent processes (configurable)
- **Chunk Size**: 50 rows per chunk per process

---

## Error Handling and Resilience

### Phase 1
- Handles pagination failures gracefully
- Continues scraping even if individual page extraction fails
- Saves progress incrementally to prevent data loss

### Phase 2
- Isolated processes prevent cascading failures
- Checkpointing every 20 rows ensures progress preservation
- Graceful handling of:
  - Missing PDF files
  - Corrupted PDFs
  - Network timeouts
  - Download failures
  - Text extraction errors
- Automatic cleanup of temporary resources

---

## Performance Considerations

### Optimization Strategies
1. **Parallel Processing**: Multiple states/chunks processed simultaneously
2. **Headless Browser**: Reduces resource consumption
3. **Immediate File Deletion**: Prevents disk space accumulation
4. **Cache Management**: Regular cleanup of Chrome cache directories
5. **Process Isolation**: Prevents resource contention between workers
6. **Checkpointing**: Enables resumption without full restart

### Scalability
- Process count limited by available CPU cores
- Chunk size (50 rows) balances memory usage and checkpoint frequency
- Can be adjusted based on system resources and data volume

---

## Summary

This two-phase pipeline successfully:
1. Extracts comprehensive insurance filing data from SERFF website across multiple states
2. Downloads and processes PDF attachments in parallel
3. Computes readability scores for each document
4. Aggregates results into a unified dataset

The implementation emphasizes reliability through checkpointing, efficiency through parallel processing, and resource management through automatic cleanup mechanisms.

