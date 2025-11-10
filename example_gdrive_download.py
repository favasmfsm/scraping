"""
Example script showing how to download files from Google Drive.

This demonstrates different ways to download files from Google Drive.
"""

from src.gdrive_download import download_file, extract_file_id

# Example 1: Download a public file using auto method (simplest)
# Replace with your actual Google Drive URL or file ID
gdrive_url = "https://drive.google.com/file/d/YOUR_FILE_ID/view"
output_path = "downloaded_file.pdf"

try:
    file_path = download_file(gdrive_url, output_path, method="auto")
    print(f"✅ Downloaded to: {file_path}")
except Exception as e:
    print(f"❌ Error: {e}")

# Example 2: Download using file ID directly
file_id = "YOUR_FILE_ID"
output_path = "my_file.pdf"

try:
    file_path = download_file(file_id, output_path)
    print(f"✅ Downloaded to: {file_path}")
except Exception as e:
    print(f"❌ Error: {e}")

# Example 3: Extract file ID from various URL formats
urls = [
    "https://drive.google.com/file/d/1ABC123XYZ/view",
    "https://drive.google.com/open?id=1ABC123XYZ",
    "https://drive.google.com/uc?id=1ABC123XYZ",
    "1ABC123XYZ"  # Direct ID
]

for url in urls:
    file_id = extract_file_id(url)
    print(f"URL: {url} -> File ID: {file_id}")

