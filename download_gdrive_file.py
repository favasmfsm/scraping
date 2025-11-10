"""
Script to download a specific file from Google Drive.
"""

from src.gdrive_download import download_file
import os

# Google Drive URL
gdrive_url = "https://drive.google.com/file/d/1oupXYUYl-TRkVi9ycqinh-OQz7BrCO_B/view?usp=sharing"

# Output directory
output_dir = "downloads"
os.makedirs(output_dir, exist_ok=True)

# Download the file
print(f"Downloading file from: {gdrive_url}")
print("File ID: 1oupXYUYl-TRkVi9ycqinh-OQz7BrCO_B")
print(f"Output directory: {output_dir}")
print("-" * 50)

try:
    # Use requests method which handles virus scan warnings better
    from src.gdrive_download import download_public_file_requests
    
    output_file = os.path.join(output_dir, "form_data_full.csv")
    file_path = download_public_file_requests(gdrive_url, output_file)
    
    print(f"\n✅ Successfully downloaded to: {file_path}")
    if os.path.exists(file_path):
        file_size_mb = os.path.getsize(file_path) / (1024*1024)
        print(f"File size: {file_size_mb:.2f} MB")
    
except Exception as e:
    print(f"\n❌ Error downloading file: {e}")
    print("\nTrying auto method...")
    
    try:
        # Try auto method as fallback
        file_path = download_file(
            gdrive_url,
            output_path=os.path.join(output_dir, "form_data_full.csv"),
            method="auto",
            quiet=False
        )
        
        print(f"\n✅ Successfully downloaded to: {file_path}")
        if os.path.exists(file_path):
            file_size_mb = os.path.getsize(file_path) / (1024*1024)
            print(f"File size: {file_size_mb:.2f} MB")
    except Exception as e2:
        print(f"\n❌ Alternative method also failed: {e2}")
        print("\nNote: If the file is private, you may need to:")
        print("1. Make the file public (share with 'Anyone with the link')")
        print("2. Or use the API method with Google credentials")

