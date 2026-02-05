import csv
import requests
import time
import os
import re
from datetime import datetime
import math

# --- CONFIGURATION ---
API_URL = "https://lylfy0m6gg.execute-api.us-east-1.amazonaws.com/testVirginiaUno/getAdDetailsVirginia"

# [IMPORTANT] REPLACE THIS WITH YOUR ACTUAL CLOUD FUNCTION URL
# The provided JS file imported this from a constants file, so the value wasn't visible.
CLOUD_FUNCTION_URL = "https://vsyvz3xevj.execute-api.us-east-1.amazonaws.com/GuardarEnBucket/GuardarAS3"

# Target technologies we want to track (case-insensitive matching)
TARGET_TECHNOLOGIES = [
    "hotmart", "kiwify", "shopify", "panda", "stripe", "clickfunnels",
    "vturb", "pandavideo", "vidalytics", "atomicat", "vimeo", "youtube", "digistore24"
] 

# --- TECHNOLOGY DETECTOR IMPORT ---
# Trying to import as requested. If the file is missing, the script will warn you.
try:
    from .tech_detector_new import TechnologyDetector
except Exception:
    try:
        from tech_detector_new import TechnologyDetector
    except ImportError:
        print("Warning: 'tech_detector_new.py' not found. Using Mock Detector for demonstration.")

        class MockTechnologyDetector:
            def detect_technologies(self, url):
                return ["Unknown-Tech"]

        TechnologyDetector = MockTechnologyDetector

# --- HELPER FUNCTIONS ---

def get_current_timestamp_mariadb():
    """Returns format: YYYY-MM-DD HH:MM:SS"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def is_detectable_tech(url):
    """
    Verifies if the URL contains non-detectable keywords.
    """
    if not url:
        return False
        
    non_detectable_keywords = [
        "instagram", "facebook", "fb", "ig", "api", "messenger",
        "whatsapp", "google", "youtube", "drive.google",
        "bitly", "tinyurl", "rebrandly", "temu", "amazon"
    ]

    url_lower = url.lower()
    for keyword in non_detectable_keywords:
        if keyword in url_lower:
            return False
    return True

def upload_media_with_cloud_function(url_src, file_type="video", file_name="media"):
    """
    Refactored to mimic JS UploadMediaWhitCloudFunction logic:
    - Uses HTTP GET
    - Constructs query params for filename and URLCreative
    - Returns s3_url from response
    """
    if not url_src:
        return None

    try:
        sub_name = int(time.time() * 1000)
        extension = ".mp4" if file_type == "video" else ".jpg"
        # file_name param: fileName + "_" + sub_name + extension
        file_name_param = f"{file_name}_{sub_name}{extension}"
        url_creative_param = requests.utils.quote(url_src, safe='')
        endpoint = f"{CLOUD_FUNCTION_URL}?filename={file_name_param}&URLCreative={url_creative_param}"

        headers = {'Content-Type': 'application/json'}
        response = requests.get(endpoint, headers=headers)

        if response.status_code == 200:
            data = response.json()
            # JS returns response.data, which contains s3_url
            return data.get("s3_url", "")
        else:
            print(f"Error uploading media: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception during upload: {e}")
        return None

def process_date_field(date_str):
    """
    Converts startDate to epoch and calculates days elapsed.
    Assumes date_str format from CSV. Since format isn't specified, 
    we try generic parsing or assume ISO.
    """
    try:
        # Attempting standard formats. Adjust if CSV has specific format like DD/MM/YYYY
        # Trying ISO first 
        dt = None
        formats = [
            '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%m/%d/%Y',
            '%b %d, %Y', '%B %d, %Y'  # e.g., 'Dec 12, 2024', 'December 12, 2024'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                break
            except ValueError:
                continue
        
        if dt:
            epoch = int(dt.timestamp())
            current_epoch = int(time.time())
            diff_seconds = current_epoch - epoch
            days_since = math.floor(diff_seconds / (24 * 3600))
            return epoch, days_since
    except Exception:
        pass
    
    return 0, 0


def extract_keyword_from_filename(input_file: str) -> str:
    """Extracts keyword from filenames like: YYYYMMDD_HHMMSS_<keyword>.csv"""
    base_name = os.path.basename(str(input_file or ""))
    stem = base_name.rsplit('.', 1)[0]

    match = re.match(r'^\d{8}_\d{6}_(?P<keyword>.+)$', stem)
    if match:
        return match.group('keyword')

    # Fallback: return the stem if it has alphabetic chars; otherwise empty.
    if re.search(r'[A-Za-zÀ-ÖØ-öø-ÿ]', stem):
        return stem
    return ""

# --- MAIN PROCESSING ---

def main(input_file: str):
    print("Starting process...")
    start_time = time.time()

    filename = os.path.basename(str(input_file or ""))
    output_file = "processed_" + filename

    keyword = extract_keyword_from_filename(input_file)

    detector = TechnologyDetector()

    # Read Input CSV
    rows = []
    try:
        with open(input_file, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return

    # Define all headers (Original + New)
    fieldnames = [
        # Original
        "libraryID", "startDate", "Duplicates", "Keyword",
        # New
        "cta_text", "cta_type", "__html", "page_profile_uri", "publisherPlatform",
        "URLCreative", "url_preview_creative", "AdCreative", "AdMedia",
        "profilePict", "page_profile_picture_url", "Active", "Estatus",
        "CollectionCount", "CollationID", "endDate", "LibraryID", "ahref",
        "pageName", "pageID", "AdDescription", "AdTitle", "age", "gender",
        "languages", "countries", "lazy_load", "contains_details", "domain",
        "createdAt", "updatedAt", "AdDescription_plain", "AdTitle_plain",
        "daysSincePublication", "codeBelongs"
    ]

    # Process in batches of 5
    batch_size = 5
    total_rows = len(rows)
    
    print(f"Found {total_rows} rows. Processing...")

    # Open output file
    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            
            # Extract IDs for query
            # Filtering out empty IDs just in case
            ids_list = [row['libraryID'] for row in batch if row.get('libraryID')]
            
            if not ids_list:
                continue

            ids_query_string = ",".join(ids_list)
            
            print(f"Processing batch {i//batch_size + 1}: IDs {ids_query_string}")

            # Make API Request
            try:
                response = requests.get(f"{API_URL}?ids={ids_query_string}")
                api_data = response.json()
            except Exception as e:
                print(f"API Request failed: {e}")
                # We still write the rows, but with empty API data
                api_data = []

            # Create a lookup dictionary for API results
            # The API returns 'LibraryID' (capital L) usually, or 'id'
            api_lookup = {}
            if isinstance(api_data, list):
                for item in api_data:
                    # Handle potential key differences
                    lib_id = item.get('LibraryID') or item.get('id')
                    if lib_id:
                        api_lookup[str(lib_id)] = item

            # Update rows
            for row in batch:
                lib_id = row.get('libraryID')
                json_obj = api_lookup.get(lib_id, {})
                
                # --- Tech Detection (Early filter to save S3 costs) ---
                link_url = json_obj.get('link_url', '')
                code_belongs = ""
                
                if link_url and is_detectable_tech(link_url):
                    try:
                        technologies = detector.detect_technologies(link_url)
                        if technologies:
                            code_belongs = ','.join(technologies)
                    except Exception as e:
                        print(f"Tech detection failed for {link_url}: {e}")
                
                # Skip rows that don't have any target technology
                if not code_belongs or not any(
                    target.lower() in code_belongs.lower() for target in TARGET_TECHNOLOGIES
                ):
                    continue
                
                # --- Update Date Fields ---
                epoch_date, days_since = process_date_field(row.get('startDate', ''))
                
                # --- Mappings ---
                row['startDate'] = epoch_date # Updating existing field
                row['daysSincePublication'] = days_since
                row['Keyword'] = keyword
                
                # New Fields Mapping
                row['cta_text'] = json_obj.get('cta_text', '')
                row['cta_type'] = json_obj.get('cta_type', '')
                row['__html'] = json_obj.get('__html', '')
                row['page_profile_uri'] = json_obj.get('page_profile_uri', '')
                row['publisherPlatform'] = "facebook"
                row['URLCreative'] = json_obj.get('URLCreative', '')
                row['url_preview_creative'] = json_obj.get('url_preview_creative', '')
                
                # AdCreative & AdMedia Logic (Upload to S3)
                original_creative_url = json_obj.get('AdCreative', '') or json_obj.get('URLCreative', '')
                
                # Upload to S3
                s3_url = ""
                if original_creative_url:
                    # Determine type for the function based on AdMedia or URL extension
                    ftype = "video" if "mp4" in original_creative_url else "img"
                    # Use lib_id as file_name for uniqueness
                    s3_url = upload_media_with_cloud_function(original_creative_url, ftype, str(lib_id) if lib_id else "media")
                
                # If upload failed or returned empty, we might want to keep original or leave empty.
                # Prompt implies assigning the result.
                row['AdCreative'] = s3_url if s3_url else original_creative_url
                row['AdMedia'] = s3_url if s3_url else original_creative_url # Prompt asked for same value
                
                row['profilePict'] = json_obj.get('profilePict', '')
                row['page_profile_picture_url'] = json_obj.get('page_profile_picture_url', '')
                row['Active'] = json_obj.get('Active', '')
                row['Estatus'] = json_obj.get('Active', '') # Prompt asks for Active value here too
                
                # Static/Zero values
                row['CollectionCount'] = 0
                row['CollationID'] = 0
                row['endDate'] = 0
                row['LibraryID'] = json_obj.get('LibraryID', lib_id)
                row['ahref'] = "" # Prompt says JSON_object. (empty property name?) assuming empty or link_url
                row['pageName'] = json_obj.get('pageName', '')
                row['pageID'] = json_obj.get('pageID', '')
                row['AdDescription'] = ""
                row['AdTitle'] = json_obj.get('title', '')
                row['age'] = 0
                row['gender'] = ""
                row['languages'] = ""
                row['countries'] = ""
                row['lazy_load'] = True
                row['contains_details'] = True
                row['domain'] = ""
                
                # Timestamps
                now_ts = get_current_timestamp_mariadb()
                row['createdAt'] = now_ts
                row['updatedAt'] = now_ts
                
                row['AdDescription_plain'] = ""
                row['AdTitle_plain'] = ""
                row['codeBelongs'] = code_belongs

                # Write row and flush to disk immediately
                writer.writerow(row)
                outfile.flush()
                os.fsync(outfile.fileno())

    print(f"Done. Results saved to {output_file}")
    elapsed = time.time() - start_time
    print(f"Total processing time: {elapsed:.2f} seconds")
    return output_file

if __name__ == "__main__":
    filename = "20260203_113620_agora.csv"
    main(filename)