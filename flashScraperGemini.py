import csv
import time
import re
import gc
import sys
import datetime
from urllib.parse import quote

# 1. Start session implementing this libraries
from facebook_auth import FacebookAuth

# 9. Use this configuration for selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def setup_driver():
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument("--start-maximized")
    options.add_argument('--lang=en')
    options.add_argument("--disable-notifications")
    
    # Initialize driver
    driver = webdriver.Chrome(options=options)
    return driver

def main(keyword, country="ALL"):
    driver = setup_driver()
    
    start_time = datetime.datetime.now()
    try:
        # 1.1 Authentication using FacebookAuth
        print("--- 1. Starting Session ---")
        auth = FacebookAuth(driver)
        
        # Try loading cookies first
        cookies_loaded = auth.load_cookies(driver)
        if cookies_loaded:
            print("Cookies loaded successfully.")
            driver.refresh()
            time.sleep(2)
        
        # Verify if actually logged in
        if not auth.is_logged_in(driver):
            print("Not logged in. Attempting interactive login...")
            if not auth.perform_login(driver):
                print("Login failed. Cannot proceed without authentication.")
                return None
            print("Login successful!")
        else:
            print("Session is active.")

        # 2. Navigate to URL
        target_url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country={country}&is_targeted_country=false&media_type=all&q={quote(keyword)}&search_type=keyword_unordered&sort_data[direction]=desc&sort_data[mode]=total_impressions"
        print(f"--- 2. Navigating to: {target_url} ---")
        driver.get(target_url)

        # 3. Wait until it loads completely
        print("--- 3. Waiting for page load ---")
        
        # JS snippet to count valid ads (used for both initial wait and scroll loop)
        count_script = """
            let divs = document.querySelectorAll('div.xh8yej3');
            let count = 0;
            for (let div of divs) {
                // Check for direct child with class containing 'x1plvlek'
                let child = div.querySelector(':scope > div[class*="x1plvlek"]');
                if (child) count++;
            }
            return count;
        """
        
        try:
            # Wait for DOM element to be present first
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CLASS_NAME, "xh8yej3"))
            )
            # Then wait for at least 1 actual ad to be visible/rendered
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script(count_script) > 0
            )
            print("First ads loaded and visible.")
        except TimeoutException:
            print("Timeout waiting for ads to load. Exiting.")
            return

        # 4. Scroll Loop (Optimized)
        print("--- 4. Starting Scroll Loop (Target: 100 ads) ---")

        current_count = driver.execute_script(count_script)
        stagnant_attempts = 0
        max_stagnant_attempts = 5

        while current_count < 100:
            last_count = current_count
            
            # Scroll to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for new content to appear (dynamic wait instead of fixed sleep)
            try:
                WebDriverWait(driver, 5).until(
                    lambda d: d.execute_script(count_script) > last_count
                )
                # Small buffer to allow rendering to complete
                time.sleep(0.3)
                stagnant_attempts = 0
            except TimeoutException:
                # No new content loaded within timeout
                stagnant_attempts += 1
                if stagnant_attempts >= max_stagnant_attempts:
                    print("No new ads loaded after several attempts. Stopping scroll.")
                    break
            
            current_count = driver.execute_script(count_script)
            print(f"Current count: {current_count} ads")

        print("--- 5. Executing High-Speed JS Extraction ---")
        
        # 5. HIGH SPEED EXTRACTION
        # Instead of 100+ Python Selenium calls, we send ONE script to the browser.
        # This script runs instantly inside the browser and returns the clean data.
        extraction_script = """
            let results = [];
            let containers = document.querySelectorAll('div.xh8yej3');
            
            // Regex patterns in JS
            let libIdRegex = /Library ID:\\s*(\\d+)/;
            let dateRegex = /Started running on\\s+(.*)/;
            let dupeRegex = /(\\d+)\\s+ads/;

            for (let container of containers) {
                if (results.length >= 100) break;

                // 1. Filter Check (must have child with x1plvlek)
                let child = container.querySelector(':scope > div[class*="x1plvlek"]');
                if (!child) continue;

                let textContent = container.innerText;
                let htmlContent = container.innerHTML;

                // 2. Extract Library ID
                // Try finding specific span first for accuracy, fallback to regex on full text
                let libId = "N/A";
                let libIdMatch = textContent.match(libIdRegex);
                if (libIdMatch) libId = libIdMatch[1];

                // 3. Extract Date
                let startDate = "N/A";
                let dateMatch = textContent.match(dateRegex);
                if (dateMatch) {
                    // Clean up the date string (remove newlines if any)
                    startDate = dateMatch[1].trim(); 
                }

                // 4. Extract Duplicates
                let duplicates = 0;
                // Look for strong tag specifically for duplicates as per requirement
                let strongTags = container.getElementsByTagName('strong');
                for (let strong of strongTags) {
                    let match = strong.innerText.match(dupeRegex);
                    if (match) {
                        duplicates = parseInt(match[1]);
                        break;
                    }
                }

                results.push({
                    "libraryID": libId,
                    "startDate": startDate,
                    "Duplicates": duplicates
                });
            }
            return results;
        """

        scraped_data = driver.execute_script(extraction_script)

        print(f"Extracted {len(scraped_data)} records.")

        # 7. Save results
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_keyword = re.sub(r'[^\w\-]', '_', keyword)  # sanitize keyword for filename
        filename = f"{timestamp}_{safe_keyword}.csv"
        print(f"--- 7. Saving to {filename} ---")
        
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=["libraryID", "startDate", "Duplicates"])
            writer.writeheader()
            writer.writerows(scraped_data)

        end_time = datetime.datetime.now()
        elapsed = end_time - start_time
        elapsed_seconds = int(elapsed.total_seconds())
        minutes, seconds = divmod(elapsed_seconds, 60)
        print(f"--- Processing and CSV creation took: {elapsed_seconds} seconds ({minutes}m {seconds}s) ---")

        return filename

    except Exception as e:
        print(f"Critical Error: {e}")
        return None

    finally:
        # 8. Close browser, reset memory, print message
        print("--- 8. Cleaning up ---")
        driver.quit()
        gc.collect()
        print("End of process message: Scraping Completed Successfully.")

if __name__ == "__main__":
    csv_file = "ads_keywords.csv"
    
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            keyword = row['keyword']
            print(f"\n{'='*50}")
            print(f"Processing keyword: {keyword}")
            print(f"{'='*50}")
            main(keyword, country="BR")