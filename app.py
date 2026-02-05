"""Orchestration script: runs scraper then processor."""
import csv
import os

from flashScraperGemini import main as scraper_main
from ads_processor import main as processor_main
from insertProcessedCsv import main as insert_main


def run():
    """Read keywords CSV, scrape each, then process the resulting CSV."""
    keywords_file = "ads_keywords.csv"

    with open(keywords_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            keyword = row['keyword']
            print(f"\n{'='*50}")
            print(f"Processing keyword: {keyword}")
            print(f"{'='*50}")

            # Step 1: Scrape ads and get CSV filename
            csv_file = scraper_main(keyword, country="BR")

            if csv_file:
                # Step 2: Process the scraped CSV
                print(f"\n--- Running ads_processor on {csv_file} ---")
                processed_file = processor_main(csv_file)
                
                # Step 3: Insert processed rows into database
                if processed_file and os.path.exists(processed_file):
                    print(f"\n--- Inserting {processed_file} into database ---")
                    insert_main(processed_file)
                else:
                    print(f"Processed file not found, skipping database insert.")
            else:
                print(f"Scraping failed for keyword '{keyword}', skipping processor.")


if __name__ == "__main__":
    run()
