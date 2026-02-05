import csv
import pymysql
import os
import json

# --- DATABASE CONFIGURATION ---
DB_CONFIG = {
    'host': '50.31.176.8',
    'user': 'zmcrbvch_jon',
    'password': 'Runero!54Alien32.',
    'database': 'zmcrbvch_emailHacks',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# --- CSV TO DB FIELD MAPPING ---
# Maps CSV column names to DB column names where they differ
FIELD_MAPPING = {
    'libraryID': 'LibraryID',
    'Keyword': 'keywords',
    'Duplicates': 'duplicates',
}

# All DB fields in correct order (excluding generated columns)
DB_FIELDS = [
    'cta_text', 'cta_type', '__html', 'page_profile_uri', 'publisherPlatform',
    'URLCreative', 'url_preview_creative', 'AdCreative', 'AdMedia', 'profilePict',
    'page_profile_picture_url', 'Active', 'Estatus', 'CollectionCount', 'CollationID',
    'startDate', 'endDate', 'LibraryID', 'ahref', 'pageName', 'pageID', 'AdDescription',
    'AdTitle', 'age', 'gender', 'languages', 'countries', 'daysSincePublication',
    'lazy_load', 'contains_details', 'domain', 'codeBelongs', 'keywords', 'duplicates',
    'createdAt', 'updatedAt'
    # Removed: 'AdDescription_plain', 'AdTitle_plain' - these are GENERATED columns
]

# Fields that should be integers
INT_FIELDS = {'CollectionCount', 'CollationID', 'startDate', 'endDate', 'daysSincePublication', 'duplicates'}

# Fields that should be booleans (stored as TINYINT)
BOOL_FIELDS = {'Active', 'Estatus', 'lazy_load', 'contains_details'}

# Fields that require valid JSON
JSON_FIELDS = {'publisherPlatform', 'AdDescription', 'AdTitle', 'age', 'languages', 'countries'}


def connect_db():
    """Establish database connection."""
    return pymysql.connect(**DB_CONFIG)


def read_csv(filepath: str) -> list:
    """Read CSV file and return list of rows as dictionaries."""
    rows = []
    with open(filepath, mode='r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
    return rows


def convert_value(value, field_name: str):
    """Convert value to appropriate type for database insertion."""
    
    # Handle JSON fields - must be valid JSON or NULL
    if field_name in JSON_FIELDS:
        if value == '' or value is None:
            # Return JSON array with default value based on field
            if field_name == 'publisherPlatform':
                return '["facebook"]'
            elif field_name in ('AdTitle', 'AdDescription'):
                return '["Sin titulo"]'
            elif field_name in ('age', 'languages', 'countries'):
                return '[]'
            return None
        
        # If already valid JSON, return as-is
        try:
            json.loads(value)
            return value
        except (json.JSONDecodeError, TypeError):
            # Wrap the value in a JSON array
            return json.dumps([str(value)])
    
    # Handle empty strings as NULL
    if value == '' or value is None:
        return None
    
    # Handle integer fields
    if field_name in INT_FIELDS:
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    # Handle boolean fields
    if field_name in BOOL_FIELDS:
        if isinstance(value, bool):
            return 1 if value else 0
        if str(value).lower() in ('true', '1', 'yes'):
            return 1
        if str(value).lower() in ('false', '0', 'no'):
            return 0
        return None
    
    return str(value)


def map_row_to_db(csv_row: dict) -> dict:
    """Map CSV row fields to database fields."""
    db_row = {}
    
    for db_field in DB_FIELDS:
        # Check if there's a mapping, otherwise use the same name
        csv_field = None
        for csv_key, db_key in FIELD_MAPPING.items():
            if db_key == db_field:
                csv_field = csv_key
                break
        
        if csv_field is None:
            csv_field = db_field
        
        # Get value from CSV row
        raw_value = csv_row.get(csv_field, None)
        db_row[db_field] = convert_value(raw_value, db_field)
    
    return db_row


def get_existing_library_ids(cursor, library_ids: list) -> set:
    """Check which LibraryIDs already exist in the database."""
    if not library_ids:
        return set()
    
    placeholders = ', '.join(['%s'] * len(library_ids))
    sql = f"SELECT LibraryID FROM adsdomains WHERE LibraryID IN ({placeholders})"
    cursor.execute(sql, library_ids)
    results = cursor.fetchall()
    return {str(row['LibraryID']) for row in results}


def insert_batch(cursor, batch: list):
    """Insert a batch of rows into the database, skipping existing LibraryIDs."""
    if not batch:
        return 0, 0
    
    # Get LibraryIDs from batch
    library_ids = [row.get('libraryID') or row.get('LibraryID') for row in batch]
    library_ids = [lid for lid in library_ids if lid]  # Filter out None/empty
    
    # Check which already exist
    existing_ids = get_existing_library_ids(cursor, library_ids)
    
    # Filter out rows that already exist
    new_rows = []
    skipped = 0
    for row in batch:
        lib_id = str(row.get('libraryID') or row.get('LibraryID') or '')
        if lib_id in existing_ids:
            skipped += 1
            continue
        new_rows.append(row)
    
    if not new_rows:
        return 0, skipped
    
    # Build INSERT statement
    placeholders = ', '.join(['%s'] * len(DB_FIELDS))
    columns = ', '.join([f'`{f}`' for f in DB_FIELDS])
    
    sql = f"INSERT INTO adsdomains ({columns}) VALUES ({placeholders})"
    
    # Prepare values
    values_list = []
    for row in new_rows:
        db_row = map_row_to_db(row)
        values = tuple(db_row[field] for field in DB_FIELDS)
        values_list.append(values)
    
    # Execute batch insert
    cursor.executemany(sql, values_list)
    return len(values_list), skipped


def main(csv_filepath: str = None):
    """Main function to process CSV and insert into database."""
    if csv_filepath is None:
        csv_filepath = "processed_20260204_172139_truque.csv"
    
    print(f"Reading CSV: {csv_filepath}")
    
    # Read CSV
    try:
        rows = read_csv(csv_filepath)
    except FileNotFoundError:
        print(f"Error: File '{csv_filepath}' not found.")
        return
    
    total_rows = len(rows)
    print(f"Found {total_rows} rows to process.")
    
    if total_rows == 0:
        print("No rows to insert.")
        return
    
    # Connect to database
    print("Connecting to database...")
    try:
        connection = connect_db()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return
    
    print("Connected successfully.")
    
    # Process in batches of 10
    batch_size = 10
    inserted_count = 0
    skipped_count = 0
    
    try:
        with connection.cursor() as cursor:
            for i in range(0, total_rows, batch_size):
                batch = rows[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                try:
                    count, skipped = insert_batch(cursor, batch)
                    inserted_count += count
                    skipped_count += skipped
                    print(f"Batch {batch_num}: Inserted {count}, Skipped {skipped} (Total: {inserted_count} inserted, {skipped_count} skipped)")
                except Exception as e:
                    print(f"Batch {batch_num} failed: {e}")
                    # Continue with next batch
                    continue
            
            # Commit all changes
            connection.commit()
            print(f"\nDone! Inserted {inserted_count} rows, Skipped {skipped_count} duplicates.")
    
    except Exception as e:
        print(f"Error during insertion: {e}")
        connection.rollback()
    
    finally:
        connection.close()
        print("Database connection closed.")


if __name__ == "__main__":
    import sys
    
    # Allow passing CSV filename as argument
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
