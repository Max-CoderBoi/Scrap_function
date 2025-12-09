# ============================================================================
# COMPLETE MERGED SCRIPT: CGWB Coimbatore Data Pipeline (Scrape → Filter → Merge → MongoDB)
# ============================================================================

import os
import time
import logging
import csv
from pathlib import Path
from typing import List

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import pandas as pd
from pymongo import MongoClient

# ================================
# CONFIGURATION (Shared)
# ================================

BASE_URL = "https://gwdata.cgwb.gov.in/WaterLevel/DWLR"
DOWNLOAD_DIR = "CGWB"
OUTPUT_CSV_ALL = "Coimbatore_CGWB_All_Data.csv"
OUTPUT_CSV_FILTERED = "Coimbatore_CGWB_2025_Filtered.csv"
OUTPUT_CSV_MERGED = "Coimbatore_CGWB_2025_Merged.csv"

AGENCY = "CGWB"
STATE = "Tamil Nadu"
DISTRICT = "Coimbatore"

# Months to keep (January, April, August, November)
MONTHS_TO_KEEP = [1, 4, 8, 11]
YEAR = 2025

# MongoDB Configuration
MONGODB_URI = "mongodb+srv://akash_patil:akash@jalshaktidb.mzfi3l0.mongodb.net/"
DATABASE_NAME = "Jal_Shakti"
COLLECTION_NAME = "well_data"
CSV_FILE_FOR_MONGODB = f"CGWB/{OUTPUT_CSV_MERGED}"

# Selectors
SELECTORS = {
    'agency': '#agency',
    'project': '#ProjectId',
    'state': '#StateCode',
    'district': '#DistrictCode',
    'block': '#BlockCode',
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create download directory
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================================
# SCRIPT 1: WEB SCRAPER WITH LAT/LONG
# ============================================================================

def process_downloaded_csv(download_path: Path, well_info: dict, csv_writer, headers_written: list) -> bool:
    """Read downloaded CSV and append to master CSV with additional columns."""
    try:
        with open(download_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            if not headers_written[0]:
                fieldnames = ['Well_ID', 'Village', 'Latitude', 'Longitude', 'Block'] + list(reader.fieldnames)
                csv_writer.writerow(fieldnames)
                headers_written[0] = True
            
            for row in reader:
                csv_writer.writerow([
                    well_info['well_id'], 
                    well_info['village'],
                    well_info['latitude'], 
                    well_info['longitude'], 
                    well_info['block']
                ] + list(row.values()))
        
        download_path.unlink()
        return True
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        return False


def scrape_coimbatore_data():
    """Main scraping function for Coimbatore district - all wells at once."""
    stats = {'wells_found': 0, 'wells_downloaded': 0, 'wells_failed': 0}
    
    output_path = Path(DOWNLOAD_DIR) / OUTPUT_CSV_ALL
    csv_file = open(output_path, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    headers_written = [False]
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            
            try:
                logger.info("🌐 Opening website...")
                page.goto(BASE_URL, timeout=60000)
                page.wait_for_load_state("networkidle", timeout=30000)
                time.sleep(3)
                
                logger.info(f"✅ Selecting Agency: {AGENCY}")
                agency_dropdown = page.locator(SELECTORS['agency'])
                agency_dropdown.wait_for(state="visible", timeout=10000)
                agency_dropdown.select_option(label=AGENCY)
                time.sleep(2)
                
                logger.info(f"✅ Selecting Project: --All--")
                project_dropdown = page.locator(SELECTORS['project'])
                project_dropdown.wait_for(state="visible", timeout=10000)
                try:
                    project_dropdown.select_option(label="--All--")
                except:
                    project_dropdown.select_option(index=1)
                time.sleep(2)
                
                time.sleep(3)
                
                logger.info(f"✅ Selecting State: {STATE}")
                page.locator(SELECTORS['state']).select_option(label=STATE)
                time.sleep(2)
                
                page.wait_for_function(
                    f"""() => {{
                        const el = document.querySelector("{SELECTORS['district']}");
                        return el && el.options.length >= 2;
                    }}""",
                    timeout=10000
                )
                time.sleep(1)
                
                logger.info(f"✅ Selecting District: {DISTRICT}")
                page.locator(SELECTORS['district']).select_option(label=DISTRICT)
                time.sleep(2)
                
                logger.info(f"🔍 Clicking Filter button to load all wells...")
                try:
                    filter_button = page.locator("button:has-text('Filter')")
                    filter_button.click()
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"Error clicking Filter: {e}")
                    return
                
                logger.info(f"⏳ Waiting for wells table to load...")
                page.wait_for_selector("table tbody tr", timeout=15000)
                time.sleep(2)
                
                try:
                    length_selector = page.locator("select[name='simpletable_length']")
                    length_selector.wait_for(state="visible", timeout=5000)
                    length_selector.select_option(value="50")
                    time.sleep(2)
                except Exception as e:
                    logger.warning(f"Could not change table length: {e}")
                
                radio_buttons = page.locator("table tbody tr input[type='radio']").all()
                stats['wells_found'] = len(radio_buttons)
                
                if stats['wells_found'] == 0:
                    logger.error(f"❌ No wells found for {DISTRICT}")
                    return
                
                logger.info(f"📍 Found {stats['wells_found']} well(s) in {DISTRICT}")
                logger.info(f"🚀 Starting to process all wells...\n")
                
                for idx in range(1, stats['wells_found'] + 1):
                    try:
                        logger.info(f"[{idx}/{stats['wells_found']}] Processing well...")
                        
                        current_radio_buttons = page.locator("table tbody tr input[type='radio']").all()
                        if idx > len(current_radio_buttons):
                            logger.warning(f"   ⚠ Well {idx} not found in table")
                            stats['wells_failed'] += 1
                            continue
                        
                        radio = current_radio_buttons[idx - 1]
                        radio.scroll_into_view_if_needed()
                        radio.click()
                        time.sleep(1)
                        
                        page.wait_for_selector("button:has-text('Export')", timeout=5000)
                        
                        tabular_tab = page.locator("a:has-text('Tabular View'), button:has-text('Tabular View')")
                        if tabular_tab.is_visible():
                            tabular_tab.click()
                            time.sleep(1)
                        
                        well_info = {
                            'well_id': f'well_{idx}', 
                            'village': 'Unknown',
                            'latitude': 'Unknown', 
                            'longitude': 'Unknown',
                            'block': 'Unknown'
                        }
                        try:
                            row = radio.locator("xpath=ancestor::tr")
                            cells = row.locator("td").all_text_contents()
                            if len(cells) >= 5:
                                well_info['well_id'] = cells[1].strip() or f'well_{idx}'
                                well_info['village'] = cells[2].strip() or 'Unknown'
                                well_info['latitude'] = cells[3].strip() or 'Unknown'
                                well_info['longitude'] = cells[4].strip() or 'Unknown'
                            
                            logger.info(f"   Well ID: {well_info['well_id']}")
                            logger.info(f"   Village: {well_info['village']}")
                            logger.info(f"   Lat: {well_info['latitude']}, Long: {well_info['longitude']}")
                        except Exception as e:
                            logger.warning(f"   Could not extract well info: {e}")
                        
                        export_btn = page.locator("button:has-text('Export')")
                        temp_download_path = Path(DOWNLOAD_DIR) / f"temp_{idx}.csv"
                        
                        with page.expect_download(timeout=15000) as download_info:
                            export_btn.click()
                        
                        download = download_info.value
                        download.save_as(temp_download_path)
                        
                        if process_downloaded_csv(temp_download_path, well_info, csv_writer, headers_written):
                            logger.info(f"   ✅ Added to master CSV")
                            stats['wells_downloaded'] += 1
                        else:
                            logger.warning(f"   ⚠ Failed to process well data")
                            stats['wells_failed'] += 1
                        
                        try:
                            list_btn = page.locator("button.btn-list")
                            if list_btn.is_visible(timeout=2000):
                                list_btn.click()
                                time.sleep(2)
                            else:
                                alt_selectors = [
                                    "button:has-text('Close')",
                                    "button.btn-primary.btn-list",
                                    "i.feather.icon-menu >> xpath=.."
                                ]
                                for selector in alt_selectors:
                                    btn = page.locator(selector)
                                    if btn.is_visible(timeout=1000):
                                        btn.click()
                                        time.sleep(2)
                                        break
                        except Exception as e:
                            logger.warning(f"   ⚠ Could not return to table: {e}")
                        
                        try:
                            page.wait_for_selector("table tbody tr", timeout=5000)
                            time.sleep(1)
                        except:
                            pass
                        
                        logger.info("")
                        
                    except PlaywrightTimeout:
                        logger.warning(f"   ⚠ Timeout processing well {idx}\n")
                        stats['wells_failed'] += 1
                        continue
                    except Exception as e:
                        logger.error(f"   ❌ Error processing well {idx}: {e}\n")
                        stats['wells_failed'] += 1
                        continue
                
                logger.info("\n" + "="*60)
                logger.info("🏁 SCRAPING COMPLETED")
                logger.info(f"📊 Total wells found: {stats['wells_found']}")
                logger.info(f"✅ Successfully downloaded: {stats['wells_downloaded']}")
                logger.info(f"❌ Failed: {stats['wells_failed']}")
                logger.info(f"📁 Output file: {output_path}")
                logger.info("="*60)
                
            except Exception as e:
                logger.error(f"Fatal error during scraping: {e}", exc_info=True)
            finally:
                browser.close()
                
    finally:
        csv_file.close()


# ============================================================================
# SCRIPT 2: DATA FILTER (2025 + SPECIFIC MONTHS)
# ============================================================================

def filter_cgwb_data():
    print("\n" + "="*60)
    print("STARTING DATA FILTERING FOR 2025")
    print("="*60)

    INPUT_FILE = f"{DOWNLOAD_DIR}/{OUTPUT_CSV_ALL}"
    OUTPUT_FILE = f"{DOWNLOAD_DIR}/{OUTPUT_CSV_FILTERED}"

    path = Path(INPUT_FILE)
    if not path.exists() or path.stat().st_size == 0:
        print(f"❌ ERROR: Input file missing or empty: {INPUT_FILE}")
        return False

    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"❌ ERROR reading CSV: {e}")
        return False

    print(f"📊 Original data: {len(df)} rows")

    required_cols = ["Date", "Well_ID"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing columns: {missing_cols}")
        return False

    has_latlong = "Latitude" in df.columns and "Longitude" in df.columns
    if has_latlong:
        print("✅ Latitude and Longitude columns detected")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    df_2025 = df[df["Date"].dt.year == YEAR].copy()
    print(f"   Rows in {YEAR}: {len(df_2025)}")

    if len(df_2025) == 0:
        print(f"❌ No data for {YEAR}")
        return False

    df_filtered = df_2025[df_2025["Date"].dt.month.isin(MONTHS_TO_KEEP)]
    print(f"   Rows after month filter: {len(df_filtered)}")

    if len(df_filtered) == 0:
        print("❌ No matching months")
        return False

    df_filtered["YearMonth"] = df_filtered["Date"].dt.to_period("M")

    def pick_mid_month(group):
        target = 15
        group["day_diff"] = abs(group["Date"].dt.day - target)
        return group.loc[group["day_diff"].idxmin()]

    df_final = (
        df_filtered.groupby(["Well_ID", "YearMonth"], as_index=False)
        .apply(pick_mid_month)
        .reset_index(drop=True)
    )
    df_final = df_final.drop(columns=["YearMonth", "day_diff"], errors="ignore")

    if has_latlong:
        metadata_cols = ["Well_ID", "Village", "Latitude", "Longitude", "Block", "Date"]
        existing_metadata = [col for col in metadata_cols if col in df_final.columns]
        other_cols = [col for col in df_final.columns if col not in existing_metadata]
        df_final = df_final[existing_metadata + other_cols]

    df_final = df_final.sort_values(["Well_ID", "Date"])
    df_final["Date"] = df_final["Date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n💾 Saving filtered data → {OUTPUT_FILE}")
    df_final.to_csv(OUTPUT_FILE, index=False)
    print("✅ FILTERING COMPLETE")
    return True


# ============================================================================
# SCRIPT 3: DATA MERGER (PIVOT TO WIDE FORMAT)
# ============================================================================

def merge_well_readings():
    print("\n" + "="*60)
    print("STARTING DATA MERGING (PIVOT)")
    print("="*60)

    INPUT_FILE = f"{DOWNLOAD_DIR}/{OUTPUT_CSV_FILTERED}"
    OUTPUT_FILE = f"{DOWNLOAD_DIR}/{OUTPUT_CSV_MERGED}"

    path = Path(INPUT_FILE)
    if not path.exists():
        print(f"❌ File not found: {INPUT_FILE}")
        return False

    df = pd.read_csv(INPUT_FILE)
    if df.empty:
        print("❌ CSV is empty")
        return False

    print(f"📊 Loaded {len(df)} rows")

    has_coords = "Latitude" in df.columns and "Longitude" in df.columns
    has_village = "Village" in df.columns
    has_block = "Block" in df.columns

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    df["MonthName"] = df["Date"].dt.strftime("%b")

    metadata_cols = ["Well_ID"]
    if has_village:
        metadata_cols.append("Village")
    if has_coords:
        metadata_cols.extend(["Latitude", "Longitude"])
    if has_block:
        metadata_cols.append("Block")

    df_pivot = df.pivot_table(
        index="Well_ID",
        columns="MonthName",
        values="Water Level",
        aggfunc="first"
    ).reset_index()

    df_metadata = df.groupby("Well_ID", as_index=False).first()[metadata_cols]
    df_final = df_metadata.merge(df_pivot, on="Well_ID", how="left")

    month_order = ["Jan", "Apr", "Aug", "Nov"]
    existing_months = [m for m in month_order if m in df_final.columns]
    final_cols = metadata_cols + existing_months
    df_final = df_final.reindex(columns=final_cols)

    if has_coords:
        df_final["coordinates"] = df_final.apply(
            lambda row: [
                float(row["Latitude"]) if pd.notna(row["Latitude"]) and row["Latitude"] != "Unknown" else None,
                float(row["Longitude"]) if pd.notna(row["Longitude"]) and row["Longitude"] != "Unknown" else None
            ] if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]) else [None, None],
            axis=1
        )

    print(f"\n💾 Saving merged data → {OUTPUT_FILE}")
    df_final.to_csv(OUTPUT_FILE, index=False)
    print("✅ MERGING COMPLETE")
    return True


# ============================================================================
# SCRIPT 4: IMPORT TO MONGODB
# ============================================================================

def connect_to_mongodb(mongo_url):
    try:
        client = MongoClient(mongo_url)
        client.server_info()
        logger.info("Connected to MongoDB")

        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        collection.create_index("wellId", unique=True)
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def parse_coordinates(coord_str):
    try:
        coord_str = coord_str.strip().strip('"').strip("'")
        coord_str = coord_str.strip('[]')
        parts = coord_str.split(',')
        return [float(parts[0].strip()), float(parts[1].strip())]
    except:
        return None


def import_well_data(collection, csv_file):
    logger.info(f"Reading CSV for MongoDB import: {csv_file}")

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        logger.error(f"CSV read error: {e}")
        return

    logger.info(f"Found {len(df)} wells in CSV")

    stats = {
        'inserted': 0,
        'updated': 0,
        'unchanged': 0,
        'skipped': 0,
        'errors': 0
    }

    for idx, row in df.iterrows():
        try:
            if (
                pd.isna(row["Latitude"]) or
                pd.isna(row["Longitude"]) or
                row["Latitude"] == "Unknown" or
                row["Longitude"] == "Unknown"
            ):
                logger.warning(f"Skipping {row['Well_ID']}: Unknown coordinates")
                stats['skipped'] += 1
                continue

            if pd.isna(row["Village"]) or row["Village"] == "Unknown":
                logger.warning(f"Skipping {row['Well_ID']}: Unknown village")
                stats['skipped'] += 1
                continue

            lat = float(row["Latitude"])
            lon = float(row["Longitude"])

            coords = None
            if "coordinates" in row and pd.notna(row["coordinates"]):
                coords = parse_coordinates(row["coordinates"])
            if coords is None:
                coords = [lat, lon]

            well_doc = {
                "wellId": str(row["Well_ID"]),
                "village": str(row["Village"]),
                "latitude": lat,
                "longitude": lon,
                "coordinates": coords
            }

            water_cols = {
                "january": "Jan",
                "april": "Apr",
                "august": "Aug",
                "november": "Nov"
            }

            for key, csv_col in water_cols.items():
                if csv_col in row and pd.notna(row[csv_col]):
                    well_doc[key] = float(row[csv_col])

            result = collection.update_one(
                {"wellId": well_doc["wellId"]},
                {"$set": well_doc},
                upsert=True
            )

            if result.upserted_id:
                stats["inserted"] += 1
                logger.info(f"Inserted: {well_doc['wellId']} - {well_doc['village']}")
            elif result.modified_count > 0:
                stats["updated"] += 1
                logger.info(f"Updated: {well_doc['wellId']} - {well_doc['village']}")
            else:
                stats["unchanged"] += 1

            if (stats["inserted"] + stats["updated"]) % 10 == 0:
                logger.info(f"Progress: {idx + 1}/{len(df)} processed")

        except Exception as e:
            logger.error(f"Error processing {row['Well_ID']}: {e}")
            stats["errors"] += 1

    logger.info("\n" + "="*60)
    logger.info("IMPORT SUMMARY")
    logger.info(f"Inserted : {stats['inserted']}")
    logger.info(f"Updated  : {stats['updated']}")
    logger.info(f"Unchanged: {stats['unchanged']}")
    logger.info(f"Skipped  : {stats['skipped']}")
    logger.info(f"Errors   : {stats['errors']}")
    logger.info(f"Total DB count: {collection.count_documents({})}")
    logger.info("="*60)


def verify_data(collection):
    logger.info("\nSample Wells from DB:")
    for doc in collection.find().limit(3):
        logger.info(f"\nWell ID: {doc['wellId']}")
        logger.info(f"Village: {doc['village']}")
        logger.info(f"Coords : {doc['coordinates']}")
        logger.info(f"Water Levels: Jan={doc.get('january', 'N/A')}, "
                    f"Apr={doc.get('april', 'N/A')}, "
                    f"Aug={doc.get('august', 'N/A')}, "
                    f"Nov={doc.get('november', 'N/A')}")


# ============================================================================
# MAIN: RUN ALL STEPS SEQUENTIALLY
# ============================================================================

def main():
    logger.info("STARTING FULL CGWB COIMBATORE PIPELINE")
    
    # Step 1: Scrape
    logger.info("\n>>> STEP 1: WEB SCRAPING")
    scrape_coimbatore_data()
    
    # Step 2: Filter
    logger.info("\n>>> STEP 2: FILTERING 2025 DATA")
    if not filter_cgwb_data():
        logger.error("Filtering failed. Stopping pipeline.")
        return
    
    # Step 3: Merge/Pivot
    logger.info("\n>>> STEP 3: MERGING (PIVOT)")
    if not merge_well_readings():
        logger.error("Merging failed. Stopping pipeline.")
        return
    
    # Step 4: Import to MongoDB
    logger.info("\n>>> STEP 4: IMPORT TO MONGODB")
    try:
        collection = connect_to_mongodb(MONGODB_URI)
        import_well_data(collection, CSV_FILE_FOR_MONGODB)
        verify_data(collection)
    except Exception as e:
        logger.error(f"MongoDB import failed: {e}")

    logger.info("\n🎉 FULL PIPELINE COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    main()