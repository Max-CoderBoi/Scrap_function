from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import os
import time
import logging
import csv
from pathlib import Path
from typing import List

# Configuration
BASE_URL = "https://gwdata.cgwb.gov.in/WaterLevel/DWLR"
DOWNLOAD_DIR = "CGWB"
OUTPUT_CSV = "Coimbatore_CGWB_All_Data.csv"
AGENCY = "CGWB"
STATE = "Tamil Nadu"
DISTRICT = "Coimbatore"

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
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create download directory
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)


def process_downloaded_csv(download_path: Path, well_info: dict, csv_writer, headers_written: list) -> bool:
    """Read downloaded CSV and append to master CSV with additional columns."""
    try:
        with open(download_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Write headers on first file
            if not headers_written[0]:
                fieldnames = ['Well_ID', 'Village', 'Latitude', 'Longitude', 'Block'] + list(reader.fieldnames)
                csv_writer.writerow(fieldnames)
                headers_written[0] = True
            
            # Write data rows with additional columns
            for row in reader:
                csv_writer.writerow([
                    well_info['well_id'], 
                    well_info['village'],
                    well_info['latitude'], 
                    well_info['longitude'], 
                    well_info['block']
                ] + list(row.values()))
        
        # Delete the temporary file
        download_path.unlink()
        return True
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        return False


def scrape_coimbatore_data():
    """Main scraping function for Coimbatore district - all wells at once."""
    stats = {'wells_found': 0, 'wells_downloaded': 0, 'wells_failed': 0}
    
    # Open master CSV file
    output_path = Path(DOWNLOAD_DIR) / OUTPUT_CSV
    csv_file = open(output_path, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    headers_written = [False]  # Using list to pass by reference
    
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
                
                # Select Agency
                logger.info(f"✅ Selecting Agency: {AGENCY}")
                agency_dropdown = page.locator(SELECTORS['agency'])
                agency_dropdown.wait_for(state="visible", timeout=10000)
                agency_dropdown.select_option(label=AGENCY)
                time.sleep(2)
                
                # Select Project - All
                logger.info(f"✅ Selecting Project: --All--")
                project_dropdown = page.locator(SELECTORS['project'])
                project_dropdown.wait_for(state="visible", timeout=10000)
                try:
                    # Try to select by label "--All--"
                    project_dropdown.select_option(label="--All--")
                except:
                    # If that fails, select first non-empty option
                    project_dropdown.select_option(index=1)
                time.sleep(2)
                
                # Wait for state dropdown
                logger.info("⏳ Waiting for States to populate...")
                time.sleep(3)
                
                # Select State
                logger.info(f"✅ Selecting State: {STATE}")
                page.locator(SELECTORS['state']).select_option(label=STATE)
                time.sleep(2)
                
                # Wait for districts
                page.wait_for_function(
                    f"""
                    () => {{
                        const el = document.querySelector("{SELECTORS['district']}");
                        return el && el.options.length >= 2;
                    }}
                    """,
                    timeout=10000
                )
                time.sleep(1)
                
                # Select District
                logger.info(f"✅ Selecting District: {DISTRICT}")
                page.locator(SELECTORS['district']).select_option(label=DISTRICT)
                time.sleep(2)
                
                # Click Filter button to load ALL wells (without selecting block)
                logger.info(f"🔍 Clicking Filter button to load all wells...")
                try:
                    filter_button = page.locator("button:has-text('Filter')")
                    filter_button.click()
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"Error clicking Filter: {e}")
                    return
                
                # Wait for table to load
                logger.info(f"⏳ Waiting for wells table to load...")
                page.wait_for_selector("table tbody tr", timeout=15000)
                time.sleep(2)
                
                # Change table length to 50
                logger.info(f"📊 Changing table display to 50 entries...")
                try:
                    length_selector = page.locator("select[name='simpletable_length']")
                    length_selector.wait_for(state="visible", timeout=5000)
                    length_selector.select_option(value="50")
                    time.sleep(2)
                    logger.info(f"   ✅ Table length set to 50")
                except Exception as e:
                    logger.warning(f"   ⚠  Could not change table length: {e}")
                    logger.info(f"   Continuing with default table length...")
                
                # Get all radio buttons in the table
                radio_buttons = page.locator("table tbody tr input[type='radio']").all()
                stats['wells_found'] = len(radio_buttons)
                
                if stats['wells_found'] == 0:
                    logger.error(f"❌ No wells found for {DISTRICT}")
                    return
                
                logger.info(f"📍 Found {stats['wells_found']} well(s) in {DISTRICT}")
                logger.info(f"🚀 Starting to process all wells...\n")
                
                # Process each well
                for idx in range(1, stats['wells_found'] + 1):
                    try:
                        logger.info(f"[{idx}/{stats['wells_found']}] Processing well...")
                        
                        # Re-fetch radio buttons each time (DOM might refresh)
                        current_radio_buttons = page.locator("table tbody tr input[type='radio']").all()
                        if idx > len(current_radio_buttons):
                            logger.warning(f"   ⚠  Well {idx} not found in table")
                            stats['wells_failed'] += 1
                            continue
                        
                        radio = current_radio_buttons[idx - 1]
                        
                        # Click the radio button
                        radio.scroll_into_view_if_needed()
                        radio.click()
                        time.sleep(1)
                        
                        # Wait for the tabular view section to load
                        page.wait_for_selector("button:has-text('Export')", timeout=5000)
                        
                        # Click on Tabular View tab if needed
                        tabular_tab = page.locator("a:has-text('Tabular View'), button:has-text('Tabular View')")
                        if tabular_tab.is_visible():
                            tabular_tab.click()
                            time.sleep(1)
                        
                        # Get well information from the row
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
                            
                            # Extract information based on table columns
                            # Column order: [Radio], WellNo, Village, Lat, Long, Data Availability
                            if len(cells) >= 5:
                                well_info['well_id'] = cells[1].strip() or f'well_{idx}'  # WellNo
                                well_info['village'] = cells[2].strip() or 'Unknown'       # Village
                                well_info['latitude'] = cells[3].strip() or 'Unknown'      # Lat
                                well_info['longitude'] = cells[4].strip() or 'Unknown'     # Long
                            
                            logger.info(f"   Well ID: {well_info['well_id']}")
                            logger.info(f"   Village: {well_info['village']}")
                            logger.info(f"   Lat: {well_info['latitude']}, Long: {well_info['longitude']}")
                        except Exception as e:
                            logger.warning(f"   Could not extract well info: {e}")
                        
                        # Click Export button
                        export_btn = page.locator("button:has-text('Export')")
                        
                        # Temporary download path
                        temp_download_path = Path(DOWNLOAD_DIR) / f"temp_{idx}.csv"
                        
                        with page.expect_download(timeout=15000) as download_info:
                            export_btn.click()
                        
                        download = download_info.value
                        download.save_as(temp_download_path)
                        
                        # Process and append to master CSV
                        if process_downloaded_csv(temp_download_path, well_info, csv_writer, headers_written):
                            logger.info(f"   ✅ Added to master CSV")
                            stats['wells_downloaded'] += 1
                        else:
                            logger.warning(f"   ⚠  Failed to process well data")
                            stats['wells_failed'] += 1
                        
                        # Click the list button to return to table view
                        try:
                            list_btn = page.locator("button.btn-list")
                            if list_btn.is_visible(timeout=2000):
                                list_btn.click()
                                logger.info(f"   🔙 Returned to table view")
                                time.sleep(2)
                            else:
                                logger.warning(f"   ⚠  List button not found, trying alternatives...")
                                # Try alternative selectors
                                alt_selectors = [
                                    "button:has-text('Close')",
                                    "button.btn-primary.btn-list",
                                    "i.feather.icon-menu >> xpath=.."
                                ]
                                for selector in alt_selectors:
                                    btn = page.locator(selector)
                                    if btn.is_visible(timeout=1000):
                                        btn.click()
                                        logger.info(f"   🔙 Closed view with alternative selector")
                                        time.sleep(2)
                                        break
                        except Exception as e:
                            logger.warning(f"   ⚠  Could not return to table: {e}")
                        
                        # Wait for table to be visible again
                        try:
                            page.wait_for_selector("table tbody tr", timeout=5000)
                            time.sleep(1)
                        except:
                            logger.warning(f"   ⚠  Table not immediately visible")
                        
                        logger.info("")
                        
                    except PlaywrightTimeout:
                        logger.warning(f"   ⚠  Timeout processing well {idx}\n")
                        stats['wells_failed'] += 1
                        continue
                    except Exception as e:
                        logger.error(f"   ❌ Error processing well {idx}: {e}\n")
                        stats['wells_failed'] += 1
                        continue
                
                # Print summary
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


if __name__ == "__main__":
    scrape_coimbatore_data()