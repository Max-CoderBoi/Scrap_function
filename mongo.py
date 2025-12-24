import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import logging

# Configuration
MONGODB_URI = process.env.MONGO_URL;
DATABASE_NAME = "Jal_Shakti"
COLLECTION_NAME = "well_data"
CSV_FILE = "CGWB/Coimbatore_CGWB_2025_Merged.csv"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def connect_to_mongodb(mongo_url):
    """Connect to MongoDB and return database and collection."""
    try:
        client = MongoClient(mongo_url)
        # Test connection
        client.server_info()
        logger.info(f"✅ Connected to MongoDB at {mongo_url}")
        
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # Create unique index on wellId
        collection.create_index("wellId", unique=True)
        logger.info(f"✅ Using database: {DATABASE_NAME}, collection: {COLLECTION_NAME}")
        
        return collection
    except Exception as e:
        logger.error(f"❌ Failed to connect to MongoDB: {e}")
        raise


def parse_coordinates(coord_str):
    """Parse coordinates string '[10.525, 76.9994]' to list of floats."""
    try:
        # Remove brackets and quotes, split by comma
        coord_str = coord_str.strip().strip('"').strip("'")
        coord_str = coord_str.strip('[]')
        parts = coord_str.split(',')
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        return [lat, lon]
    except:
        return None


def import_well_data(collection, csv_file):
    """Read CSV and import/update data in MongoDB."""
    
    logger.info(f"📂 Reading CSV file: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        logger.error(f"❌ Failed to read CSV: {e}")
        return
    
    logger.info(f"📊 Found {len(df)} wells in CSV")
    
    # Statistics
    stats = {
        'inserted': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    for idx, row in df.iterrows():
        try:
            # Skip if coordinates are Unknown
            if (pd.isna(row['Latitude']) or 
                pd.isna(row['Longitude']) or 
                row['Latitude'] == 'Unknown' or 
                row['Longitude'] == 'Unknown'):
                logger.warning(f"⚠ Skipping {row['Well_ID']}: Unknown coordinates")
                stats['skipped'] += 1
                continue
            
            # Skip if village is Unknown
            if pd.isna(row['Village']) or row['Village'] == 'Unknown':
                logger.warning(f"⚠ Skipping {row['Well_ID']}: Unknown village")
                stats['skipped'] += 1
                continue
            
            # Parse coordinates
            lat = float(row['Latitude'])
            lon = float(row['Longitude'])
            
            # Parse coordinates array if available
            coords = None
            if 'coordinates' in row and pd.notna(row['coordinates']):
                coords = parse_coordinates(row['coordinates'])
            
            # If parsing failed or not available, create from lat/lon
            if coords is None:
                coords = [lat, lon]
            
            # Prepare document
            well_doc = {
                'wellId': str(row['Well_ID']),
                'village': str(row['Village']),
                'latitude': lat,
                'longitude': lon,
                'coordinates': coords
            }
            
            # Add water level data (only if not NaN)
            if 'Jan' in row and pd.notna(row['Jan']):
                well_doc['january'] = float(row['Jan'])
            
            if 'Apr' in row and pd.notna(row['Apr']):
                well_doc['april'] = float(row['Apr'])
            
            if 'Aug' in row and pd.notna(row['Aug']):
                well_doc['august'] = float(row['Aug'])
            
            if 'Nov' in row and pd.notna(row['Nov']):
                well_doc['november'] = float(row['Nov'])
            
            # Insert or update in MongoDB
            result = collection.update_one(
                {'wellId': well_doc['wellId']},
                {'$set': well_doc},
                upsert=True
            )
            
            if result.upserted_id:
                stats['inserted'] += 1
                logger.info(f"✅ Inserted: {well_doc['wellId']} - {well_doc['village']}")
            else:
                stats['updated'] += 1
                logger.info(f"🔄 Updated: {well_doc['wellId']} - {well_doc['village']}")
            
            # Show progress every 10 wells
            if (stats['inserted'] + stats['updated']) % 10 == 0:
                logger.info(f"   Progress: {stats['inserted'] + stats['updated']}/{len(df)} processed")
        
        except Exception as e:
            logger.error(f"❌ Error processing {row['Well_ID']}: {e}")
            stats['errors'] += 1
            continue
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("🏁 IMPORT COMPLETED")
    logger.info(f"✅ Inserted: {stats['inserted']}")
    logger.info(f"🔄 Updated: {stats['updated']}")
    logger.info(f"⚠ Skipped: {stats['skipped']}")
    logger.info(f"❌ Errors: {stats['errors']}")
    logger.info(f"📊 Total in database: {collection.count_documents({})}")
    logger.info("="*60)


def verify_data(collection):
    """Verify imported data with sample queries."""
    logger.info("\n📋 Sample data from database:")
    
    # Get first 3 documents
    sample_docs = collection.find().limit(3)
    
    for doc in sample_docs:
        logger.info(f"\n  Well ID: {doc['wellId']}")
        logger.info(f"  Village: {doc['village']}")
        logger.info(f"  Coordinates: {doc['coordinates']}")
        logger.info(f"  Water Levels: Jan={doc.get('january', 'N/A')}, "
                   f"Apr={doc.get('april', 'N/A')}, "
                   f"Aug={doc.get('august', 'N/A')}, "
                   f"Nov={doc.get('november', 'N/A')}")


def main():
    """Main function to run the import."""
    try:
        # Connect to MongoDB
        collection = connect_to_mongodb(MONGODB_URI)
        
        # Import data from CSV
        import_well_data(collection, CSV_FILE)
        
        # Verify data
        verify_data(collection)
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
