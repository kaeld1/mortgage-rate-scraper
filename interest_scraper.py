"""
Interest.co.nz Mortgage Rate Scraper with Enhanced Parsing Logic

This script scrapes mortgage rates from interest.co.nz and updates a database.
It includes improved parsing logic to handle the current website structure.
"""

import os
import logging
import requests
import re
import traceback
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine, text, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
URL = "https://www.interest.co.nz/borrowing"

# Bank name mapping - expanded to handle variations in names
BANK_MAPPING = {
    "ANZ": "ANZ",
    "ASB": "ASB",
    "BNZ": "BNZ",
    "Kiwibank": "Kiwibank",
    "Westpac": "Westpac",
    "Co-operative Bank": "Co-operative Bank",
    "SBS Bank": "SBS Bank",
    "SBS": "SBS Bank",
    "TSB Bank": "TSB",
    "TSB": "TSB",
    "HSBC": "HSBC",
    "Heartland Bank": "Heartland Bank"
}

# Tenor mapping
TENOR_MAPPING = {
    "Variable floating": {"name": "Floating", "months": 1},
    "floating": {"name": "Floating", "months": 1},
    "6 months": {"name": "6 months", "months": 6},
    "1 year": {"name": "1 year", "months": 12},
    "18 months": {"name": "18 months", "months": 18},
    "2 years": {"name": "2 years", "months": 24},
    "3 years": {"name": "3 years", "months": 36},
    "4 years": {"name": "4 years", "months": 48},
    "5 years": {"name": "5 years", "months": 60}
}

def get_db_connection():
    """Create a database connection using environment variables."""
    try:
        db_user = os.environ.get("DB_USER")
        db_pass = os.environ.get("DB_PASS")
        db_name = os.environ.get("DB_NAME")
        instance_conn_name = os.environ.get("INSTANCE_CONNECTION_NAME")
        
        logger.info(f"Connecting to database: {db_name}")
        logger.info(f"Using instance connection name: {instance_conn_name}")
        
        # Print environment variables (without password)
        logger.info(f"DB_USER: {db_user}")
        logger.info(f"DB_NAME: {db_name}")
        logger.info(f"INSTANCE_CONNECTION_NAME: {instance_conn_name}")
        
        if not all([db_user, db_pass, db_name, instance_conn_name]):
            logger.error("Missing required environment variables for database connection")
            logger.error(f"DB_USER present: {bool(db_user)}")
            logger.error(f"DB_PASS present: {bool(db_pass)}")
            logger.error(f"DB_NAME present: {bool(db_name)}")
            logger.error(f"INSTANCE_CONNECTION_NAME present: {bool(instance_conn_name)}")
            return None
        
        # Create connection string
        db_socket_dir = "/cloudsql"
        db_socket_path = f"{db_socket_dir}/{instance_conn_name}"
        logger.info(f"Using socket path: {db_socket_path}")
        
        # Cloud SQL Proxy connection string
        db_url = f"postgresql+pg8000://{db_user}:{db_pass}@/{db_name}?unix_sock={db_socket_path}/.s.PGSQL.5432"
        logger.info("Database URL created (password hidden)")
        
        # Create engine
        logger.info("Creating database engine...")
        engine = create_engine(db_url, pool_size=5, max_overflow=2)
        
        # Test connection
        logger.info("Testing database connection...")
        with engine.connect() as connection:
            logger.info("Successfully connected to the database!")
            
            # Test query to verify schema
            try:
                logger.info("Testing query to verify schema...")
                banks_result = connection.execute(text("SELECT COUNT(*) FROM banks"))
                bank_count = banks_result.scalar()
                logger.info(f"Found {bank_count} banks in database")
                
                tenors_result = connection.execute(text("SELECT COUNT(*) FROM tenors"))
                tenor_count = tenors_result.scalar()
                logger.info(f"Found {tenor_count} tenors in database")
                
                rates_result = connection.execute(text("SELECT COUNT(*) FROM bank_rates"))
                rates_count = rates_result.scalar()
                logger.info(f"Found {rates_count} existing rates in database")
            except Exception as e:
                logger.error(f"Schema verification failed: {e}")
                logger.error(traceback.format_exc())
            
            return engine
            
    except SQLAlchemyError as e:
        logger.error(f"Database connection error: {e}")
        logger.error(f"SQLAlchemy error details: {str(e.__dict__)}")
        logger.error(traceback.format_exc())
        return None
    except Exception as e:
        logger.error(f"Unexpected error during database connection: {e}")
        logger.error(traceback.format_exc())
        return None

def fetch_data():
    """Fetch mortgage rate data from interest.co.nz."""
    try:
        logger.info(f"Fetching data from {URL}")
        response = requests.get(URL)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None

def extract_bank_name(row):
    """Extract bank name from a table row."""
    # Try to find bank name in image alt or title attributes
    img = row.find('img')
    if img:
        if img.get('alt'):
            return img.get('alt')
        if img.get('title'):
            return img.get('title')
    
    # Try to find bank name in text content of first cell
    first_cell = row.find('td')
    if first_cell and first_cell.get_text().strip():
        return first_cell.get_text().strip()
    
    # Try to find bank name in any cell with class 'inst-name'
    inst_name_cell = row.find('td', class_='inst-name')
    if inst_name_cell and inst_name_cell.get_text().strip():
        return inst_name_cell.get_text().strip()
    
    return None

def normalize_bank_name(raw_name):
    """Normalize bank name to match database entries."""
    if not raw_name:
        return None
    
    # Clean up the raw name
    name = raw_name.strip()
    
    # Remove common prefixes/suffixes
    prefixes = ["click to contact ", "click here to contact "]
    for prefix in prefixes:
        if name.lower().startswith(prefix):
            name = name[len(prefix):]
    
    suffixes = [" Home Loans %u2013 Apply now or find out more", " - creating futures."]
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    # Map to standard bank name
    return BANK_MAPPING.get(name, name)

def extract_rate(cell_text):
    """Extract numeric rate from cell text."""
    if not cell_text:
        return None
    
    # Find numeric rate pattern (e.g., 5.99)
    rate_match = re.search(r'(\d+\.\d+)', cell_text)
    if rate_match:
        try:
            return float(rate_match.group(1))
        except ValueError:
            return None
    
    return None

def extract_special_18month_rate(row):
    """Extract 18-month rate from special format."""
    for cell in row.find_all('td'):
        text = cell.get_text().strip()
        if "18 months =" in text:
            rate_match = re.search(r'18 months = (\d+\.\d+)', text)
            if rate_match:
                try:
                    return float(rate_match.group(1))
                except ValueError:
                    return None
    return None

def parse_rates(html_content):
    """Parse mortgage rates from HTML content with improved handling of the current website structure."""
    rates = []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all tables on the page
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables on the page")
        
        current_bank = None
        
        # Process each table
        for table_idx, table in enumerate(tables):
            # Skip tables that don't look like rate tables
            if not table.find('th') and not table.find('td', class_='inst-name'):
                continue
            
            logger.info(f"Processing table {table_idx}")
            
            # Process each row in the table
            rows = table.find_all('tr')
            for row in rows:
                # Check if this is a bank header row
                bank_name = extract_bank_name(row)
                if bank_name:
                    normalized_bank = normalize_bank_name(bank_name)
                    if normalized_bank:
                        current_bank = normalized_bank
                        logger.info(f"Found bank: {current_bank}")
                
                # Skip rows without a bank
                if not current_bank:
                    continue
                
                # Extract rates from the row
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                
                # First cell might contain product type
                product_type = cells[0].get_text().strip() if cells[0] else ""
                
                # Check for special 18-month rate format
                special_18month_rate = extract_special_18month_rate(row)
                if special_18month_rate:
                    rates.append({
                        "bank": current_bank,
                        "tenor": "18 months",
                        "rate_type": "Special" if "Special" in product_type else "Standard",
                        "rate": special_18month_rate
                    })
                    logger.info(f"Extracted rate: {current_bank}, 18 months, {'Special' if 'Special' in product_type else 'Standard'}, {special_18month_rate}")
                
                # Process standard tenor columns
                # Assuming columns follow this pattern: Product, Variable floating, 6 months, 1 year, etc.
                tenors = ["Variable floating", "6 months", "1 year", "2 years", "3 years", "4 years", "5 years"]
                
                # Skip the first cell (product type) and process the rest as potential rates
                for i, cell in enumerate(cells[1:], 1):
                    if i <= len(tenors):
                        tenor = tenors[i-1]
                        cell_text = cell.get_text().strip()
                        rate = extract_rate(cell_text)
                        
                        if rate:
                            rate_type = "Special" if "Special" in product_type else "Standard"
                            rates.append({
                                "bank": current_bank,
                                "tenor": tenor,
                                "rate_type": rate_type,
                                "rate": rate
                            })
                            logger.info(f"Extracted rate: {current_bank}, {tenor}, {rate_type}, {rate}")
    
    except Exception as e:
        logger.error(f"Error parsing rates: {e}")
        logger.error(traceback.format_exc())
    
    logger.info(f"Scraped {len(rates)} rates from interest.co.nz")
    return rates

def process_rates(rates):
    """Process rates to find the lowest rate per bank per tenor."""
    processed_rates = {}
    
    try:
        # Group rates by bank and tenor
        for rate_data in rates:
            bank = rate_data["bank"]
            tenor = rate_data["tenor"]
            rate_type = rate_data["rate_type"]
            rate = rate_data["rate"]
            
            key = (bank, tenor)
            
            if key not in processed_rates or rate < processed_rates[key]["rate"]:
                processed_rates[key] = {
                    "bank": bank,
                    "tenor": tenor,
                    "rate_type": rate_type,
                    "rate": rate
                }
        
        logger.info(f"Processed {len(processed_rates)} unique bank/tenor/rate_type combinations")
    except Exception as e:
        logger.error(f"Error processing rates: {e}")
        logger.error(traceback.format_exc())
    
    return list(processed_rates.values())

def update_database(processed_rates, engine):
    """Update database with processed rates."""
    if not engine:
        logger.error("Cannot update database: No database connection")
        return 0
    
    updated_count = 0
    
    try:
        # Create a session
        logger.info("Creating database session")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            logger.info("Starting database update transaction")
            
            # Get all banks from database
            logger.info("Fetching banks from database")
            banks_result = session.execute(text("SELECT id, name FROM banks"))
            banks = {row[1]: row[0] for row in banks_result}
            logger.info(f"Found {len(banks)} banks in database: {list(banks.keys())}")
            
            # Get all tenors from database
            logger.info("Fetching tenors from database")
            tenors_result = session.execute(text("SELECT id, name, months FROM tenors"))
            tenors = {row[1]: {"id": row[0], "months": row[2]} for row in tenors_result}
            logger.info(f"Found {len(tenors)} tenors in database: {list(tenors.keys())}")
            
            # Update rates
            logger.info(f"Processing {len(processed_rates)} rates for database update")
            for rate_data in processed_rates:
                bank_name = rate_data["bank"]
                
                # Map tenor name to database tenor name
                tenor_info = TENOR_MAPPING.get(rate_data["tenor"])
                if not tenor_info:
                    logger.warning(f"Tenor '{rate_data['tenor']}' not mapped, skipping")
                    continue
                
                tenor_name = tenor_info["name"]
                rate_value = rate_data["rate"]
                rate_type = rate_data["rate_type"]
                
                # Skip if bank or tenor not in database
                if bank_name not in banks:
                    logger.warning(f"Bank '{bank_name}' not found in database, skipping")
                    continue
                
                if tenor_name not in tenors:
                    logger.warning(f"Tenor '{tenor_name}' not found in database, skipping")
                    continue
                
                bank_id = banks[bank_name]
                tenor_id = tenors[tenor_name]["id"]
                
                try:
                    # Check if rate exists
                    logger.info(f"Checking if rate exists: {bank_name}, {tenor_name}, {rate_type}")
                    check_query = text("""
                        SELECT id FROM bank_rates 
                        WHERE bank_id = :bank_id AND tenor_id = :tenor_id AND rate_type = :rate_type
                    """)
                    
                    result = session.execute(check_query, {
                        "bank_id": bank_id,
                        "tenor_id": tenor_id,
                        "rate_type": rate_type
                    })
                    
                    rate_id = result.scalar()
                    
                    if rate_id:
                        # Update existing rate
                        logger.info(f"Updating rate: {bank_name}, {tenor_name}, {rate_type}, {rate_value}")
                        update_query = text("""
                            UPDATE bank_rates 
                            SET rate = :rate, updated_at = NOW() 
                            WHERE id = :id
                        """)
                        
                        session.execute(update_query, {
                            "rate": rate_value,
                            "id": rate_id
                        })
                    else:
                        # Insert new rate
                        logger.info(f"Inserting new rate: {bank_name}, {tenor_name}, {rate_type}, {rate_value}")
                        insert_query = text("""
                            INSERT INTO bank_rates (bank_id, tenor_id, rate, rate_type, updated_at)
                            VALUES (:bank_id, :tenor_id, :rate, :rate_type, NOW())
                        """)
                        
                        session.execute(insert_query, {
                            "bank_id": bank_id,
                            "tenor_id": tenor_id,
                            "rate": rate_value,
                            "rate_type": rate_type
                        })
                    
                    updated_count += 1
                
                except SQLAlchemyError as e:
                    logger.error(f"Error updating rate for {bank_name}, {tenor_name}, {rate_type}: {e}")
                    logger.error(f"SQL error details: {str(e.__dict__)}")
                    logger.error(traceback.format_exc())
            
            # Commit the transaction
            logger.info("Committing database transaction")
            session.commit()
            logger.info(f"Updated {updated_count} rates in database")
        
        except SQLAlchemyError as e:
            logger.error(f"Database transaction error: {e}")
            logger.error(f"SQLAlchemy error details: {str(e.__dict__)}")
            logger.error(traceback.format_exc())
            session.rollback()
            logger.info("Transaction rolled back")
        
        finally:
            session.close()
            logger.info("Database session closed")
    
    except Exception as e:
        logger.error(f"Unexpected error during database update: {e}")
        logger.error(traceback.format_exc())
    
    return updated_count

def main():
    """Main function to scrape and update mortgage rates."""
    logger.info("Starting mortgage rate scraper")
    
    # Fetch data from interest.co.nz
    html_content = fetch_data()
    if not html_content:
        logger.error("Failed to fetch data from interest.co.nz")
        return {"status": "error", "message": "Failed to fetch data"}
    
    # Parse rates from HTML
    rates = parse_rates(html_content)
    logger.info(f"Parsed {len(rates)} rates from HTML")
    
    # Process rates to find lowest per bank/tenor
    processed_rates = process_rates(rates)
    logger.info(f"Processed into {len(processed_rates)} unique bank/tenor combinations")
    
    # Connect to database - THIS IS THE CRITICAL PART
    logger.info("Connecting to database")
    engine = get_db_connection()
    
    if not engine:
        logger.error("Failed to connect to database")
        return {"status": "error", "message": "Failed to connect to database"}
    
    # Update database with processed rates
    logger.info("Updating database with processed rates")
    updated_count = update_database(processed_rates, engine)
    logger.info(f"Database update complete. Updated {updated_count} rates.")
    
    logger.info(f"Mortgage rate scraper completed. Updated {updated_count} rates.")
    return {"status": "success", "rates_updated": updated_count}

if __name__ == "__main__":
    main()
