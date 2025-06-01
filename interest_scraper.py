"""
Interest.co.nz Mortgage Rate Scraper with Enhanced Database Logging

This script scrapes mortgage rates from interest.co.nz and updates a database.
It includes comprehensive error logging for database operations.
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
RATE_TABLE_ID = "interest_financial_datatable"

# Bank name mapping
BANK_MAPPING = {
    "ANZ": "ANZ",
    "ASB": "ASB",
    "BNZ": "BNZ",
    "Kiwibank": "Kiwibank",
    "Westpac": "Westpac",
    "Co-operative Bank": "Co-operative Bank",
    "SBS Bank": "SBS Bank",
    "TSB": "TSB",
    "HSBC": "HSBC"
}

# Tenor mapping
TENOR_MAPPING = {
    "Variable floating": {"name": "Floating", "months": 1},
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
        
        # Cloud SQL Proxy connection string
        db_url = f"postgresql+pg8000://{db_user}:{db_pass}@/{db_name}?unix_sock={db_socket_path}/.s.PGSQL.5432"
        
        # Create engine
        engine = create_engine(db_url, pool_size=5, max_overflow=2)
        
        # Test connection
        with engine.connect() as connection:
            logger.info("Successfully connected to the database!")
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

def parse_rates(html_content):
    """Parse mortgage rates from HTML content."""
    rates = []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the mortgage rates table
        table = soup.find('table', id=RATE_TABLE_ID)
        if not table:
            logger.error(f"Could not find table with ID: {RATE_TABLE_ID}")
            # Try to find any table as a fallback
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables on the page")
            if tables:
                table = tables[0]
                logger.info("Using first table as fallback")
            else:
                return rates
        
        # Process each row in the table
        rows = table.find_all('tr')
        current_bank = None
        
        for row in rows:
            # Check if this is a bank header row
            bank_img = row.find('img')
            if bank_img and bank_img.get('title'):
                bank_name = bank_img.get('title')
                # Map to standard bank name
                current_bank = BANK_MAPPING.get(bank_name, bank_name)
                logger.info(f"Found bank: {current_bank}")
                continue
            
            # Skip rows without a bank
            if not current_bank:
                continue
            
            # Extract rates from the row
            cells = row.find_all('td')
            if len(cells) >= 2:
                tenor_cell = cells[0].get_text().strip()
                
                # Handle special case for 18-month rates which might be in a different format
                if "18" in tenor_cell and "month" in tenor_cell.lower():
                    tenor = "18 months"
                else:
                    tenor = tenor_cell
                
                # Skip rows without a recognized tenor
                if tenor not in TENOR_MAPPING:
                    continue
                
                # Extract standard rate
                standard_rate_cell = cells[1].get_text().strip()
                standard_rate_match = re.search(r'(\d+\.\d+)', standard_rate_cell)
                if standard_rate_match:
                    standard_rate = float(standard_rate_match.group(1))
                    rates.append({
                        "bank": current_bank,
                        "tenor": tenor,
                        "rate_type": "Standard",
                        "rate": standard_rate
                    })
                    logger.info(f"Extracted rate: {current_bank}, {tenor}, Standard, {standard_rate}")
                
                # Extract special rate if available
                if len(cells) >= 3:
                    special_rate_cell = cells[2].get_text().strip()
                    special_rate_match = re.search(r'(\d+\.\d+)', special_rate_cell)
                    if special_rate_match:
                        special_rate = float(special_rate_match.group(1))
                        rates.append({
                            "bank": current_bank,
                            "tenor": tenor,
                            "rate_type": "Special",
                            "rate": special_rate
                        })
                        logger.info(f"Extracted rate: {current_bank}, {tenor}, Special, {special_rate}")
    
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
            for rate_data in processed_rates:
                bank_name = rate_data["bank"]
                tenor_name = TENOR_MAPPING[rate_data["tenor"]]["name"]
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
        return
    
    # Parse rates from HTML
    rates = parse_rates(html_content)
    
    # Process rates to find lowest per bank/tenor
    processed_rates = process_rates(rates)
    
    # Connect to database
    logger.info("Connecting to database")
    engine = get_db_connection()
    if not engine:
        logger.error("Failed to connect to database")
        return
    
    # Update database with processed rates
    updated_count = update_database(processed_rates, engine)
    
    logger.info(f"Mortgage rate scraper completed. Updated {updated_count} rates.")

if __name__ == "__main__":
    main()
