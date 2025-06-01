"""
Interest.co.nz Mortgage Rate Scraper

This script scrapes mortgage rates from interest.co.nz/borrowing,
processes the data to find the lowest rates per bank per tenor,
and updates the database with the latest rates.
"""

import os
import logging
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pg8000
from google.cloud.sql.connector import Connector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection settings
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "mortgage_data")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME", "")

# Mapping of bank names as they appear on interest.co.nz to our database names
BANK_NAME_MAPPING = {
    "ANZ": "ANZ",
    "ASB": "ASB",
    "BNZ": "BNZ",
    "Westpac": "Westpac",
    "Kiwibank": "Kiwibank",
    "HSBC": "HSBC",
    "SBS": "SBS Bank",
    "TSB": "TSB",
    "Co-operative Bank": "Co-operative Bank",
    "Heartland Bank": "Heartland Bank",
    "ICBC": "ICBC",
    "Bank of China": "Bank of China",
    "China Construction Bank": "China Construction Bank",
    "Commonwealth Bank": "Commonwealth Bank",
    "NAB": "NAB"
}

# Mapping of tenor descriptions to months
TENOR_MAPPING = {
    "Floating": 0,
    "6 months": 6,
    "1 year": 12,
    "18 months": 18,
    "2 years": 24,
    "3 years": 36,
    "4 years": 48,
    "5 years": 60
}

def initialize_db_connection():
    """Initialize database connection using Cloud SQL Python Connector."""
    try:
        # Initialize Cloud SQL Python Connector
        connector = Connector()
        
        def getconn():
            conn = connector.connect(
                INSTANCE_CONNECTION_NAME,
                "pg8000",
                user=DB_USER,
                password=DB_PASS,
                db=DB_NAME
            )
            return conn
        
        # Create SQLAlchemy engine using the connection pool
        engine = create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        
        # Create a session factory
        Session = sessionmaker(bind=engine)
        
        logger.info("Database connection initialized successfully")
        return engine, Session
    except Exception as e:
        logger.error(f"Error initializing database connection: {e}")
        raise

def get_bank_id(session, bank_name):
    """Get bank ID from database, create if not exists."""
    try:
        # Try to find the bank in our mapping
        mapped_name = BANK_NAME_MAPPING.get(bank_name, bank_name)
        
        # Query the database for the bank
        result = session.execute(
            text("SELECT id FROM banks WHERE name = :name"),
            {"name": mapped_name}
        ).fetchone()
        
        if result:
            return result[0]
        else:
            # Create a new bank entry if it doesn't exist
            result = session.execute(
                text("INSERT INTO banks (name, created_at, updated_at) VALUES (:name, NOW(), NOW()) RETURNING id"),
                {"name": mapped_name}
            )
            session.commit()
            return result.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting bank ID for {bank_name}: {e}")
        session.rollback()
        raise

def get_tenor_id(session, tenor_months):
    """Get tenor ID from database, create if not exists."""
    try:
        # Query the database for the tenor
        result = session.execute(
            text("SELECT id FROM tenors WHERE months = :months"),
            {"months": tenor_months}
        ).fetchone()
        
        if result:
            return result[0]
        else:
            # Create a new tenor entry if it doesn't exist
            name = f"{tenor_months} months"
            if tenor_months == 0:
                name = "Floating"
            elif tenor_months == 12:
                name = "1 year"
            elif tenor_months == 24:
                name = "2 years"
            elif tenor_months == 36:
                name = "3 years"
            elif tenor_months == 48:
                name = "4 years"
            elif tenor_months == 60:
                name = "5 years"
            
            result = session.execute(
                text("INSERT INTO tenors (name, months, created_at, updated_at) VALUES (:name, :months, NOW(), NOW()) RETURNING id"),
                {"name": name, "months": tenor_months}
            )
            session.commit()
            return result.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting tenor ID for {tenor_months} months: {e}")
        session.rollback()
        raise

def update_bank_rate(session, bank_id, tenor_id, rate, rate_type="Standard"):
    """Update bank rate in database."""
    try:
        # Check if rate already exists
        result = session.execute(
            text("""
                SELECT id, rate FROM bank_rates 
                WHERE bank_id = :bank_id AND tenor_id = :tenor_id AND rate_type = :rate_type
            """),
            {"bank_id": bank_id, "tenor_id": tenor_id, "rate_type": rate_type}
        ).fetchone()
        
        if result:
            # Update existing rate if different
            if float(result[1]) != float(rate):
                session.execute(
                    text("""
                        UPDATE bank_rates 
                        SET rate = :rate, updated_at = NOW() 
                        WHERE id = :id
                    """),
                    {"id": result[0], "rate": rate}
                )
                session.commit()
                logger.info(f"Updated rate for bank_id={bank_id}, tenor_id={tenor_id}, rate_type={rate_type} to {rate}")
            else:
                logger.info(f"Rate unchanged for bank_id={bank_id}, tenor_id={tenor_id}, rate_type={rate_type}")
        else:
            # Insert new rate
            session.execute(
                text("""
                    INSERT INTO bank_rates (bank_id, tenor_id, rate, rate_type, created_at, updated_at) 
                    VALUES (:bank_id, :tenor_id, :rate, :rate_type, NOW(), NOW())
                """),
                {"bank_id": bank_id, "tenor_id": tenor_id, "rate": rate, "rate_type": rate_type}
            )
            session.commit()
            logger.info(f"Inserted new rate for bank_id={bank_id}, tenor_id={tenor_id}, rate_type={rate_type}: {rate}")
        
        return True
    except Exception as e:
        logger.error(f"Error updating bank rate: {e}")
        session.rollback()
        raise

def scrape_interest_co_nz():
    """Scrape mortgage rates from interest.co.nz/borrowing."""
    url = "https://www.interest.co.nz/borrowing"
    
    try:
        logger.info(f"Fetching data from {url}")
        response = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the mortgage rates table
        tables = soup.find_all('table', class_='table-bordered')
        
        rates_data = []
        current_bank = None
        current_product = None
        
        for table in tables:
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                
                # Skip header rows or rows with insufficient cells
                if len(cells) < 3:
                    continue
                
                # Check if this is a bank row
                institution_cell = cells[0].get_text(strip=True)
                if institution_cell and institution_cell != "Institution":
                    current_bank = institution_cell
                
                # Check if this is a product row
                product_cell = cells[1].get_text(strip=True)
                if product_cell and product_cell != "Product":
                    current_product = product_cell
                
                # Process rate cells
                for i, cell in enumerate(cells[2:], 2):
                    rate_text = cell.get_text(strip=True)
                    
                    # Skip empty cells or non-rate cells
                    if not rate_text or not re.match(r'^\d+\.\d+%$', rate_text):
                        continue
                    
                    # Get the tenor from the table header
                    header_row = table.find('tr')
                    if header_row:
                        headers = header_row.find_all('th')
                        if i < len(headers):
                            tenor = headers[i].get_text(strip=True)
                            
                            # Convert rate from string to float
                            rate_value = float(rate_text.replace('%', ''))
                            
                            # Add to our data collection
                            rates_data.append({
                                'bank': current_bank,
                                'product': current_product,
                                'tenor': tenor,
                                'rate': rate_value
                            })
        
        logger.info(f"Scraped {len(rates_data)} rates from interest.co.nz")
        return rates_data
    
    except Exception as e:
        logger.error(f"Error scraping interest.co.nz: {e}")
        raise

def process_rates_data(rates_data):
    """Process scraped rates data to find lowest rates per bank per tenor."""
    # Group rates by bank and tenor
    bank_tenor_rates = {}
    
    for rate_item in rates_data:
        bank = rate_item['bank']
        product = rate_item['product']
        tenor = rate_item['tenor']
        rate = rate_item['rate']
        
        # Skip non-standard and non-special products
        if not ('Standard' in product or 'Special' in product or 'Special LVR under 80%' in product):
            continue
        
        # Map tenor to months
        tenor_months = TENOR_MAPPING.get(tenor)
        if tenor_months is None:
            logger.warning(f"Unknown tenor: {tenor}")
            continue
        
        # Determine rate type
        rate_type = "Standard"
        if "Special" in product:
            rate_type = "Special"
        
        # Create key for bank and tenor
        key = (bank, tenor_months, rate_type)
        
        # Keep only the lowest rate for each bank, tenor, and rate type
        if key not in bank_tenor_rates or rate < bank_tenor_rates[key]:
            bank_tenor_rates[key] = rate
    
    # Convert to list of dictionaries
    processed_rates = []
    for (bank, tenor_months, rate_type), rate in bank_tenor_rates.items():
        processed_rates.append({
            'bank': bank,
            'tenor_months': tenor_months,
            'rate_type': rate_type,
            'rate': rate
        })
    
    logger.info(f"Processed {len(processed_rates)} unique bank/tenor/rate_type combinations")
    return processed_rates

def update_database(processed_rates):
    """Update database with processed rates."""
    engine, Session = initialize_db_connection()
    session = Session()
    
    try:
        update_count = 0
        for rate_item in processed_rates:
            bank = rate_item['bank']
            tenor_months = rate_item['tenor_months']
            rate_type = rate_item['rate_type']
            rate = rate_item['rate']
            
            # Get bank and tenor IDs
            bank_id = get_bank_id(session, bank)
            tenor_id = get_tenor_id(session, tenor_months)
            
            # Update rate in database
            if update_bank_rate(session, bank_id, tenor_id, rate, rate_type):
                update_count += 1
        
        logger.info(f"Updated {update_count} rates in database")
        return update_count
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        raise
    finally:
        session.close()

def main():
    """Main function to run the scraper."""
    try:
        logger.info("Starting mortgage rate scraper")
        
        # Scrape rates from interest.co.nz
        rates_data = scrape_interest_co_nz()
        
        # Process rates data
        processed_rates = process_rates_data(rates_data)
        
        # Update database
        update_count = update_database(processed_rates)
        
        logger.info("Mortgage rate scraper completed successfully")
        return {
            "status": "success",
            "scraped_count": len(rates_data),
            "processed_count": len(processed_rates),
            "updated_count": update_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
