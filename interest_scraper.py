"""
Interest.co.nz Mortgage Rate Scraper

This script scrapes mortgage rates from interest.co.nz and updates a database with the latest rates.
It is designed to be run as a scheduled task to keep the database up to date.
"""

import requests
from bs4 import BeautifulSoup
import logging
import re
import os
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime
from google.cloud.sql.connector import Connector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection settings
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "mortgage_data")
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME", "")

# Bank name mapping to standardize bank names
BANK_NAME_MAPPING = {
    "ASB": "ASB",
    "BNZ": "BNZ",
    "ANZ": "ANZ",
    "Westpac": "Westpac",
    "Kiwibank": "Kiwibank",
    "TSB Bank": "TSB Bank",
    "SBS Bank": "SBS Bank",
    "Co-operative Bank": "Co-operative Bank",
    "Heartland Bank": "Heartland Bank",
    "Bank of China": "Bank of China",
    "Bank of Baroda": "Bank of Baroda",
    "China Construction Bank": "China Construction Bank",
    "ICBC": "ICBC",
    "Kookmin": "Kookmin",
    "Heretaunga Building Society": "Heretaunga Building Society",
    # Add any other banks that might appear
}

# Tenor mapping to standardize tenor names and convert to months
TENOR_MAPPING = {
    "Variable floating": {"name": "Floating", "months": 1},
    "6 months": {"name": "6 months", "months": 6},
    "1 year": {"name": "1 year", "months": 12},
    "18 months": {"name": "18 months", "months": 18},
    "2 years": {"name": "2 years", "months": 24},
    "3 years": {"name": "3 years", "months": 36},
    "4 years": {"name": "4 years", "months": 48},
    "5 years": {"name": "5 years", "months": 60},
    # Add any other tenors that might appear
}

def initialize_db_connection():
    """Initialize a connection to the Cloud SQL database."""
    try:
        # Initialize Connector
        connector = Connector()
        
        # Function to create the SQLAlchemy engine
        def getconn():
            conn = connector.connect(
                INSTANCE_CONNECTION_NAME,
                "pg8000",
                user=DB_USER,
                password=DB_PASS,
                db=DB_NAME
            )
            return conn
        
        # Create SQLAlchemy engine
        engine = create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        
        logger.info("Database connection initialized successfully")
        return engine
    except Exception as e:
        logger.error(f"Error initializing database connection: {e}")
        raise

def scrape_interest_co_nz():
    """Scrape mortgage rates from interest.co.nz."""
    logger.info("Starting mortgage rate scraper")
    
    url = "https://www.interest.co.nz/borrowing"
    logger.info(f"Fetching data from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the mortgage rate table by ID
        rates_table = soup.find('table', id='interest_financial_datatable')
        
        if not rates_table:
            logger.error("Could not find mortgage rates table on the page")
            return []
        
        # Extract rates
        rates = []
        current_bank = None
        
        # Get all rows in the table body
        rows = rates_table.find('tbody').find_all('tr')
        
        for row in rows:
            cells = row.find_all('td')
            
            # Skip rows with insufficient cells
            if not cells:
                continue
            
            # Check if this row contains a bank name (first cell with an image)
            bank_cell = cells[0]
            bank_img = bank_cell.find('img')
            
            if bank_img and bank_img.get('title'):
                current_bank = bank_img.get('title')
                logger.info(f"Found bank: {current_bank}")
            
            # Skip if we haven't found a bank yet
            if not current_bank:
                continue
            
            # Standardize bank name
            standardized_bank = BANK_NAME_MAPPING.get(current_bank, current_bank)
            
            # Check if this is an 18-month special row
            if len(cells) >= 3 and "18 months" in cells[2].get_text():
                rate_text = cells[2].get_text().strip()
                rate_match = re.search(r'18 months = (\d+\.\d+)', rate_text)
                
                if rate_match:
                    rate_value = float(rate_match.group(1))
                    
                    # Get rate type from the second cell
                    rate_type = "Standard"
                    if len(cells) >= 2:
                        rate_type_text = cells[1].get_text().strip()
                        if rate_type_text:
                            rate_type = rate_type_text
                    
                    rates.append({
                        'bank': standardized_bank,
                        'tenor': '18 months',
                        'rate': rate_value,
                        'rate_type': rate_type
                    })
                    logger.info(f"Extracted 18-month rate: {standardized_bank}, {rate_type}, {rate_value}")
                
                # Skip further processing for this row
                continue
            
            # Get the rate type from the second cell
            rate_type = "Standard"
            if len(cells) >= 2:
                rate_type_text = cells[1].get_text().strip()
                if rate_type_text:
                    rate_type = rate_type_text
            
            # Process regular tenor columns (cells 2-8)
            tenor_indices = {
                2: 'Variable floating',
                3: '6 months',
                4: '1 year',
                5: '2 years',
                6: '3 years',
                7: '4 years',
                8: '5 years'
            }
            
            for idx, tenor_name in tenor_indices.items():
                if idx < len(cells):
                    rate_text = cells[idx].get_text().strip()
                    
                    # Extract numeric rate value
                    rate_match = re.search(r'(\d+\.\d+)', rate_text)
                    if rate_match:
                        try:
                            rate_value = float(rate_match.group(1))
                            rates.append({
                                'bank': standardized_bank,
                                'tenor': tenor_name,
                                'rate': rate_value,
                                'rate_type': rate_type
                            })
                            logger.info(f"Extracted rate: {standardized_bank}, {tenor_name}, {rate_type}, {rate_value}")
                        except (ValueError, AttributeError) as e:
                            logger.warning(f"Could not parse rate value: {rate_text}, Error: {e}")
        
        logger.info(f"Scraped {len(rates)} rates from interest.co.nz")
        return rates
    
    except Exception as e:
        logger.error(f"Error scraping interest.co.nz: {e}")
        return []

def process_rates(rates):
    """Process the scraped rates to find the lowest rate for each bank/tenor/rate_type combination."""
    processed_rates = {}
    
    for rate in rates:
        bank = rate['bank']
        tenor = rate['tenor']
        rate_type = rate['rate_type']
        rate_value = rate['rate']
        
        # Create a unique key for each bank/tenor/rate_type combination
        key = f"{bank}|{tenor}|{rate_type}"
        
        # If we haven't seen this combination before, or if this rate is lower than what we've seen
        if key not in processed_rates or rate_value < processed_rates[key]['rate']:
            processed_rates[key] = {
                'bank': bank,
                'tenor': tenor,
                'rate_type': rate_type,
                'rate': rate_value
            }
    
    logger.info(f"Processed {len(processed_rates)} unique bank/tenor/rate_type combinations")
    return list(processed_rates.values())

def update_database(engine, processed_rates):
    """Update the database with the processed rates."""
    try:
        # Connect to the database
        with engine.connect() as conn:
            # Get existing banks and tenors
            banks = {}
            tenors = {}
            
            # Get banks
            result = conn.execute(text("SELECT id, name FROM banks"))
            for row in result:
                banks[row[1]] = row[0]
            
            # Get tenors
            result = conn.execute(text("SELECT id, name, months FROM tenors"))
            for row in result:
                tenors[row[1]] = {'id': row[0], 'months': row[2]}
            
            # Update rates
            updated_count = 0
            
            for rate_data in processed_rates:
                bank_name = rate_data['bank']
                tenor_name = TENOR_MAPPING.get(rate_data['tenor'], {}).get('name', rate_data['tenor'])
                tenor_months = TENOR_MAPPING.get(rate_data['tenor'], {}).get('months', 0)
                rate_value = rate_data['rate']
                rate_type = rate_data['rate_type']
                
                # Skip if we don't have a valid tenor mapping
                if tenor_months == 0:
                    logger.warning(f"No tenor mapping for {rate_data['tenor']}, skipping")
                    continue
                
                # Get or create bank
                bank_id = banks.get(bank_name)
                if not bank_id:
                    result = conn.execute(
                        text("INSERT INTO banks (name) VALUES (:name) RETURNING id"),
                        {"name": bank_name}
                    )
                    bank_id = result.fetchone()[0]
                    banks[bank_name] = bank_id
                    logger.info(f"Created new bank: {bank_name}, id: {bank_id}")
                
                # Get or create tenor
                tenor_id = tenors.get(tenor_name, {}).get('id')
                if not tenor_id:
                    result = conn.execute(
                        text("INSERT INTO tenors (name, months) VALUES (:name, :months) RETURNING id"),
                        {"name": tenor_name, "months": tenor_months}
                    )
                    tenor_id = result.fetchone()[0]
                    tenors[tenor_name] = {'id': tenor_id, 'months': tenor_months}
                    logger.info(f"Created new tenor: {tenor_name}, months: {tenor_months}, id: {tenor_id}")
                
                # Update or insert rate
                result = conn.execute(
                    text("""
                        INSERT INTO bank_rates (bank_id, tenor_id, rate, rate_type, updated_at)
                        VALUES (:bank_id, :tenor_id, :rate, :rate_type, NOW())
                        ON CONFLICT (bank_id, tenor_id)
                        DO UPDATE SET rate = :rate, rate_type = :rate_type, updated_at = NOW()
                        RETURNING id
                    """),
                    {
                        "bank_id": bank_id,
                        "tenor_id": tenor_id,
                        "rate": rate_value,
                        "rate_type": rate_type
                    }
                )
                
                updated_count += 1
                logger.info(f"Updated rate: {bank_name}, {tenor_name}, {rate_type}, {rate_value}")
            
            # Commit the transaction
            conn.commit()
            
            logger.info(f"Updated {updated_count} rates in database")
            return updated_count
    
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        return 0

def main():
    """Main function to run the scraper."""
    try:
        # Scrape rates from interest.co.nz
        rates = scrape_interest_co_nz()
        
        if not rates:
            logger.warning("No rates were scraped, check the scraping logic")
            return "No rates were scraped, check the scraping logic"
        
        # Process rates to find the lowest for each bank/tenor/rate_type
        processed_rates = process_rates(rates)
        
        if not processed_rates:
            logger.warning("No processed rates available")
            return "No processed rates available"
        
        # Initialize database connection
        engine = initialize_db_connection()
        
        # Update database
        updated_count = update_database(engine, processed_rates)
        
        if updated_count > 0:
            logger.info("Mortgage rate scraper completed successfully")
            return "Mortgage rates scraped successfully"
        else:
            logger.warning("No rates were updated in the database")
            return "No rates were updated in the database"
    
    except Exception as e:
        logger.error(f"Error in mortgage rate scraper: {e}")
        return f"Error: {str(e)}"

if __name__ == "__main__":
    main()
