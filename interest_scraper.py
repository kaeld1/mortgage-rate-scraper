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
    "Bank of Baroda": "Bank of Baroda",
    "Bank of China": "Bank of China",
    "China Construction Bank": "China Construction Bank",
    "Co-operative Bank": "Co-operative Bank",
    "Heartland Bank": "Heartland Bank",
    "ICBC": "ICBC",
    "Kookmin": "Kookmin",
    "SBS Bank": "SBS Bank",
    "TSB Bank": "TSB Bank",
    "Westpac": "Westpac",
    "Kiwibank": "Kiwibank",
    "ANZ": "ANZ",
    # Add any other banks that might appear
}

# Tenor mapping to standardize tenor names and convert to months
TENOR_MAPPING = {
    "floating": {"name": "Floating", "months": 1},
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
        
        # Find the mortgage rate tables
        rates = []
        
        # The main table contains all the mortgage rates
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            
            current_bank = None
            
            for row in rows:
                # Check if this row contains a bank name
                bank_cell = row.find('td', class_='views-field-field-provider-name')
                if bank_cell:
                    bank_img = bank_cell.find('img')
                    if bank_img and bank_img.get('alt'):
                        current_bank = bank_img.get('alt')
                    else:
                        # Try to get text if no image
                        bank_text = bank_cell.get_text(strip=True)
                        if bank_text:
                            current_bank = bank_text
                
                # Skip if we haven't found a bank yet
                if not current_bank:
                    continue
                
                # Standardize bank name
                standardized_bank = BANK_NAME_MAPPING.get(current_bank, current_bank)
                
                # Get the rate type (Standard, Special, etc.)
                rate_type_cell = row.find('td', class_='views-field-field-mortgage-type')
                rate_type = "Standard"  # Default
                if rate_type_cell:
                    rate_type_text = rate_type_cell.get_text(strip=True)
                    if rate_type_text:
                        rate_type = rate_type_text
                
                # Extract rates for different tenors
                # The columns are typically: floating, 6m, 1y, 18m, 2y, 3y, 4y, 5y
                cells = row.find_all('td')
                
                # Skip rows with too few cells
                if len(cells) < 8:
                    continue
                
                # Check for 18 months special case (often in a separate row)
                eighteen_month_cell = row.find('td', string=lambda s: s and '18 months' in s)
                if eighteen_month_cell:
                    rate_text = eighteen_month_cell.get_text(strip=True)
                    rate_match = re.search(r'18 months = (\d+\.\d+)', rate_text)
                    if rate_match:
                        rate_value = float(rate_match.group(1))
                        rates.append({
                            'bank': standardized_bank,
                            'tenor': '18 months',
                            'rate': rate_value,
                            'rate_type': rate_type
                        })
                
                # Process regular tenor columns
                tenor_indices = {
                    1: 'floating',
                    2: '6 months',
                    3: '1 year',
                    4: '2 years',
                    5: '3 years',
                    6: '4 years',
                    7: '5 years'
                }
                
                for idx, tenor_name in tenor_indices.items():
                    if idx < len(cells):
                        rate_cell = cells[idx]
                        rate_text = rate_cell.get_text(strip=True)
                        
                        # Extract numeric rate value
                        if rate_text and re.match(r'^\d+\.\d+', rate_text):
                            try:
                                rate_value = float(re.match(r'^\d+\.\d+', rate_text).group())
                                rates.append({
                                    'bank': standardized_bank,
                                    'tenor': tenor_name,
                                    'rate': rate_value,
                                    'rate_type': rate_type
                                })
                            except (ValueError, AttributeError):
                                # Skip if we can't parse the rate
                                pass
        
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
                
                # Get or create tenor
                tenor_id = tenors.get(tenor_name, {}).get('id')
                if not tenor_id:
                    result = conn.execute(
                        text("INSERT INTO tenors (name, months) VALUES (:name, :months) RETURNING id"),
                        {"name": tenor_name, "months": tenor_months}
                    )
                    tenor_id = result.fetchone()[0]
                    tenors[tenor_name] = {'id': tenor_id, 'months': tenor_months}
                
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
        
        # Process rates to find the lowest for each bank/tenor/rate_type
        processed_rates = process_rates(rates)
        
        # Initialize database connection
        engine = initialize_db_connection()
        
        # Update database
        update_database(engine, processed_rates)
        
        logger.info("Mortgage rate scraper completed successfully")
        return "Mortgage rates scraped successfully"
    
    except Exception as e:
        logger.error(f"Error in mortgage rate scraper: {e}")
        return f"Error: {str(e)}"

if __name__ == "__main__":
    main()
