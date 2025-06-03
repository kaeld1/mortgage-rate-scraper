print("Starting cloud_run_server.py")

import os
from flask import Flask, request

try:
    from interest_scraper import main as scraper_main
    print("Successfully imported main from interest_scraper")
except Exception as e:
    print(f"Error importing from interest_scraper: {str(e)}")
    # Fallback to a simple function that just logs
    def scraper_main():
        print("Using fallback scraper_main function")
        return {"status": "error", "message": "Failed to import real scraper"}

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return "Mortgage Rate Scraper Service", 200

@app.route('/run-scraper', methods=['POST'])
def run_scraper():
    try:
        # Run the scraper and capture the return value
        result = scraper_main()
        
        # Log and check the result
        print(f"Scraper result: {result}")
        
        if result.get('status') == 'success':
            return f"Mortgage rates scraped successfully. {result.get('rates_updated', 0)} rates updated.", 200
        else:
            error_msg = result.get('message', 'Unknown error')
            print(f"Scraper reported error: {error_msg}")
            return f"Error: {error_msg}", 500
            
    except Exception as e:
        print(f"Exception in run_scraper: {str(e)}")
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
