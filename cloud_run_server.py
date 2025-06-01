import os
from flask import Flask, request
from interest_scraper import main as scraper_main

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return "Mortgage Rate Scraper Service", 200

@app.route('/run-scraper', methods=['POST'])
def run_scraper():
    try:
        # Run the scraper
        scraper_main()
        return "Mortgage rates scraped successfully", 200
    except Exception as e:
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
