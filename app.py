from flask import Flask
from database import download_redbook_codes
from gumtree_scraper import GumtreeScraper
import threading

app = Flask(__name__)


@app.route("/get_gumtree_au_listings", methods=['GET'])
def get_gumtree_au_listings():
    download_redbook_codes()
    scraper = GumtreeScraper()
    scraper_thread = threading.Thread(target=scraper.get_gumtree_listings)
    scraper_thread.start()
    return "Gumtree Scraping Started"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6123)
