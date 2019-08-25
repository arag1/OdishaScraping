The odischascraping project is a thread based selenium web scraper for RCCMS data
The program has the following flags

--noThreads - the lower level scraping can be run in single or multi-threaded mode. We use 16 threads and the code will run 16 times faster.
--debug - displays the chrome browser. Run production with with headless mode and this flag can be skipped for production.
--scrapeTopLevel - scrapes the top level web sites for each case type, court type, year and month
--scrapeLowerLevel - this clicks on the URL link for each district and then scrapes the underlying Tashil data

command line

python odishascraping.py <option flags>

To install the code and run it in your python
1) first create a virtual environment
2) then pip install the requirements file
pip install -r requirements.txt
You are then set to run the code.

Happy Scraping!
Anurag Aiyer
