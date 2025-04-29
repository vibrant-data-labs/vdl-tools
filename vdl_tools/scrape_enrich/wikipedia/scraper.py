### imports
import time
from os import path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

### helper functions
def element_exists_by_css(selector, scraper):
    if scraper is None:
        print(f"FatalScrapeError --> No scraper object found")
    else:
        try:
            scraper.find_element_by_css_selector(selector)
        except NoSuchElementException:
            return False
        return True


### scraper init
def init_scraper(unattended=True, chrome_driver_path=None):
    """
    ---------------------
    ------- description : sets up the scraper instance
    ---------------------
    -------- parameters :
             unattended : unattended mode (true or false)
     chrome_driver_path : path to chromedriver executable allowed for override
    ---------------------
    ----------- returns : logged in scraper session object
    ---------------------
    """
    # init chromedriver
    chrome_driver_binary = None
    options = webdriver.ChromeOptions() or Options()
    # Run headless if unattended == true
    if unattended:
        options.add_argument("headless")
    # determine correct chrome driver path
    if chrome_driver_path:
        chrome_driver_binary = chrome_driver_path
    elif path.exists("./chromedriver"):
        chrome_driver_binary = "./chromedriver"
    elif path.exists("/usr/local/bin/chromedriver"):
        chrome_driver_binary = "/usr/local/bin/chromedriver"
    else:
        print("ChromedriverPathError --> Unable to find chromedriver.")
    # init driver
    if chrome_driver_binary is not None:
        driver = webdriver.Chrome(chrome_driver_binary, options=options)
    else:
        print("SeleniumDriverInitError --> Unable to initialize driver object.")
        driver = None
    # return the driver
    return driver


### scrape keyword
def scrape_keyword(keyword=None, scraper=None):
    if scraper is None:
        print(f"FatalScrapeError --> No scraper object found")
    else:
        if keyword is None:
            print(f"FatalScrapeError --> Keyword not passed as argument")
        else:
            ## search for keyword
            scraper.get("https://www.wikipedia.org/")
            search_text = scraper.find_element_by_id("searchInput")
            search_text.send_keys(keyword)
            search_btn = scraper.find_element_by_class_name("svg-search-icon")
            search_btn.click()
            if element_exists_by_css("#disambigbox", scraper):
                print(f"PageType --> This is a disambiguation page")
            else:
                print(f"PageType --> This is an actual page")
                body = scraper.find_element_by_id("bodyContent")
                body_text = body.text
                print(f"corpus text --> ")
                print(body_text)
            time.sleep(7)


### main
driver = init_scraper(unattended=False)

for keywd in ["Climate change", "Climate change (disambiguation)"]:
    scrape_keyword(keywd, driver)
