import json
from os import path
import pathlib as pl
import time

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service as ChromiumService
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from vdl_tools.scrape_enrich.scraper.chrome_utils import get_chromedriver_version
from vdl_tools.shared_tools.tools.logger import logger as log

import logging
logging.getLogger('selenium').setLevel(logging.INFO)
logging.getLogger('webdriver_manager').setLevel(logging.INFO)


def page_scraper():
    '''
    sets up the scraper chromium
    
    returns : Chrome scraper session object
    '''
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Enables headless mode
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model, OPTIONAL
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems, OPTIONAL

    capabilities = DesiredCapabilities.CHROME
    capabilities['goog:loggingPrefs'] = {'performance': 'ALL'}
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    driver = webdriver.Chrome(
        service=ChromiumService(
            desired_capabilities=capabilities,
        ),
        options=chrome_options,
    )
    return driver


def check_400s(performance_logs, target_url):
    for entry in performance_logs:
        logs = json.loads(entry["message"])["message"]
        if logs["method"] == "Network.responseReceived" and "response" in logs["params"]:
            status_code = logs["params"]["response"]["status"]
            if status_code >= 400:
                url = logs["params"]["response"]["url"]
                if target_url in url:
                    return True
    return False


def scrape_website(url: str, scraper):
    if not url.startswith('http'):
        url = f'https://{url}'
    try:
        scraper.get(url)
        scraper.implicitly_wait(10)
        time.sleep(5)
        logs = scraper.get_log("performance")
        if check_400s(logs, target_url=url):
            log.warn(f"404 Error Detected for {url}")
            return None

    except TimeoutException as tex:
        log.warn(f"{url}: Timed out, attempting to scrape what has been loaded at the moment")
    except Exception as e:
        log.warn(f"URL {url} is not valid")
        return None

    try:
        body = scraper.find_element(By.TAG_NAME, 'body')
    except NoSuchElementException:
        log.warn(f'No body found for {url}')
        return None

    if not body.text.strip():
        log.warn(f'Received empty page content for {url}')
        return None

    if 'https://challenges.cloudflare.com' in body.text:
        log.warn(f'Looks like Cloudflare protection is enabled for {url}')

    return scraper.page_source
