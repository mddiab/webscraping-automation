import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# try to keep fake_useragent but handle failures gracefully
try:
    from fake_useragent import UserAgent
except Exception:
    UserAgent = None

# -----------------------
# Config
# -----------------------
URL = "https://coinmarketcap.com/currencies/bitcoin/"
CSV_FILE = "bitcoin_hourly_data.csv"
DEFAULT_TIMEOUT = 15
RETRY_ATTEMPTS = 2
HEADLESS = True

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("btc.scraper")


def build_user_agent() -> str:
    """Return a random UA if possible, otherwise a sensible fallback."""
    if UserAgent:
        try:
            ua = UserAgent()
            return ua.random
        except Exception:
            log.debug("fake_useragent failed, falling back to static UA")
    # fallback UA (reasonable and stable)
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"


def create_driver(headless: bool = HEADLESS, user_agent: Optional[str] = None) -> webdriver.Chrome:
    """Create and return a configured Chrome webdriver instance."""
    opts = Options()
    if headless:
        # use new headless mode when available and add CI flags
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    ua = user_agent or build_user_agent()
    opts.add_argument(f"user-agent={ua}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver


def safe_get_text(driver: webdriver.Chrome, wait: WebDriverWait, by: By, locator: str) -> Optional[str]:
    """Return the text content of an element if present, else None."""
    try:
        el = wait.until(EC.presence_of_element_located((by, locator)))
        return el.text.strip()
    except Exception as e:
        log.debug("safe_get_text failed for %s: %s", locator, e)
        return None


def try_multiple(driver: webdriver.Chrome, wait: WebDriverWait, locators: List[tuple]) -> Optional[str]:
    """
    Try multiple (By, locator) pairs in order until one returns text.
    Useful because site structure or class names change.
    """
    for by, loc in locators:
        text = safe_get_text(driver, wait, by, loc)
        if text:
            return text
    return None


def scrape_bitcoin_data(driver: webdriver.Chrome, timeout: int = DEFAULT_TIMEOUT) -> Optional[Dict[str, Any]]:
    """
    Scrape Bitcoin data from CoinMarketCap using the provided driver.
    Returns a dict with the same keys as your original script, or None on failure.
    This function is resilient: missing one field won't abort collection of others.
    """
    wait = WebDriverWait(driver, timeout)
    driver.get(URL)

    # It's possible the site delays JS rendering; we rely on WebDriverWait for each element.
    # Price: try a couple of possible selectors
    price = try_multiple(driver, wait, [
        (By.XPATH, '//span[@data-test="text-cdp-price-display"]'),
        (By.CSS_SELECTOR, "div.priceValue > span"),
        (By.XPATH, "//div[contains(@class,'priceValue')]/span")
    ]) or "N/A"

    # Market cap: multiple possible locators (page markup may vary)
    market_cap = try_multiple(driver, wait, [
        (By.XPATH, "//dt[.//div[contains(text(),'Market cap')]]/following-sibling::dd//span"),
        (By.XPATH, "//div[contains(text(),'Market Cap')]/following-sibling::div//span"),
        (By.XPATH, "//div[.//span[text()[contains(., 'Market Cap')]]]/div//span")
    ]) or "N/A"

    # 24h Volume
    volume_24h = try_multiple(driver, wait, [
        (By.XPATH, "//dt[.//div[contains(text(),'Volume (24h')]]/following-sibling::dd//span"),
        (By.XPATH, "//div[contains(text(),'Volume') and contains(text(),'24h')]/following-sibling::div//span"),
        (By.XPATH, "//div[contains(.,'Volume (24h)')]/div//span")
    ]) or "N/A"

    # Circulating supply
    circulating_supply = try_multiple(driver, wait, [
        (By.XPATH, "//dt[.//div[contains(text(),'Circulating supply')]]/following-sibling::dd//span"),
        (By.XPATH, "//div[contains(text(),'Circulating Supply')]/following-sibling::div//span"),
    ]) or "N/A"

    # 24h price change (change-text class or similar)
    price_change_24h = try_multiple(driver, wait, [
        (By.XPATH, "//p[contains(@class, 'change-text')]"),
        (By.CSS_SELECTOR, "span.sc-15yy2pl-0.kAXKAX")  # fallback example
    ]) or "N/A"

    # Community sentiment: try to locate the "Community Sentiment" section then grab ratios
    bullish = bearish = "N/A"
    try:
        # try a few patterns; this is defensive because classes change often
        sentiment_parent = try_multiple(driver, wait, [
            (By.XPATH, "//div[contains(., 'Community Sentiment') and .//span]"),
            (By.XPATH, "//h3[contains(., 'Community Sentiment')]/following-sibling::div"),
            (By.XPATH, "//div[contains(@class,'community-sentiment')]")
        ])

        # If we found a parent block, try to grab ratio spans under it
        if sentiment_parent:
            # locate the two ratio spans under the section using a relative query
            elems = driver.find_elements(By.XPATH,
                                        "//div[contains(., 'Community Sentiment')]//span[contains(@class,'ratio') or contains(@class,'sc-')]/span | //div[contains(., 'Community Sentiment')]//span[contains(@class,'ratio')]")
            # fallback: look for percentage-looking spans near the heading
            if not elems:
                elems = driver.find_elements(By.XPATH, "//div[contains(., 'Community Sentiment')]//span")
            if elems:
                # pick first two if available
                if len(elems) >= 1:
                    bullish = elems[0].text.strip() or "N/A"
                if len(elems) >= 2:
                    bearish = elems[1].text.strip() or "N/A"
    except Exception:
        log.debug("Community sentiment parsing failed", exc_info=True)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bitcoin_data = {
        "timestamp": timestamp,
        "price": price,
        "market_cap": market_cap,
        "volume_24h": volume_24h,
        "circulating_supply": circulating_supply,
        "price_change_24h": price_change_24h,
        "bullish_sentiment": bullish,
        "bearish_sentiment": bearish
    }

    return bitcoin_data


def save_to_csv(data: Dict[str, Any], file_name: str = CSV_FILE) -> None:
    """Append scraped row to CSV, creating the file with headers if necessary."""
    cols = [
        "timestamp", "price", "market_cap", "volume_24h",
        "circulating_supply", "price_change_24h", "bullish_sentiment", "bearish_sentiment"
    ]
    try:
        df_existing = pd.read_csv(file_name)
    except FileNotFoundError:
        df_existing = pd.DataFrame(columns=cols)

    new_row = pd.DataFrame([data], columns=cols)
    df = pd.concat([df_existing, new_row], ignore_index=True)
    df.to_csv(file_name, index=False)
    log.info("Saved row to %s", file_name)


def run_once(url: str = URL, attempts: int = RETRY_ATTEMPTS) -> bool:
    """Run the scraping process once, with a small retry loop for transient failures."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        log.info("Scrape attempt %d/%d", attempt, attempts)
        driver = None
        try:
            driver = create_driver()
            data = scrape_bitcoin_data(driver)
            if data:
                save_to_csv(data)
                log.info("Scrape successful: %s", data["timestamp"])
                return True
            else:
                log.warning("Scrape returned no data on attempt %d", attempt)
        except Exception as e:
            last_exc = e
            log.exception("Error during scraping attempt %d", attempt)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    log.debug("driver.quit() failed", exc_info=True)
    # all attempts exhausted
    log.error("All scrape attempts failed. Last exception: %s", last_exc)
    return False


if __name__ == "__main__":
    log.info("Starting Bitcoin scraper")
    success = run_once()
    if success:
        log.info("Finished successfully")
    else:
        log.error("Finished with errors")
