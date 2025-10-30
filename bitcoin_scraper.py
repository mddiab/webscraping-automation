from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time
from datetime import datetime
from webdriver_manager.chrome import ChromeDriverManager

# keep fake_useragent usage but fail gracefully
try:
    from fake_useragent import UserAgent
except Exception:
    UserAgent = None

def build_user_agent():
    if UserAgent:
        try:
            ua = UserAgent()
            return ua.random
        except Exception:
            pass
    # fallback UA
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"

def create_driver(headless=True):
    options = Options()
    if headless:
        # use the legacy headless flag for maximum compatibility with sites
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={build_user_agent()}")
    # Helpful flags for CI / limited environments
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# Define the target website (CoinMarketCap Bitcoin page)
URL = "https://coinmarketcap.com/currencies/bitcoin/"

def safe_find_text(driver, by, locator):
    """Return text of first matching element or 'N/A' if not present."""
    try:
        el = driver.find_element(by, locator)
        return el.text.strip()
    except Exception:
        return "N/A"

def scrape_bitcoin_data(driver):
    """Scrape Bitcoin details from CoinMarketCap (keeps the same selectors and outputs)."""
    driver.get(URL)

    # keep the original full pause to preserve identical DOM timing/structure
    time.sleep(10)

    try:
        # Extract Bitcoin Price (same locator as original)
        price = safe_find_text(driver, By.XPATH, '//span[@data-test="text-cdp-price-display"]')

        # Extract Market Cap (same locator as original)
        market_cap = safe_find_text(driver, By.XPATH,
                                    "//dt[.//div[contains(text(),'Market cap')]]/following-sibling::dd//span")

        # Extract 24h Trading Volume (same locator as original)
        volume_24h = safe_find_text(driver, By.XPATH,
                                    "//dt[.//div[contains(text(),'Volume (24h')]]/following-sibling::dd//span")

        # Extract Circulating Supply (same locator as original)
        circulating_supply = safe_find_text(driver, By.XPATH,
                                           "//dt[.//div[contains(text(),'Circulating supply')]]/following-sibling::dd//span")

        # Extract 24h Price Change (same locator as original)
        price_change_24h = safe_find_text(driver, By.XPATH, "//p[contains(@class, 'change-text')]")

        # Extract Community Sentiment using the original find_elements calls (keeps original behavior)
        try:
            bullish_sentiment_elems = driver.find_elements(By.XPATH,
                "//span[contains(@class, 'sc-65e7f566-0 cOjBdO') and contains(@class, 'ratio')]")
            bearish_sentiment_elems = driver.find_elements(By.XPATH,
                "//span[contains(@class, 'sc-65e7f566-0 iKkbth') and contains(@class, 'ratio')]")

            bullish = bullish_sentiment_elems[0].text if bullish_sentiment_elems else "N/A"
            bearish = bearish_sentiment_elems[0].text if bearish_sentiment_elems else "N/A"
        except Exception:
            # fallback: try locating any percentage-like spans nearby the Community Sentiment heading
            bullish = "N/A"
            bearish = "N/A"
            try:
                elems = driver.find_elements(By.XPATH,
                    "//div[contains(., 'Community Sentiment')]//span[contains(., '%')]")
                if len(elems) >= 1:
                    bullish = elems[0].text.strip()
                if len(elems) >= 2:
                    bearish = elems[1].text.strip()
            except Exception:
                pass

        # Capture timestamp (same format)
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

    except Exception as e:
        # keep the same print behaviour for parity with your original script
        print("Error occurred:", e)
        return None

def save_to_csv(data):
    """Save scraped data to CSV (identical behavior to your original script)."""
    file_name = "bitcoin_hourly_data.csv"
    try:
        df = pd.read_csv(file_name)
    except FileNotFoundError:
        df = pd.DataFrame(columns=[
            "timestamp", "price", "market_cap", "volume_24h",
            "circulating_supply", "price_change_24h", "bullish_sentiment", "bearish_sentiment"
        ])

    new_row = pd.DataFrame([data])
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(file_name, index=False)

if __name__ == "__main__":
    driver = create_driver(headless=True)
    print("Scraping Bitcoin Data...")
    scraped_data = scrape_bitcoin_data(driver)

    if scraped_data:
        save_to_csv(scraped_data)
        print("Data saved to bitcoin_hourly_data.csv")
    else:
        print("Failed to scrape data.")

    driver.quit()
