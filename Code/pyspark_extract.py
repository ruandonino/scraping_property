from selenium import webdriver
from selenium.webdriver.common.by import By
#from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import re
import time
import math
from datetime import date

# Setup Chrome options
def get_chrome_options():
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    return options


# Extract data from a property card
def extract_property_data(card, reg_price, patterns):
    house = {}
    try:
        price = card.find_element(By.CSS_SELECTOR, '.Price-sc-12dh9kl-3.geYYII')
        condominio = card.find_element(By.CSS_SELECTOR, '.Expenses-sc-12dh9kl-1.iboaIF')
        address = card.find_element(By.CSS_SELECTOR, '.LocationAddress-sc-ge2uzh-0.iylBOA.postingAddress')
        city = card.find_element(By.CSS_SELECTOR, '.LocationLocation-sc-ge2uzh-2.fziprF')
        infos = card.find_element(By.CSS_SELECTOR, '.PostingMainFeaturesBlock-sc-1uhtbxc-0.cHDgeO')

        house['price'] = float(re.search(patterns['price'], price.text.strip()).group(1).replace('.', ''))
        cond_match = re.search(patterns['price'], condominio.text.strip())
        house['condominium'] = float(cond_match.group(1).replace('.', '')) if cond_match else None
        house['address'] = address.text.strip()
        house['neighborhood'] = city.text.strip()

        for key, pattern in patterns.items():
            match = re.search(pattern, infos.text.strip())
            house[key] = int(match.group(1)) if match else (1 if key == 'bathrooms' else None)

        return house
    except Exception as e:
        print(f"Could not find data for a property: {e}")
        return None

def saveDataframeToParquet(df_data, output_path):
  df_data.write.mode("overwrite").parquet(output_path)
  print(f"Data saved to {output_path}")
# Main scraping function
def scrape_properties():
    today = date.today()
    website = 'imovelweb'
    output_path = f"gs://python_files_property/outputs_extracted_data/{website}/{today}/{website}-{today}"
    options = get_chrome_options()
    driver = webdriver.Chrome(options=options)

    url = 'https://www.imovelweb.com.br/imoveis-aluguel-itajuba-mg.html'
    patterns = {
        'price': r'R\$\s*(\d+\.*\d+)',
        'area': r'(\d+)\s*m²',
        'bedrooms': r'(\d+)\s*quartos?',
        'bathrooms': r'(\d+)\s*(?:banheiros?|ban.)',
        'garage': r'(\d+)\s*vagas?'
    }

    # Load the web page
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Aceito']"))).click()

    # Get total number of pages
    quant_pag_text = driver.find_element(By.CSS_SELECTOR, '.Title-sc-1oqs0ed-0.kNcbvY').text
    total_pages = math.ceil(float(re.search(r'(\d+)', quant_pag_text).group(1)) / 20)
    list_of_houses = []
    print(total_pages)

    for page_num in range(total_pages):
        if page_num > 0:
            page_url = f'https://www.imovelweb.com.br/imoveis-aluguel-itajuba-mg-pagina-{page_num + 1}.html'
            driver = webdriver.Chrome(options=options)
            driver.get(page_url)
            time.sleep(1)
        print(page_num)

        # Find all the cards on the page
        cards = driver.find_elements(By.CSS_SELECTOR, '.CardContainer-sc-1tt2vbg-5.fvuHxG')

        # Extract data from each card
        for card in cards:
            house_data = extract_property_data(card, patterns)
            if house_data:
                list_of_houses.append(house_data)
        driver.quit()

    # Save results to CSV and Parquet
    df_houses = pd.DataFrame(list_of_houses)
    saveDataframeToParquet(df_houses, output_path)


if __name__ == "__main__":
    scrape_properties()
