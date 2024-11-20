'''
# import module
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import re
import time
import math

options = Options()
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('headless')
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')
user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
options.add_argument(f'user-agent={user_agent}')
driver = webdriver.Chrome(options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
url = 'https://www.imovelweb.com.br/imoveis-aluguel-itajuba-mg.html'
patterns = {
    'area': r'(\d+)\s*m²',
    'bedrooms': r'(\d+)\s*quartos?',
    'bathrooms': r'(\d+)\s*(?:banheiros?|ban.)',
    'garage': r'(\d+)\s*vagas?'
}
reg_price = r'R\$\s*(\d+\.*\d+)'

#load the web page
driver.get(url)
accept_button = driver.find_element(By.XPATH, "//button[text()='Aceito']")
accept_button.click()
time.sleep(2)
list_of_houses = []

quant_pag_text = driver.find_element(By.CSS_SELECTOR, '.Title-sc-1oqs0ed-0.kNcbvY').text
quant_pag = math.ceil(float(re.search(r'(\d+)', quant_pag_text).group(1))/20)
for index_page in range(quant_pag):
    if index_page > 0:
        url = f'https://www.imovelweb.com.br/imoveis-aluguel-itajuba-mg-pagina-{index_page+1}.html'
        driver = webdriver.Chrome(options=options)
        driver.get(url)
    time.sleep(2)
    # Find all the cards on the page
    cards = driver.find_elements(By.CSS_SELECTOR,'.CardContainer-sc-1tt2vbg-5.fvuHxG')
    print(cards[0].text)
    # Loop through each card and extract the price (or other info)
    for index, card in enumerate(cards):
        try:
            # Find the price element within the card
            house = {}
            price = card.find_element(By.CSS_SELECTOR, '.Price-sc-12dh9kl-3.geYYII')
            condominio = card.find_element(By.CSS_SELECTOR, '.Expenses-sc-12dh9kl-1.iboaIF')
            address = card.find_element(By.CSS_SELECTOR, '.LocationAddress-sc-ge2uzh-0.iylBOA.postingAddress')
            city = card.find_element(By.CSS_SELECTOR, '.LocationLocation-sc-ge2uzh-2.fziprF')
            infos = card.find_element(By.CSS_SELECTOR, '.PostingMainFeaturesBlock-sc-1uhtbxc-0.cHDgeO')
            house['price'] = float(re.search(reg_price,price.text.strip()).group(1).strip().replace('.', ''))
            match_cond = re.search(reg_price,condominio.text.strip())
            if match_cond:
                house['condominium'] = float(match_cond.group(1).strip().replace('.', ''))
            else:
                house['condominium'] = None
            house['address'] = address.text.strip()
            house['neighborhood'] = city.text.strip()
            for key, pattern in patterns.items():
                match = re.search(pattern, infos.text.strip())
                if match:
                    house[key] = int(match.group(1))
                elif key == 'bathrooms':
                    house[key] = 1
                else:
                    house[key] = None

            print(f"Property {index+1} house: {house['price']}")
            print(f"Property {index + 1} garage: {house['garage']}")
            print(f"Property {index + 1} area: {house['area']}")
            print(f"Property {index + 1} bedrooms: {house['bedrooms']}")
            print(f"Property {index + 1} bathrooms: {house['bathrooms']}")
            print(f"Property {index + 1} condominium: {house['condominium']}")

            list_of_houses.append(house)
        except Exception as e:
            print(f"Could not find data for property {index+1}: {e}")
    driver.close()
df_houses = pd.DataFrame(list_of_houses)
df_houses.to_csv('../Files/houses.csv', index=False)
df_houses.to_parquet('../Files/houses.parquet')


# close the driver
driver.quit()
'''
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import re
import time
import math


# Setup Chrome options
def get_chrome_options():
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('headless')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    return options


# Extract data from a property card
def extract_property_data(card, patterns):
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


# Main scraping function
def scrape_properties():
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
    df_houses.to_csv('../Files/houses.csv', index=False)
    df_houses.to_parquet('../Files/houses.parquet')


if __name__ == "__main__":
    scrape_properties()
