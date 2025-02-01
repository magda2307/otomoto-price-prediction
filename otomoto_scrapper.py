from functools import wraps
import time
import requests
from bs4 import BeautifulSoup
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from retry import retry 

"""
OTOMOTO SCRAPER

This script scrapes OTOMOTO.pl for *used* cars that have not had an accident,
across multiple brand-filtered URLs. Due to OTOMOTO’s ~500-page limit, each
URL includes a subset of brands to ensure we stay within that limit. If you
need to adjust or add/remove brand groups, update the URL list. Remove those
that were scraped to omit long checking for new listings.

OUTPUTS:
1. CSV file: 'otomoto_cars.csv'
- Appends all scraped car data with headers defined in CSV_COLUMNS.
2. Visited file: 'visited_urls.txt'
- Keeps track of visited car URLs/IDs so we don’t re-scrape them.

DEPENDENCIES:
- requests
- beautifulsoup4
- retry 
- Python 

USAGE:
1. Ensure dependencies are installed:
    pip install requests beautifulsoup4 retry
2. Run this script:
    python otomoto_scraper.py

CONFIGURATIONS:
- MAX_PAGES: Maximum pages per brand-group URL to scrape (default 510 - with some additional 10 pages just in case (it will just refresh 500)).
- MAX_CARS: Maximum total cars to scrape across all URLs (default 20000 - just in case).
- HEADERS: Custom User-Agent to mimic a real browser.
- CSV_COLUMNS: The fields we store in CSV.
"""

# SETTINGS / CONSTANTS
BASE_URL = (
    "https://www.otomoto.pl/osobowe?"
    "search%5Bfilter_enum_damaged%5D=0&"
    "search%5Border%5D=created_at_first%3Adesc"
)

# Brand-grouped URLs for 'used, no accident' cars
NEW_URLS = [
    "https://www.otomoto.pl/osobowe/uzywane/abarth--acura--aito--aiways--aixam--alfa-romeo--alpine--arcfox--asia--aston-martin?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/audi--austin--autobianchi--avatr--baic--bentley?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/alpina--bmw--brilliance--bugatti--buick--byd--cadillac--casalini--caterham--cenntro--changan--chatenet--chevrolet--chrysler--citroen--cupra?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/dacia--daewoo--daihatsu--delorean--dfm--dfsk--dkw--dodge--doosan--dr-motor--ds-automobiles--e-go--elaris--faw--fendt--ferrari--fiat--fisker--ford--forthing--gaz--geely--genesis--gmc--gwm--hiphi?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/honda--hongqi--hummer--hyundai--iamelectric--ineos--infiniti--isuzu--iveco--jac--jaecoo--jaguar--jeep--jetour--jinpeng--kia--inny?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/ktm--lada--lamborghini--lancia--land-rover--leapmotor--levc--lexus--ligier--lincoln--lixiang--lotus--lti--lucid--lynk-and-co--man--maserati--maximus--maxus--maybach--mazda--mclaren--mercedes-benz--mercury--mg--microcar--mini?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/mitsubishi--morgan--nio--nissan--nysa--oldsmobile--omoda--opel?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/peugeot--piaggio--plymouth--polestar--polonez--pontiac--porsche--ram--renault--rolls-royce--rover--saab--sarini--saturn--seat--seres--shuanghuan?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/skoda--skywell--skyworth--smart--ssangyong--subaru--suzuki--syrena--tarpan--tata--tesla?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/toyota--trabant--triumph--uaz--vauxhall--velex?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
    "https://www.otomoto.pl/osobowe/uzywane/volkswagen--volvo--voyah--waltra--marka_warszawa--wartburg--wolga--xiaomi--xpeng--zaporozec--zastawa--zeekr--zhidou--zuk?search%5Bfilter_enum_no_accident%5D=1&search%5Badvanced_search_expanded%5D=true",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

# Maximum number of pages to scrape per brand-group URL.
MAX_PAGES = 510  
# Maximum number of cars to scrape (across all URLs).
MAX_CARS = 20000  

CSV_FILE = "otomoto_cars.csv"
VISITED_FILE = "visited_urls.txt"

CSV_COLUMNS = [
    "url",
    "name",
    "description",
    "price",
    "make",
    "model",
    "version",
    "color",
    "door_count",
    "nr_seats",
    "year",
    "generation",
    "fuel_type",
    "engine_capacity",
    "engine_power",
    "body_type",
    "gearbox",
    "transmission",
    "country_origin",
    "mileage",
    "new_used",
    "registered",
    "no_accident",
]

# Prepare a single session for all requests
session = requests.Session()
session.headers.update(HEADERS)

def init_csv(file_path):
    """
    If the CSV file doesn't exist, create it and write the header row.
    """
    if not os.path.exists(file_path):
        with open(file_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

def load_visited_urls(file_path):
    """
    Load visited car IDs (or URLs) from the visited file into a set.
    Used to avoid re-scraping the same listing.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            visited = {line.strip() for line in f if line.strip()}
        return visited
    return set()

def append_to_csv(batch_data, csv_file):
    """
    Appends a batch of car records (list of dicts) to the CSV file.
    """
    with open(csv_file, mode="a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerows(batch_data)

def append_to_visited(file_path, batch_ids):
    """
    Appends a batch of visited car IDs to the visited file.
    """
    with open(file_path, "a", encoding="utf-8") as f:
        for car_id in batch_ids:
            f.write(car_id + "\n")

def scrape_car_details(car_url):
    """
    Scrapes the detail page of a specific car and returns a dict of relevant fields.
    Returns an empty dict if there's an issue or if data is not found.
    """
    details_dict = {}
    try:
        resp = session.get(car_url, timeout=10)
        if resp.status_code != 200:
            return details_dict

        soup = BeautifulSoup(resp.text, "html.parser")

        # Price
        price_tag = soup.find("h3", {"class": "offer-price__number"})
        if price_tag:
            details_dict["price"] = price_tag.get_text(strip=True)

        # Main details section
        main_details_section = soup.find("div", {"data-testid": "main-details-section"})
        if main_details_section:
            data_tests = main_details_section.find_all("div", {"data-testid": "detail"})
            for dt in data_tests:
                p_tags = dt.find_all("p")
                if len(p_tags) == 2:
                    attribute_name = p_tags[0].get_text(strip=True)
                    attribute_value = p_tags[1].get_text(strip=True)
                    details_dict[attribute_name] = attribute_value

        # Additional fields (some are in separate data-testid containers)
        data_testids = [
            "make", "model", "version", "color", "door_count", "nr_seats",
            "year", "generation", "fuel_type", "engine_capacity", "engine_power",
            "body_type", "gearbox", "transmission", "country_origin", "mileage",
            "new_used", "registered", "no_accident"
        ]
        for testid in data_testids:
            detail_section = soup.find("div", {"data-testid": testid})
            if detail_section:
                p_tags = detail_section.find_all("p")
                if len(p_tags) == 2:
                    attribute_name = p_tags[0].get_text(strip=True)
                    attribute_value = p_tags[1].get_text(strip=True)
                    details_dict[attribute_name] = attribute_value

    except Exception as e:
        print(f"[!] Exception while scraping {car_url}: {e}")

    return details_dict

def map_details_to_record(detail_data):
    """
    Maps raw detail data keys (Polish labels) to our standardized CSV column names.
    """
    mapping = {
        "Rok produkcji": "year",
        "Marka pojazdu": "make",
        "Model pojazdu": "model",
        "Wersja": "version",
        "Kolor": "color",
        "Liczba drzwi": "door_count",
        "Liczba miejsc": "nr_seats",
        "Generacja": "generation",
        "Rodzaj paliwa": "fuel_type",
        "Pojemność skokowa": "engine_capacity",
        "Moc": "engine_power",
        "Typ": "body_type",
        "Skrzynia biegów": "gearbox",
        "Napęd": "transmission",
        "Kraj pochodzenia": "country_origin",
        "Przebieg": "mileage",
        "Stan": "new_used",
        "Zarejestrowany w Polsce": "registered",
        "Bezwypadkowy": "no_accident",
        "price": "price",
    }

    record = {}
    for raw_key, mapped_key in mapping.items():
        if raw_key in detail_data:
            record[mapped_key] = detail_data[raw_key]
    return record

@retry(tries=20, delay=10, jitter=(1, 10))
def fetch_page(url):
    """
    Fetches the content of a page with retry logic.
    Raises an error if the request fails repeatedly.
    """
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    return resp.text

def scrape_otomoto_cars(url_list, max_pages=MAX_PAGES, max_cars=MAX_CARS):
    """
    Scrapes multiple pages of search results for each URL in url_list.
    If max_cars is given, stop once that many unique cars are scraped (across all URLs).
    """
    visited_urls = load_visited_urls(VISITED_FILE)
    init_csv(CSV_FILE)

    total_scraped = 0
    batch_size = 100
    csv_batch = []
    visited_batch = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        for base_url in url_list:
            current_page = 1
            while current_page <= max_pages and total_scraped < max_cars:
                page_url = f"{base_url}&page={current_page}"
                try:
                    page_content = fetch_page(page_url)
                    soup = BeautifulSoup(page_content, "html.parser")
                    search_results = soup.find("div", {"data-testid": "search-results"})

                    if not search_results:
                        print("[!] 'search-results' container not found. Possibly last page or no more data.")
                        break

                    car_elements = search_results.find_all("article", {"data-id": True})
                    if not car_elements:
                        print("[!] No car articles found on page.")
                        break

                    car_info_list = []
                    car_urls = []

                    # Collect listing data from the search results
                    for article in car_elements:
                        car_id = article.get("data-id", "").strip()
                        if not car_id or car_id in visited_urls:
                            continue

                        link_tag = article.find("a", href=True)
                        if not link_tag:
                            continue

                        car_url = link_tag["href"]
                        car_name = link_tag.get_text(strip=True)

                        # Basic listing description
                        section = article.find("section")
                        description_tag = section.find("p") if section else None
                        description_text = description_tag.get_text(strip=True) if description_tag else ""

                        car_info_list.append({
                            "car_id": car_id,
                            "url": car_url,
                            "name": car_name,
                            "description": description_text
                        })
                        car_urls.append(car_url)

                    if not car_urls:
                        print(f"[!] No new cars found on page {current_page} for this URL.")
                        current_page += 1
                        continue

                    # Fetch car details in parallel
                    future_to_url = {executor.submit(scrape_car_details, url): url for url in car_urls}
                    for future in as_completed(future_to_url):
                        url = future_to_url[future]
                        detail_data = future.result()

                        # Match the detail data to its base listing info
                        base_info = next((item for item in car_info_list if item["url"] == url), None)
                        if not base_info:
                            continue

                        car_record = {
                            "url": base_info["url"],
                            "name": base_info["name"],
                            "description": base_info["description"],
                        }

                        detail_record = map_details_to_record(detail_data)
                        car_record.update(detail_record)

                        csv_batch.append(car_record)
                        visited_batch.append(base_info["car_id"])
                        total_scraped += 1

                        if total_scraped >= max_cars:
                            break

                    # Write batches to CSV and visited file
                    if csv_batch:
                        append_to_csv(csv_batch, CSV_FILE)
                        csv_batch.clear()

                    if visited_batch:
                        append_to_visited(VISITED_FILE, visited_batch)
                        visited_urls.update(visited_batch)
                        visited_batch.clear()

                    print(f"[*] Finished page {current_page} of current URL. Total cars scraped so far: {total_scraped}")

                    current_page += 1

                except Exception as e:
                    print(f"[!] Error on page {current_page} for base_url {base_url}: {e}")
                    print("[*] Waiting for 10 seconds before retrying...")
                    time.sleep(10)

                # Stop if we hit max_cars in the middle of the loop
                if total_scraped >= max_cars:
                    break

            # If we've reached max_cars, no need to continue with the next URLs
            if total_scraped >= max_cars:
                break

    print(f"[*] Finished scraping. Total cars scraped: {total_scraped}")

# RUN SCRIPT DIRECTLY
if __name__ == "__main__":
    scrape_otomoto_cars(NEW_URLS, max_pages=MAX_PAGES, max_cars=MAX_CARS)
