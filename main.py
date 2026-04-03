import csv

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from fastapi import FastAPI

import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = FastAPI()


def get_company_list():
    """Scrape company listings from DSE website and return structured data."""
    url = "https://www.dsebd.org/company_listing.php"
    try:
        # Send HTTP request
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes

        # Parse HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        all_body_contents = soup.find_all('div', class_='BodyContent')

        # 3. Extract all <a> tags from each BodyContent div
        company_data_lists = []
        for index, body_div in enumerate(all_body_contents, 1):
            for a_tag in body_div.find_all('a', href=True):
                link = {
                    'title': a_tag.get_text(strip=True),
                    'href': urljoin(url, a_tag['href']),  # Convert to absolute URL
                    'link_text': ' '.join(a_tag.get_text().split()),  # Cleaned text
                }
                company_data_lists.append(link)
        return company_data_lists

    except Exception as e:
        print(f"Error scraping company list: {str(e)}")
        return []  # Return empty list if scraping fails


def scrape_company_data(company_data):
    url = company_data['href']
    VALID_HEADERS = [
        'Closing Price',
        "Day's Value (mn)",
        "Day's Volume (Nos.)",
        'Total No. of Outstanding Securities'
    ]
    if not url:
        return {}
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table with company information (adjust selector as needed)
        table = soup.find_all('div', class_='table-responsive')
        c = []
        data = []
        for index, body_div in enumerate(table, 1):
            current_section = {}
            for element in body_div.find_all(['th', 'td']):
                c.append(element)
                if element.name == 'th':
                    current_header = element.get_text(strip=True)
                    current_header = current_header if current_header in VALID_HEADERS else None
                elif element.name == 'td' and current_header:
                    current_section[current_header] = element.get_text(strip=True)
                    current_header = None
            if current_section:
                data.append(current_section)
            combined_dict = {}
            for item in data:
                combined_dict.update(item)
        return combined_dict
    except Exception as e:
        print(f"Error scraping data for {company_data.get('title', 'Unknown')}: {str(e)}")
        return {}


def save_to_csv(data, filename="scraped_data.csv"):
    fieldnames = [
        'Company',
        'Closing Price',
        'Day\'s Value (mn)',
        'Day\'s Volume (Nos.)',
        'Total No. of Outstanding Securities'
    ]
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()  # Write column names
        for company, values in data.items():
            row = {'Company': company}
            row.update(values)
            writer.writerow(row)  # Write all rows


def save_to_sheets(data):
    # Auth setup
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    # Open your sheet (Change 'DSE_Data' to your sheet name)
    sheet = client.open("DSE_Data").sheet1

    # Prepare data for batch update (Headers + Rows)
    headers = ['Company', 'Closing Price', "Day's Value (mn)", "Day's Volume (Nos.)",
               'Total No. of Outstanding Securities']
    rows = [headers]

    for company, values in data.items():
        rows.append([
            company,
            values.get('Closing Price', ''),
            values.get("Day's Value (mn)", ''),
            values.get("Day's Volume (Nos.)", ''),
            values.get('Total No. of Outstanding Securities', '')
        ])

    # Clear old data and update with new
    sheet.clear()
    sheet.update(rows, 'A1')


# Press the green button in the gutter to run the script.
@app.get("/execute")
def execute_logic():
    company_list_data = get_company_list()
    final_data_companies = {}
    # for test
    # temp = company_list_data[:10]
    print("Total Number of Company to scrape : ", len(company_list_data))
    for company in company_list_data:
        print("Data processing start for : ", company['title'])
        data = scrape_company_data(company)
        final_data_companies[company['title']] = data
        print("Data processing ended for : ", company['title'])
    # save_to_csv(final_data_companies, 'DSE_COMPANIES_LIST.csv')
    save_to_sheets(final_data_companies)
    return {"status": "success", "detail": "Logic executed from Google Sheets"}


@app.get("/")
def health_check():
    # Render uses this to see if the app is 'Live'
    return {"status": "active", "message": "FastAPI is running"}
