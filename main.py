import asyncio
import aiohttp
import json
import os
import csv
import io

from bs4 import BeautifulSoup
from fastapi.responses import StreamingResponse, FileResponse
from urllib.parse import urljoin
from fastapi import FastAPI

import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = FastAPI()

# --- Config ---
MAX_CONCURRENT = 50  # simultaneous connections (tune as needed)
REQUEST_TIMEOUT = 15  # seconds per request
RETRY_ATTEMPTS = 3  # retries on failure

VALID_HEADERS = [
    'Closing Price',
    "Day's Value (mn)",
    "Day's Volume (Nos.)",
    'Total No. of Outstanding Securities'
]


# ──────────────────────────────────────────────
# 1. Scrape company list (one-time, sync is fine)
# ──────────────────────────────────────────────
def get_company_list() -> list[dict]:
    """Scrape all company links from DSE listing page."""
    import requests
    url = "https://www.dsebd.org/company_listing.php"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        companies = []
        for body_div in soup.find_all('div', class_='BodyContent'):
            for a_tag in body_div.find_all('a', href=True):
                companies.append({
                    'title': a_tag.get_text(strip=True),
                    'href': urljoin(url, a_tag['href']),
                })
        print(f"[+] Found {len(companies)} companies to scrape.")
        return companies

    except Exception as e:
        print(f"[!] Failed to fetch company list: {e}")
        return []


# ──────────────────────────────────────────────
# 2. Async scrape a single company page
# ──────────────────────────────────────────────
async def scrape_company_async(
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        company: dict,
) -> tuple[str, dict]:
    """Fetch and parse a single company page. Returns (title, data_dict)."""
    title = company['title']
    url = company['href']

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    resp.raise_for_status()
                    html = await resp.text()

            soup = BeautifulSoup(html, 'html.parser')
            combined = {}
            current_header = None

            for body_div in soup.find_all('div', class_='table-responsive'):
                for element in body_div.find_all(['th', 'td']):
                    if element.name == 'th':
                        text = element.get_text(strip=True)
                        current_header = text if text in VALID_HEADERS else None
                    elif element.name == 'td' and current_header:
                        combined[current_header] = element.get_text(strip=True)
                        current_header = None

            print(f"  [✓] {title}")
            return title, combined

        except Exception as e:
            if attempt < RETRY_ATTEMPTS:
                await asyncio.sleep(2 ** attempt)  # exponential back-off
            else:
                print(f"  [✗] {title} failed after {RETRY_ATTEMPTS} attempts: {e}")
                return title, {}


# ──────────────────────────────────────────────
# 3. Run all scrapes concurrently
# ──────────────────────────────────────────────
async def scrape_all(companies: list[dict]) -> dict:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            scrape_company_async(session, semaphore, company)
            for company in companies
        ]
        results = await asyncio.gather(*tasks)

    return {title: data for title, data in results}


# ──────────────────────────────────────────────
# 4. Save to Google Sheets (batch update)
# ──────────────────────────────────────────────
def save_to_sheets(data: dict):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
    client = gspread.authorize(creds)

    sheet = client.open("DSE_Data").sheet1

    headers = [
        'Company',
        'Closing Price',
        "Day's Value (mn)",
        "Day's Volume (Nos.)",
        'Total No. of Outstanding Securities',
    ]
    rows = [headers] + [
        [
            company,
            values.get('Closing Price', ''),
            values.get("Day's Value (mn)", ''),
            values.get("Day's Volume (Nos.)", ''),
            values.get('Total No. of Outstanding Securities', ''),
        ]
        for company, values in data.items()
    ]

    sheet.clear()
    # batch_update writes everything in ONE API call — much faster than row-by-row
    sheet.update(rows, 'A1')
    print(f"[+] Google Sheet updated with {len(rows) - 1} companies.")


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
            writer.writerow(row)


def generate_csv_response(data: dict):
    """Generates an in-memory CSV and returns a FastAPI StreamingResponse."""
    fieldnames = [
        'Company',
        'Closing Price',
        "Day's Value (mn)",
        "Day's Volume (Nos.)",
        'Total No. of Outstanding Securities'
    ]

    # Create an in-memory string buffer
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()
    for company, values in data.items():
        # Ensure we only include keys that are in fieldnames to avoid errors
        row = {'Company': company}
        for field in fieldnames[1:]:
            row[field] = values.get(field, '')
        writer.writerow(row)

    # Fetch the content and seek to 0 is not needed if we use getvalue()
    # But for StreamingResponse, we wrap it in an iterator
    csv_content = output.getvalue()
    output.close()

    return StreamingResponse(
        io.BytesIO(csv_content.encode("utf-8")),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=dse_scraped_data.csv"
        }
    )


# ──────────────────────────────────────────────
# 5. FastAPI endpoints
# ──────────────────────────────────────────────
@app.get("/execute")
async def execute_logic():  # ← async endpoint
    companies = get_company_list()
    if not companies:
        return {"status": "error", "detail": "Could not fetch company list"}

    final_data = await scrape_all(companies)  # concurrent scraping
    # save_to_csv(final_data)
    # save_to_sheets(final_data)
    return generate_csv_response(final_data)
    # return {
    #     "status": "success",
    #     "companies_scraped": len(final_data),
    #     "detail": "Data saved to Google Sheets",
    # }


@app.get("/health")
def health_check():
    return {"status": "active", "message": "FastAPI is running"}


@app.get("/")
def main_page():
    # This serves the index.html file you just created
    return FileResponse("index.html")


@app.get("/execute-live")
async def execute_live():
    async def event_generator():
        companies = get_company_list()
        yield f"data: Found {len(companies)} companies. Starting...\n\n"

        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            # We use as_completed to yield names IMMEDIATELY as they finish
            tasks = [scrape_company_async(session, semaphore, c) for c in companies[:30]]  # Limit for test

            for task in asyncio.as_completed(tasks):
                title, _ = await task
                if title:
                    yield f"data: {title}\n\n"

        # CRITICAL: This exact string tells the JS to stop the spinner
        yield "data: FINISHED_ALL\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")