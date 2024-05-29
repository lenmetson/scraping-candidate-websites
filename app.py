import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup


# Step 1: Read the CSV file
csv_file_path = 'data/dc-candidates-election_date___ballot_paper_id___election_id_parl2024-07-04__party_id___cancelled___field_group_person-2024-05-29T11-36-57.csv'
df = pd.read_csv(csv_file_path)


# Step 2: Extract the homepage URLs
homepage_urls = df['homepage_url'].dropna().tolist()

# Step 3: Async function to scrape a webpage
async def fetch(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()  # Check if the request was successful
            page_content = await response.text()
            soup = BeautifulSoup(page_content, 'html.parser')
            # Example: Get the title of the page
            title = soup.title.string if soup.title else 'No Title Found'
            return {'url': url, 'title': title}
    except aiohttp.ClientError as e:
        return {'url': url, 'error': str(e)}

# Step 4: Async function to create tasks for each URL
async def scrape_all(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)

# Step 5: Run the async scraping
if __name__ == '__main__':
    results = asyncio.run(scrape_all(homepage_urls))
    
    # Step 6: Print or save the results
    for result in results:
        print(result)