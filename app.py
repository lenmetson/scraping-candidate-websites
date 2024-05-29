import pandas as pd
import aiohttp
import asyncio

from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, unquote
from pathlib import Path
import os

# Step 1: Read the CSV file
csv_file_path = 'data/dc-candidates-election_date___ballot_paper_id___election_id_parl2024-07-04__party_id___cancelled___field_group_person-2024-05-29T11-36-57.csv'
df = pd.read_csv(csv_file_path)

# Step 2: Extract relevant columns and drop rows with missing URLs
data = df[['person_id', 'person_name', 'homepage_url']].dropna()

# Step 3: Clean the URLs
data['homepage_url'] = data['homepage_url'].apply(lambda x: unquote(x.strip(' "\'').split('%22')[0]))

# Step 4: Select only the first 3 rows ( delete this line after the scarapping is successful)
data = data.head(3)

# Step 5: Function to extract all internal links from a webpage
def extract_internal_links(soup, base_url):
    internal_links = set()
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        url = urljoin(base_url, href)
        if urlparse(base_url).netloc == urlparse(url).netloc:
            internal_links.add(url)
    return internal_links

# Step 6: Async function to fetch a webpage content
async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 403:
                print(f"Failed to fetch {url}: 403 Forbidden")
                return f'Error: 403 Forbidden'
            response.raise_for_status()
            page_content = await response.text()
            return page_content
    except aiohttp.ClientError as e:
        print(f"Failed to fetch {url}: {e}")
        return f'Error: {str(e)}'

# Step 7: Async function to scrape all pages within a website
async def scrape_website(session, base_url):
    visited = set()
    to_visit = set([base_url])
    content = []
    content_urls = []

    while to_visit:
        url = to_visit.pop()
        if url not in visited:
            visited.add(url)
            print(f"Fetching: {url}")
            page_content = await fetch_page(session, url)
            if not page_content.startswith('Error:'):
                content.append((url, page_content))
                content_urls.append(url)
                soup = BeautifulSoup(page_content, 'html.parser')
                internal_links = extract_internal_links(soup, base_url)
                to_visit.update(internal_links - visited)
    
    return content, content_urls

# Step 8: Async function to handle each candidate's website
async def fetch(session, person_id, person_name, homepage_url):
    try:
        content_list, content_urls = await scrape_website(session, homepage_url)
        return {
            'person_id': person_id,
            'person_name': person_name,
            'website_url': homepage_url,
            'content_list': content_list,
            'content_urls': content_urls
        }
    except Exception as e:
        print(f"Error with {homepage_url}: {e}")
        return {
            'person_id': person_id,
            'person_name': person_name,
            'website_url': homepage_url,
            'content_list': [f'Error: {str(e)}'],
            'content_urls': []
        }

# Step 9: Async function to create tasks for each row
async def scrape_all(rows):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        tasks = [fetch(session, row['person_id'], row['person_name'], row['homepage_url']) for _, row in rows.iterrows()]
        return await asyncio.gather(*tasks)

# Step 10: Run the async scraping
if __name__ == '__main__':
    results = asyncio.run(scrape_all(data))
    
    # Step 11: Create directories and save content
    base_dir = Path('scraped_content')
    base_dir.mkdir(exist_ok=True)
    
    for result in results:
        person_id = result['person_id']
        person_name = result['person_name'].replace(' ', '_')
        website_url = result['website_url'].replace('://', '_').replace('/', '_').replace('.', '_')
        
        # Create directories
        person_dir = base_dir / f"{person_id}_{person_name}"
        website_dir = person_dir / website_url
        website_dir.mkdir(parents=True, exist_ok=True)
        
        # Save HTML content
        for idx, (url, content) in enumerate(result['content_list']):
            # Create subdirectory for internal links
            cleaned_url = url.replace('://', '_').replace('/', '_').replace('.', '_')
            content_dir = website_dir / cleaned_url
            content_dir.mkdir(parents=True, exist_ok=True)
            
            content_path = content_dir / f'content_{idx + 1}.html'
            with open(content_path, 'w', encoding='utf-8') as file:
                file.write(content)
    
    print(f"Scraped content saved to {base_dir}")