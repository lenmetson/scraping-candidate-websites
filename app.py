import pandas as pd
import aiohttp
import asyncio

from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, unquote
from pathlib import Path


# Step 1: Read the CSV file
csv_file_path = 'data/dc-candidates-election_date___ballot_paper_id___election_id_parl2024-07-04__party_id___cancelled___field_group_person-2024-05-29T11-36-57.csv'
df = pd.read_csv(csv_file_path)

# Step 2: Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Step 3: Extract relevant columns and drop rows with missing URLs
data = df[['person_id', 'person_name', 'homepage_url']].dropna()

# Step 4: Clean the URLs
data['homepage_url'] = data['homepage_url'].apply(lambda x: unquote(x.strip(' "\'').split('%22')[0]))

# Step 5: Select only the first 5 rows
data = data.head(5)

def standardize_url(url):
    """Standardize URL to ensure uniformity for deduplication."""
    parsed_url = urlparse(url)
    standardized_url = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
    return standardized_url.rstrip('/')

# Step 6: Function to extract all internal links from a webpage
def extract_internal_links(soup, base_url):
    internal_links = set()
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        url = urljoin(base_url, href)
        # add the condition to remove every url contain "search?"
        if urlparse(base_url).netloc == urlparse(url).netloc and 'search?' not in url:
            standardized_url = standardize_url(url)
            internal_links.add(standardized_url)
    return internal_links


# Step 7: Async function to fetch a webpage content
async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 403:
                print(f"Failed to fetch {url}: 403 Forbidden")
                return f'Error: 403 Forbidden'
            response.raise_for_status()
            try:
                page_content = await response.text()
            except UnicodeDecodeError:
                print(f"Failed to decode {url}")
                return f'Error: Unicode Decode Error'
            return page_content
    except aiohttp.ClientError as e:
        print(f"Failed to fetch {url}: {e}")
        return f'Error: {str(e)}'

# Step 8: Async function to scrape all pages within a website with depth limiting

#(below block of code if we want to limit the depth)
# async def scrape_website(session, base_url, max_depth=4):
#     visited = set()
#     to_visit = {(base_url, 0)}  # Store tuples of (URL, depth)
#     content = []
#     content_urls = []

#     while to_visit:
#         url, depth = to_visit.pop()
#         if url not in visited and depth <= max_depth:
#             visited.add(url)
#             print(f"Fetching: {url} at depth {depth}")
#             page_content = await fetch_page(session, url)
#             if not page_content.startswith('Error:'):
#                 content.append((url, page_content))
#                 content_urls.append(url)
#                 soup = BeautifulSoup(page_content, 'html.parser')
#                 if depth < max_depth:
#                     internal_links = extract_internal_links(soup, base_url)
#                     to_visit.update((link, depth + 1) for link in internal_links if link not in visited)
    
#     return content, content_urls




async def scrape_website(session, base_url):
    visited = set()
    to_visit = {standardize_url(base_url)}
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

# Step 9: Async function to handle each candidate's website
async def fetch(session, person_id, person_name, homepage_url, global_visited):
    try:
        content_list, content_urls = await scrape_website(session, homepage_url)
        content_urls = list(set(content_urls))  # Ensure unique URLs within each scrape

        # Add deduplication across all candidates
        unique_content_urls = [url for url in content_urls if url not in global_visited]
        global_visited.update(unique_content_urls)
        
        return {
            'person_id': person_id,
            'person_name': person_name,
            'website_url': homepage_url,
            'content_list': content_list,
            'content_urls': unique_content_urls
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


# Step 10: Async function to create tasks for each row
async def scrape_all(rows):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        global_visited = set()
        tasks = [fetch(session, row['person_id'], row['person_name'], row['homepage_url'], global_visited) for _, row in rows.iterrows()]
        results = await asyncio.gather(*tasks)
        
        # Check for duplicates across all results
        all_urls = [url for result in results for url in result['content_urls']]
        duplicate_urls = [url for url in set(all_urls) if all_urls.count(url) > 1]
        if duplicate_urls:
            print(f"Duplicate URLs found: {duplicate_urls}")
        
        return results

# Step 11: Run the async scraping
if __name__ == '__main__':
    results = asyncio.run(scrape_all(data))
    
    # Step 12: Create directories and save content
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