import os
import csv
import requests
from datetime import datetime
from urllib.parse import urlparse
import time
import json

# Configuration
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID')
BASE_URL = "jobs.ashbyhq.com"
CSV_FILE = "ashby_companies.csv"

def get_existing_slugs():
    """Load existing slugs from CSV file"""
    existing_slugs = {}
    try:
        with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_slugs[row['slug']] = {
                    'company_name': row['company_name'],
                    'first_seen_date': row['first_seen_date'],
                    'last_checked_date': row['last_checked_date']
                }
    except FileNotFoundError:
        print(f"No existing CSV found. Creating new file: {CSV_FILE}")
    return existing_slugs

def extract_slug_from_url(url):
    """Extract company slug from Ashby URL"""
    parsed = urlparse(url)
    if BASE_URL in parsed.netloc:
        path_parts = parsed.path.strip('/').split('/')
        if path_parts and path_parts[0]:
            return path_parts[0]
    return None

def get_company_name_from_slug(slug):
    """Try to fetch company name from the actual page"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200:
            # Try to extract company name from page title or content
            content = response.text
            # Look for common patterns in Ashby pages
            if '<title>' in content:
                title = content.split('<title>')[1].split('</title>')[0]
                # Remove " - Ashby" or similar suffixes
                company_name = title.split(' - ')[0].strip()
                return company_name
            return slug.replace('-', ' ').title()
        else:
            return slug.replace('-', ' ').title()
    except:
        return slug.replace('-', ' ').title()

def google_custom_search(query, start_index=1):
    """Query Google Custom Search API"""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_CSE_ID,
        'q': query,
        'start': start_index,
        'num': 10  # Max results per request
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying Google CSE: {e}")
        return None

def discover_slugs_via_google():
    """Use Google Custom Search to discover all Ashby company slugs"""
    discovered_slugs = set()
    query = f"site:{BASE_URL}"
    
    print(f"Starting Google Custom Search discovery...")
    print(f"Query: {query}")
    
    start_index = 1
    total_results_estimate = None
    
    while True:
        print(f"Fetching results starting at index {start_index}...")
        result = google_custom_search(query, start_index)
        
        if not result:
            print("No results returned. Stopping.")
            break
        
        # Get total results estimate on first query
        if total_results_estimate is None and 'searchInformation' in result:
            total_results_estimate = result['searchInformation'].get('totalResults', 'unknown')
            print(f"Estimated total results: {total_results_estimate}")
        
        # Extract slugs from results
        if 'items' in result:
            for item in result['items']:
                url = item.get('link', '')
                slug = extract_slug_from_url(url)
                if slug:
                    discovered_slugs.add(slug)
                    print(f"  Found: {slug}")
        else:
            print("No more items in results. Stopping.")
            break
        
        # Check if there are more results
        if 'queries' in result and 'nextPage' in result['queries']:
            start_index = result['queries']['nextPage'][0]['startIndex']
            time.sleep(1)  # Be nice to the API
        else:
            print("No more pages available.")
            break
        
        # Google CSE has a limit of 100 results per query (10 pages × 10 results)
        # If we hit this limit, we need a different strategy
        if start_index > 100:
            print("Reached Google CSE limit for single query (100 results).")
            break
    
    print(f"\nTotal unique slugs discovered via Google: {len(discovered_slugs)}")
    return discovered_slugs

def discover_slugs_via_alphabet_search():
    """Search with alphabet prefixes to get more results beyond 100-result limit"""
    all_slugs = set()
    
    print("\n=== Starting alphabet-based discovery ===")
    print("This helps bypass the 100-result limit by searching with different prefixes")
    
    # Common starting letters and patterns
    prefixes = [chr(i) for i in range(ord('a'), ord('z')+1)]  # a-z
    prefixes.extend([f"{chr(i)}{chr(j)}" for i in range(ord('a'), ord('d')+1) for j in range(ord('a'), ord('z')+1)])  # aa-az, ba-bz, ca-cz, da-dz
    prefixes.append('')  # Empty prefix for base search
    
    for prefix in prefixes:
        query = f"site:{BASE_URL} {prefix}" if prefix else f"site:{BASE_URL}"
        print(f"\nSearching with prefix: '{prefix}' (query: {query})")
        
        start_index = 1
        while start_index <= 100:  # Google CSE max
            result = google_custom_search(query, start_index)
            
            if not result or 'items' not in result:
                break
            
            for item in result['items']:
                url = item.get('link', '')
                slug = extract_slug_from_url(url)
                if slug:
                    if slug not in all_slugs:
                        all_slugs.add(slug)
                        print(f"  New slug found: {slug}")
            
            if 'queries' not in result or 'nextPage' not in result['queries']:
                break
            
            start_index = result['queries']['nextPage'][0]['startIndex']
            time.sleep(1)  # Rate limiting
    
    print(f"\n=== Alphabet search complete ===")
    print(f"Total unique slugs discovered: {len(all_slugs)}")
    return all_slugs

def validate_slug(slug):
    """Check if a slug actually exists and returns 200"""
    url = f"https://{BASE_URL}/{slug}"
    try:
        response = requests.head(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        return response.status_code == 200
    except:
        return False

def save_to_csv(slugs_dict):
    """Save slugs dictionary to CSV file"""
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['slug', 'company_name', 'first_seen_date', 'last_checked_date']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by first_seen_date (newest first)
        sorted_slugs = sorted(slugs_dict.items(), key=lambda x: x[1]['first_seen_date'], reverse=True)
        
        for slug, data in sorted_slugs:
            writer.writerow({
                'slug': slug,
                'company_name': data['company_name'],
                'first_seen_date': data['first_seen_date'],
                'last_checked_date': data['last_checked_date']
            })
    
    print(f"\n✅ Saved {len(slugs_dict)} companies to {CSV_FILE}")

def main():
    print("=" * 60)
    print("ASHBY COMPANY SLUG DISCOVERY TOOL")
    print("=" * 60)
    
    # Load existing data
    existing_slugs = get_existing_slugs()
    print(f"\nExisting companies in database: {len(existing_slugs)}")
    
    # Discover new slugs using multiple methods
    discovered_slugs = set()
    
    # Method 1: Basic Google search
    discovered_slugs.update(discover_slugs_via_google())
    
    # Method 2: Alphabet-based search (to bypass 100-result limit)
    discovered_slugs.update(discover_slugs_via_alphabet_search())
    
    print(f"\n{'='*60}")
    print(f"DISCOVERY SUMMARY")
    print(f"{'='*60}")
    print(f"Total unique slugs discovered: {len(discovered_slugs)}")
    print(f"Existing slugs in database: {len(existing_slugs)}")
    
    # Find new slugs
    new_slugs = discovered_slugs - set(existing_slugs.keys())
    print(f"New slugs to add: {len(new_slugs)}")
    
    if new_slugs:
        print(f"\n🆕 NEW COMPANIES DISCOVERED:")
        for slug in sorted(new_slugs):
            print(f"  - {slug}")
    
    # Update database
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Add new slugs
    for slug in new_slugs:
        print(f"\nProcessing new slug: {slug}")
        company_name = get_company_name_from_slug(slug)
        existing_slugs[slug] = {
            'company_name': company_name,
            'first_seen_date': today,
            'last_checked_date': today
        }
        time.sleep(0.5)  # Be nice to the server
    
    # Update last_checked_date for all existing slugs
    for slug in existing_slugs:
        existing_slugs[slug]['last_checked_date'] = today
    
    # Save to CSV
    save_to_csv(existing_slugs)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETE!")
    print(f"{'='*60}")
    print(f"Total companies in database: {len(existing_slugs)}")
    print(f"New companies added today: {len(new_slugs)}")
    print(f"CSV file: {CSV_FILE}")

if __name__ == "__main__":
    main()
