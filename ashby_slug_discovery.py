import os
import csv
import requests
from datetime import datetime
from urllib.parse import urlparse
import time
import json
import re

# Configuration
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID')
BASE_URL = "jobs.ashbyhq.com"
CSV_FILE = "ashby_companies.csv"
ZERO_JOBS_FILE = "ashby_zero_jobs.csv"
FEW_JOBS_FILE = "ashby_few_jobs.csv"  # 1-4 jobs

# Thresholds
FEW_JOBS_THRESHOLD = 5  # Companies with fewer than this many jobs

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
                    'last_checked_date': row['last_checked_date'],
                    'job_count': row.get('job_count', '0')
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

def check_job_postings_via_api(slug):
    """Try to get job count via Ashby's API endpoint"""
    try:
        # Ashby exposes job data through their API
        api_url = f"https://app.ashbyhq.com/api/xml-feed/job-postings/organization/{slug}"
        response = requests.get(api_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        
        if response.status_code == 200:
            # Count job entries in XML
            content = response.text
            job_count = content.count('<job>')
            return job_count, "API Success"
        else:
            return None, f"API returned {response.status_code}"
    except Exception as e:
        return None, f"API Error: {str(e)}"

def check_job_postings_via_page(slug):
    """Fallback: Check job count by scraping the page"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        if response.status_code != 200:
            return 0, f"Page returned {response.status_code}"
        
        content = response.text
        
        # Method 1: Look for explicit "no jobs" messages
        no_jobs_indicators = [
            'no open positions',
            'no positions available',
            'no current openings',
            'no openings at this time',
            'not currently hiring',
            'no active job postings'
        ]
        
        content_lower = content.lower()
        for indicator in no_jobs_indicators:
            if indicator in content_lower:
                return 0, "Page shows no openings message"
        
        # Method 2: Count job-related HTML elements
        job_count = 0
        
        # Common Ashby patterns
        patterns = [
            r'data-job-id="[^"]*"',  # Job IDs
            r'class="[^"]*job-posting[^"]*"',  # Job posting elements
            r'<li[^>]*class="[^"]*ashby-job[^"]*"',  # List items
            r'role="article"[^>]*data-job',  # Article elements with job data
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            job_count = max(job_count, len(matches))
        
        # Method 3: Look for structured data (JSON-LD)
        json_ld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
        json_matches = re.findall(json_ld_pattern, content, re.DOTALL)
        
        for json_str in json_matches:
            try:
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get('@type') == 'JobPosting':
                    job_count += 1
                elif isinstance(data, list):
                    job_count += sum(1 for item in data if isinstance(item, dict) and item.get('@type') == 'JobPosting')
            except:
                pass
        
        return job_count, "Page scraping"
        
    except Exception as e:
        return 0, f"Page Error: {str(e)}"

def get_job_count(slug):
    """Get job count using multiple methods"""
    # Try API first (most reliable)
    job_count, status = check_job_postings_via_api(slug)
    
    if job_count is not None:
        return job_count, status
    
    # Fallback to page scraping
    job_count, status = check_job_postings_via_page(slug)
    return job_count, status

def get_company_name_from_slug(slug):
    """Try to fetch company name from the actual page"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200:
            content = response.text
            
            # Try multiple methods to extract company name
            # Method 1: Page title
            if '<title>' in content:
                title = content.split('<title>')[1].split('</title>')[0]
                company_name = title.split(' - ')[0].split(' | ')[0].strip()
                if company_name and company_name.lower() != 'jobs':
                    return company_name
            
            # Method 2: Meta tags
            meta_patterns = [
                r'<meta property="og:site_name" content="([^"]+)"',
                r'<meta name="application-name" content="([^"]+)"',
            ]
            for pattern in meta_patterns:
                match = re.search(pattern, content)
                if match:
                    return match.group(1).strip()
            
            # Fallback: Format slug
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
        'num': 10
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
        
        if total_results_estimate is None and 'searchInformation' in result:
            total_results_estimate = result['searchInformation'].get('totalResults', 'unknown')
            print(f"Estimated total results: {total_results_estimate}")
        
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
        
        if 'queries' in result and 'nextPage' in result['queries']:
            start_index = result['queries']['nextPage'][0]['startIndex']
            time.sleep(1)
        else:
            print("No more pages available.")
            break
        
        if start_index > 100:
            print("Reached Google CSE limit for single query (100 results).")
            break
    
    print(f"\nTotal unique slugs discovered via Google: {len(discovered_slugs)}")
    return discovered_slugs

def discover_slugs_via_alphabet_search():
    """Search with alphabet prefixes to get more results beyond 100-result limit"""
    all_slugs = set()
    
    print("\n=== Starting alphabet-based discovery ===")
    
    prefixes = [chr(i) for i in range(ord('a'), ord('z')+1)]
    prefixes.extend([f"{chr(i)}{chr(j)}" for i in range(ord('a'), ord('d')+1) for j in range(ord('a'), ord('z')+1)])
    prefixes.append('')
    
    for prefix in prefixes:
        query = f"site:{BASE_URL} {prefix}" if prefix else f"site:{BASE_URL}"
        print(f"\nSearching with prefix: '{prefix}'")
        
        start_index = 1
        while start_index <= 100:
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
            time.sleep(1)
    
    print(f"\n=== Alphabet search complete ===")
    print(f"Total unique slugs discovered: {len(all_slugs)}")
    return all_slugs

def save_to_csv(slugs_dict):
    """Save slugs dictionary to CSV file"""
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['slug', 'company_name', 'first_seen_date', 'last_checked_date', 'job_count']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        sorted_slugs = sorted(slugs_dict.items(), key=lambda x: x[1]['first_seen_date'], reverse=True)
        
        for slug, data in sorted_slugs:
            writer.writerow({
                'slug': slug,
                'company_name': data['company_name'],
                'first_seen_date': data['first_seen_date'],
                'last_checked_date': data['last_checked_date'],
                'job_count': data.get('job_count', '0')
            })
    
    print(f"\n✅ Saved {len(slugs_dict)} companies to {CSV_FILE}")

def save_filtered_lists(slugs_dict):
    """Save filtered lists of companies by job count"""
    zero_jobs = {}
    few_jobs = {}
    
    for slug, data in slugs_dict.items():
        try:
            job_count = int(data.get('job_count', 0))
            if job_count == 0:
                zero_jobs[slug] = data
            elif job_count < FEW_JOBS_THRESHOLD:
                few_jobs[slug] = data
        except:
            pass
    
    # Save zero jobs list
    if zero_jobs:
        with open(ZERO_JOBS_FILE, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['slug', 'company_name', 'first_seen_date', 'job_count', 'url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            sorted_zero = sorted(zero_jobs.items(), key=lambda x: x[1]['first_seen_date'], reverse=True)
            
            for slug, data in sorted_zero:
                writer.writerow({
                    'slug': slug,
                    'company_name': data['company_name'],
                    'first_seen_date': data['first_seen_date'],
                    'job_count': data['job_count'],
                    'url': f"https://{BASE_URL}/{slug}"
                })
        
        print(f"\n🆕 Saved {len(zero_jobs)} companies with ZERO jobs to {ZERO_JOBS_FILE}")
    
    # Save few jobs list (1-4 jobs)
    if few_jobs:
        with open(FEW_JOBS_FILE, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['slug', 'company_name', 'first_seen_date', 'job_count', 'url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            sorted_few = sorted(few_jobs.items(), key=lambda x: x[1]['first_seen_date'], reverse=True)
            
            for slug, data in sorted_few:
                writer.writerow({
                    'slug': slug,
                    'company_name': data['company_name'],
                    'first_seen_date': data['first_seen_date'],
                    'job_count': data['job_count'],
                    'url': f"https://{BASE_URL}/{slug}"
                })
        
        print(f"\n🔥 Saved {len(few_jobs)} companies with 1-{FEW_JOBS_THRESHOLD-1} jobs to {FEW_JOBS_FILE}")

def main():
    print("=" * 60)
    print("ASHBY COMPANY SLUG DISCOVERY TOOL")
    print(f"Flagging companies with fewer than {FEW_JOBS_THRESHOLD} jobs")
    print("=" * 60)
    
    existing_slugs = get_existing_slugs()
    print(f"\nExisting companies in database: {len(existing_slugs)}")
    
    discovered_slugs = set()
    discovered_slugs.update(discover_slugs_via_google())
    discovered_slugs.update(discover_slugs_via_alphabet_search())
    
    print(f"\n{'='*60}")
    print(f"DISCOVERY SUMMARY")
    print(f"{'='*60}")
    print(f"Total unique slugs discovered: {len(discovered_slugs)}")
    print(f"Existing slugs in database: {len(existing_slugs)}")
    
    new_slugs = discovered_slugs - set(existing_slugs.keys())
    print(f"New slugs to add: {len(new_slugs)}")
    
    if new_slugs:
        print(f"\n🆕 NEW COMPANIES DISCOVERED:")
        for slug in sorted(new_slugs):
            print(f"  - {slug}")
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Process new slugs
    if new_slugs:
        print(f"\n{'='*60}")
        print(f"PROCESSING NEW COMPANIES...")
        print(f"{'='*60}")
        
        for i, slug in enumerate(new_slugs, 1):
            print(f"\n[{i}/{len(new_slugs)}] Processing: {slug}")
            
            company_name = get_company_name_from_slug(slug)
            job_count, status = get_job_count(slug)
            
            print(f"  Company: {company_name}")
            print(f"  Job count: {job_count}")
            print(f"  Status: {status}")
            
            existing_slugs[slug] = {
                'company_name': company_name,
                'first_seen_date': today,
                'last_checked_date': today,
                'job_count': str(job_count)
            }
            
            time.sleep(0.5)
    
    # Update last_checked_date for all slugs
    for slug in existing_slugs:
        if slug not in new_slugs:
            existing_slugs[slug]['last_checked_date'] = today
    
    # Save all results
    save_to_csv(existing_slugs)
    save_filtered_lists(existing_slugs)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETE!")
    print(f"{'='*60}")
    print(f"Total companies in database: {len(existing_slugs)}")
    print(f"New companies added today: {len(new_slugs)}")
    
    # Stats
    zero_count = sum(1 for data in existing_slugs.values() if data.get('job_count') == '0')
    few_count = sum(1 for data in existing_slugs.values() 
                    if data.get('job_count', '0').isdigit() and 0 < int(data.get('job_count', '0')) < FEW_JOBS_THRESHOLD)
    many_count = sum(1 for data in existing_slugs.values() 
                     if data.get('job_count', '0').isdigit() and int(data.get('job_count', '0')) >= FEW_JOBS_THRESHOLD)
    
    print(f"\n📊 JOB COUNT BREAKDOWN:")
    print(f"  0 jobs (brand new!): {zero_count}")
    print(f"  1-{FEW_JOBS_THRESHOLD-1} jobs (very new!): {few_count}")
    print(f"  {FEW_JOBS_THRESHOLD}+ jobs (established): {many_count}")
    
    print(f"\n📁 FILES CREATED:")
    print(f"  {CSV_FILE} - All companies")
    print(f"  {ZERO_JOBS_FILE} - Companies with 0 jobs")
    print(f"  {FEW_JOBS_FILE} - Companies with 1-{FEW_JOBS_THRESHOLD-1} jobs")

if __name__ == "__main__":
    main()
