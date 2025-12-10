import os
import csv
import requests
from datetime import datetime
from urllib.parse import urlparse
import time
import json
import re
import itertools

# Configuration
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID')
BASE_URL = "jobs.ashbyhq.com"
CSV_FILE = "ashby_companies.csv"
ZERO_JOBS_FILE = "ashby_zero_jobs.csv"
FEW_JOBS_FILE = "ashby_few_jobs.csv"
FEW_JOBS_THRESHOLD = 5

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
            # Clean up the slug
            slug = path_parts[0].lower().strip()
            # Remove query parameters if any
            slug = slug.split('?')[0].split('#')[0]
            return slug if slug else None
    return None

def validate_slug(slug):
    """Check if a slug exists and returns 200"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.head(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        return response.status_code == 200
    except:
        return False

def check_job_postings_via_api(slug):
    """Try to get job count via Ashby's API endpoint"""
    try:
        api_url = f"https://app.ashbyhq.com/api/xml-feed/job-postings/organization/{slug}"
        response = requests.get(api_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        
        if response.status_code == 200:
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
        
        job_count = 0
        patterns = [
            r'data-job-id="[^"]*"',
            r'class="[^"]*job-posting[^"]*"',
            r'<li[^>]*class="[^"]*ashby-job[^"]*"',
            r'role="article"[^>]*data-job',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            job_count = max(job_count, len(matches))
        
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
    job_count, status = check_job_postings_via_api(slug)
    
    if job_count is not None:
        return job_count, status
    
    job_count, status = check_job_postings_via_page(slug)
    return job_count, status

def get_company_name_from_slug(slug):
    """Try to fetch company name from the actual page"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code == 200:
            content = response.text
            
            if '<title>' in content:
                title = content.split('<title>')[1].split('</title>')[0]
                company_name = title.split(' - ')[0].split(' | ')[0].strip()
                if company_name and company_name.lower() != 'jobs':
                    return company_name
            
            meta_patterns = [
                r'<meta property="og:site_name" content="([^"]+)"',
                r'<meta name="application-name" content="([^"]+)"',
            ]
            for pattern in meta_patterns:
                match = re.search(pattern, content)
                if match:
                    return match.group(1).strip()
            
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

def discover_via_google_exhaustive():
    """Exhaustive Google search with ALL 2-letter combinations"""
    all_slugs = set()
    
    print("\n" + "="*60)
    print("METHOD 1: EXHAUSTIVE GOOGLE CUSTOM SEARCH")
    print("="*60)
    
    # Generate ALL possible search prefixes exhaustively
    prefixes = ['']
    
    # Single letters (a-z)
    prefixes.extend([chr(i) for i in range(ord('a'), ord('z')+1)])
    
    # Single digits (0-9)
    prefixes.extend([str(i) for i in range(10)])
    
    # ALL two-letter combinations (26 x 26 = 676)
    print("Generating all 2-letter combinations...")
    two_letter = []
    for c1 in 'abcdefghijklmnopqrstuvwxyz':
        for c2 in 'abcdefghijklmnopqrstuvwxyz':
            two_letter.append(c1 + c2)
    prefixes.extend(two_letter)
    print(f"Generated {len(two_letter)} two-letter combinations")
    
    # Two-letter with numbers (a0, a1... z9)
    for c1 in 'abcdefghijklmnopqrstuvwxyz':
        for c2 in '0123456789':
            prefixes.append(c1 + c2)
    
    # Numbers with letters (0a, 1a... 9z)
    for c1 in '0123456789':
        for c2 in 'abcdefghijklmnopqrstuvwxyz':
            prefixes.append(c1 + c2)
    
    print(f"Will search with {len(prefixes)} different prefixes (including all 2-letter combos)...")
    print(f"Estimated time: 30-45 minutes for exhaustive search\n")
    
    for i, prefix in enumerate(prefixes):
        query = f"site:{BASE_URL} {prefix}" if prefix else f"site:{BASE_URL}"
        
        if i % 50 == 0:
            print(f"\nProgress: {i}/{len(prefixes)} prefixes searched")
            print(f"Unique slugs found so far: {len(all_slugs)}")
        
        start_index = 1
        page_count = 0
        while start_index <= 100:
            result = google_custom_search(query, start_index)
            
            if not result or 'items' not in result:
                break
            
            page_count += 1
            new_in_batch = 0
            for item in result['items']:
                url = item.get('link', '')
                slug = extract_slug_from_url(url)
                if slug and slug not in all_slugs:
                    all_slugs.add(slug)
                    new_in_batch += 1
            
            if new_in_batch > 0 and page_count == 1:  # Only log first page with results
                print(f"  '{prefix}': +{new_in_batch} new (total: {len(all_slugs)})")
            
            if 'queries' not in result or 'nextPage' not in result['queries']:
                break
            
            start_index = result['queries']['nextPage'][0]['startIndex']
            time.sleep(0.5)  # Be nice to the API
    
    print(f"\n✅ Google Exhaustive Search Complete: {len(all_slugs)} unique slugs")
    return all_slugs

def discover_via_brute_force():
    """Brute force common company name patterns"""
    print("\n" + "="*60)
    print("METHOD 2: BRUTE FORCE VALIDATION")
    print("="*60)
    
    all_slugs = set()
    
    # Common company name patterns and words to test
    words = [
        'ai', 'app', 'tech', 'labs', 'data', 'cloud', 'soft', 'ware', 'systems',
        'solutions', 'group', 'digital', 'ventures', 'partners', 'capital',
        'health', 'care', 'medical', 'bio', 'pharma', 'finance', 'pay', 'bank',
        'crypto', 'web', 'net', 'media', 'games', 'studio', 'works', 'labs',
        'inc', 'corp', 'company', 'co', 'io', 'hq', 'base', 'hub', 'space'
    ]
    
    # Generate combinations
    slugs_to_test = set()
    
    # Single words
    slugs_to_test.update(words)
    
    # Two-word combinations (limited to avoid explosion)
    for w1, w2 in itertools.combinations(words[:20], 2):
        slugs_to_test.add(f"{w1}{w2}")
        slugs_to_test.add(f"{w1}-{w2}")
    
    print(f"Testing {len(slugs_to_test)} potential slug patterns...")
    
    tested = 0
    found = 0
    
    for slug in slugs_to_test:
        tested += 1
        
        if tested % 100 == 0:
            print(f"  Tested {tested}/{len(slugs_to_test)}, Found: {found}")
        
        if validate_slug(slug):
            all_slugs.add(slug)
            found += 1
            print(f"  ✓ Found: {slug}")
        
        time.sleep(0.1)  # Be nice to the server
    
    print(f"\n✅ Brute Force Complete: {found} valid slugs found")
    return all_slugs

def discover_via_specific_searches():
    """Targeted search method - currently disabled for validation testing"""
    print("\n" + "="*60)
    print("METHOD 3: TARGETED SEARCH")
    print("="*60)
    print("Skipped - allows validation that Methods 1 & 2 find everything organically")
    print("✅ Complete: 0 slugs (method intentionally disabled)")
    
    return set()

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
    print("="*60)
    print("ASHBY AGGRESSIVE DISCOVERY TOOL")
    print("3-Method Exhaustive Search (No DataForSEO)")
    print("="*60)
    
    existing_slugs = get_existing_slugs()
    print(f"\nExisting companies in database: {len(existing_slugs)}")
    
    # Run all discovery methods
    all_discovered = set()
    
    # Method 1: Exhaustive Google Search (ALL 2-letter combos)
    all_discovered.update(discover_via_google_exhaustive())
    
    # Method 2: Brute Force
    all_discovered.update(discover_via_brute_force())
    
    # Method 3: Targeted Search (empty for validation)
    all_discovered.update(discover_via_specific_searches())
    
    print(f"\n" + "="*60)
    print(f"COMBINED DISCOVERY RESULTS")
    print("="*60)
    print(f"Total unique slugs discovered: {len(all_discovered)}")
    print(f"Existing slugs in database: {len(existing_slugs)}")
    
    new_slugs = all_discovered - set(existing_slugs.keys())
    print(f"New slugs to add: {len(new_slugs)}")
    
    if new_slugs:
        print(f"\n🆕 NEW COMPANIES DISCOVERED ({len(new_slugs)}):")
        for slug in sorted(list(new_slugs)[:50]):  # Show first 50
            print(f"  - {slug}")
        if len(new_slugs) > 50:
            print(f"  ... and {len(new_slugs) - 50} more")
    
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
            
            existing_slugs[slug] = {
                'company_name': company_name,
                'first_seen_date': today,
                'last_checked_date': today,
                'job_count': str(job_count)
            }
            
            time.sleep(0.3)
    
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
    
    print(f"\n🧪 VALIDATION TEST:")
    print(f"  Check if 'cyvl' is in the list: {'YES ✓' if 'cyvl' in existing_slugs else 'NO ✗'}")
    print(f"  Check if 'hyphametrics' is in the list: {'YES ✓' if 'hyphametrics' in existing_slugs else 'NO ✗'}")

if __name__ == "__main__":
    main()
