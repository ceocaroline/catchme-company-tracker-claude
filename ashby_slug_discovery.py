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
BATCH_SIZE = 100  # Process prefixes in batches to avoid hanging

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
        print(f"No existing CSV found. Creating new file: {CSV_FILE}", flush=True)
    return existing_slugs

def extract_slug_from_url(url):
    """Extract company slug from Ashby URL"""
    parsed = urlparse(url)
    if BASE_URL in parsed.netloc:
        path_parts = parsed.path.strip('/').split('/')
        if path_parts and path_parts[0]:
            slug = path_parts[0].lower().strip()
            slug = slug.split('?')[0].split('#')[0]
            return slug if slug else None
    return None

def google_custom_search(query, start_index=1):
    """Query Google Custom Search API with timeout"""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_CSE_ID,
        'q': query,
        'start': start_index,
        'num': 10
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  ⚠ API error: {str(e)[:100]}", flush=True)
        return None

def search_with_prefix(prefix, all_slugs):
    """Search Google with a single prefix and return new slugs found"""
    query = f"site:{BASE_URL} {prefix}" if prefix else f"site:{BASE_URL}"
    new_count = 0
    
    start_index = 1
    while start_index <= 100:  # Max 100 results per query
        result = google_custom_search(query, start_index)
        
        if not result or 'items' not in result:
            break
        
        for item in result['items']:
            url = item.get('link', '')
            slug = extract_slug_from_url(url)
            if slug and slug not in all_slugs:
                all_slugs.add(slug)
                new_count += 1
        
        if 'queries' not in result or 'nextPage' not in result['queries']:
            break
        
        start_index = result['queries']['nextPage'][0]['startIndex']
        time.sleep(0.5)
    
    return new_count

def discover_via_google_chunked():
    """Exhaustive Google search in manageable chunks"""
    all_slugs = set()
    
    print("\n" + "="*60, flush=True)
    print("METHOD 1: EXHAUSTIVE GOOGLE SEARCH (CHUNKED)", flush=True)
    print("="*60, flush=True)
    
    # Generate all prefixes
    print("Generating search prefixes...", flush=True)
    prefixes = ['']
    prefixes.extend([chr(i) for i in range(ord('a'), ord('z')+1)])
    prefixes.extend([str(i) for i in range(10)])
    
    # All two-letter combinations
    for c1 in 'abcdefghijklmnopqrstuvwxyz':
        for c2 in 'abcdefghijklmnopqrstuvwxyz':
            prefixes.append(c1 + c2)
    
    # Letter + digit
    for c1 in 'abcdefghijklmnopqrstuvwxyz':
        for c2 in '0123456789':
            prefixes.append(c1 + c2)
    
    # Digit + letter
    for c1 in '0123456789':
        for c2 in 'abcdefghijklmnopqrstuvwxyz':
            prefixes.append(c1 + c2)
    
    print(f"Generated {len(prefixes)} prefixes", flush=True)
    print(f"Processing in batches of {BATCH_SIZE}...\n", flush=True)
    
    # Process in batches
    total_batches = (len(prefixes) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(prefixes))
        batch = prefixes[start_idx:end_idx]
        
        print(f"BATCH {batch_num + 1}/{total_batches}", flush=True)
        print(f"  Processing prefixes {start_idx} to {end_idx-1}", flush=True)
        print(f"  Current total slugs: {len(all_slugs)}", flush=True)
        
        batch_start_time = time.time()
        
        for i, prefix in enumerate(batch):
            new_found = search_with_prefix(prefix, all_slugs)
            
            # Print every 10th in the batch
            if i % 10 == 0 or new_found > 0:
                display_prefix = prefix if prefix else '(empty)'
                print(f"    [{start_idx + i}] '{display_prefix}': +{new_found} (total: {len(all_slugs)})", flush=True)
        
        batch_time = time.time() - batch_start_time
        print(f"  Batch completed in {batch_time:.1f}s", flush=True)
        print(f"  Total unique slugs so far: {len(all_slugs)}\n", flush=True)
    
    print(f"✅ Google Search Complete: {len(all_slugs)} unique slugs", flush=True)
    return all_slugs

def validate_slug(slug):
    """Check if a slug exists"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.head(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'}, allow_redirects=True)
        return response.status_code == 200
    except:
        return False

def check_job_postings_via_api(slug):
    """Get job count via Ashby's API"""
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
    """Fallback: Check job count by scraping"""
    try:
        url = f"https://{BASE_URL}/{slug}"
        response = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        if response.status_code != 200:
            return 0, f"Page returned {response.status_code}"
        
        content = response.text
        no_jobs_indicators = [
            'no open positions', 'no positions available', 'no current openings',
            'no openings at this time', 'not currently hiring', 'no active job postings'
        ]
        
        content_lower = content.lower()
        for indicator in no_jobs_indicators:
            if indicator in content_lower:
                return 0, "Page shows no openings"
        
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
        
        return job_count, "Page scraping"
    except Exception as e:
        return 0, f"Error: {str(e)}"

def get_job_count(slug):
    """Get job count using multiple methods"""
    job_count, status = check_job_postings_via_api(slug)
    if job_count is not None:
        return job_count, status
    job_count, status = check_job_postings_via_page(slug)
    return job_count, status

def get_company_name_from_slug(slug):
    """Fetch company name from page"""
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
            return slug.replace('-', ' ').title()
        else:
            return slug.replace('-', ' ').title()
    except:
        return slug.replace('-', ' ').title()

def save_to_csv(slugs_dict):
    """Save to CSV"""
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
    
    print(f"\n✅ Saved {len(slugs_dict)} companies to {CSV_FILE}", flush=True)

def save_filtered_lists(slugs_dict):
    """Save filtered lists by job count"""
    zero_jobs = {s: d for s, d in slugs_dict.items() if d.get('job_count') == '0'}
    few_jobs = {s: d for s, d in slugs_dict.items() 
                if d.get('job_count', '0').isdigit() and 0 < int(d.get('job_count', '0')) < FEW_JOBS_THRESHOLD}
    
    if zero_jobs:
        with open(ZERO_JOBS_FILE, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['slug', 'company_name', 'first_seen_date', 'job_count', 'url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for slug, data in sorted(zero_jobs.items(), key=lambda x: x[1]['first_seen_date'], reverse=True):
                writer.writerow({
                    'slug': slug, 'company_name': data['company_name'],
                    'first_seen_date': data['first_seen_date'], 'job_count': data['job_count'],
                    'url': f"https://{BASE_URL}/{slug}"
                })
        print(f"🆕 Saved {len(zero_jobs)} companies with 0 jobs to {ZERO_JOBS_FILE}", flush=True)
    
    if few_jobs:
        with open(FEW_JOBS_FILE, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['slug', 'company_name', 'first_seen_date', 'job_count', 'url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for slug, data in sorted(few_jobs.items(), key=lambda x: x[1]['first_seen_date'], reverse=True):
                writer.writerow({
                    'slug': slug, 'company_name': data['company_name'],
                    'first_seen_date': data['first_seen_date'], 'job_count': data['job_count'],
                    'url': f"https://{BASE_URL}/{slug}"
                })
        print(f"🔥 Saved {len(few_jobs)} companies with 1-{FEW_JOBS_THRESHOLD-1} jobs to {FEW_JOBS_FILE}", flush=True)

def main():
    print("="*60, flush=True)
    print("ASHBY COMPANY DISCOVERY - CHUNKED VERSION", flush=True)
    print("="*60, flush=True)
    print(f"Started: {datetime.now()}", flush=True)
    
    existing_slugs = get_existing_slugs()
    print(f"Loaded {len(existing_slugs)} existing companies\n", flush=True)
    
    # Discovery
    all_discovered = discover_via_google_chunked()
    
    print(f"\n" + "="*60, flush=True)
    print("DISCOVERY COMPLETE", flush=True)
    print("="*60, flush=True)
    print(f"Total unique slugs: {len(all_discovered)}", flush=True)
    print(f"Existing in database: {len(existing_slugs)}", flush=True)
    
    new_slugs = all_discovered - set(existing_slugs.keys())
    print(f"New companies found: {len(new_slugs)}\n", flush=True)
    
    if new_slugs:
        print(f"NEW COMPANIES ({len(new_slugs)}):", flush=True)
        for slug in sorted(list(new_slugs)[:20]):
            print(f"  - {slug}", flush=True)
        if len(new_slugs) > 20:
            print(f"  ... and {len(new_slugs) - 20} more", flush=True)
    
    # Process new companies
    today = datetime.now().strftime('%Y-%m-%d')
    
    if new_slugs:
        print(f"\n" + "="*60, flush=True)
        print(f"PROCESSING {len(new_slugs)} NEW COMPANIES", flush=True)
        print("="*60, flush=True)
        
        for i, slug in enumerate(new_slugs, 1):
            if i % 10 == 1:  # Progress every 10
                print(f"\nProcessing {i}-{min(i+9, len(new_slugs))} of {len(new_slugs)}...", flush=True)
            
            company_name = get_company_name_from_slug(slug)
            job_count, status = get_job_count(slug)
            
            print(f"  {slug}: {job_count} jobs", flush=True)
            
            existing_slugs[slug] = {
                'company_name': company_name,
                'first_seen_date': today,
                'last_checked_date': today,
                'job_count': str(job_count)
            }
            time.sleep(0.3)
    
    # Update dates
    for slug in existing_slugs:
        if slug not in new_slugs:
            existing_slugs[slug]['last_checked_date'] = today
    
    # Save
    save_to_csv(existing_slugs)
    save_filtered_lists(existing_slugs)
    
    # Stats
    zero = sum(1 for d in existing_slugs.values() if d.get('job_count') == '0')
    few = sum(1 for d in existing_slugs.values() 
              if d.get('job_count', '0').isdigit() and 0 < int(d.get('job_count', '0')) < FEW_JOBS_THRESHOLD)
    many = sum(1 for d in existing_slugs.values() 
               if d.get('job_count', '0').isdigit() and int(d.get('job_count', '0')) >= FEW_JOBS_THRESHOLD)
    
    print(f"\n" + "="*60, flush=True)
    print("FINAL RESULTS", flush=True)
    print("="*60, flush=True)
    print(f"Total companies: {len(existing_slugs)}", flush=True)
    print(f"New today: {len(new_slugs)}", flush=True)
    print(f"\nJob counts:", flush=True)
    print(f"  0 jobs: {zero}", flush=True)
    print(f"  1-{FEW_JOBS_THRESHOLD-1} jobs: {few}", flush=True)
    print(f"  {FEW_JOBS_THRESHOLD}+ jobs: {many}", flush=True)
    print(f"\n🧪 VALIDATION:", flush=True)
    print(f"  cyvl found: {'YES ✓' if 'cyvl' in existing_slugs else 'NO ✗'}", flush=True)
    print(f"  hyphametrics found: {'YES ✓' if 'hyphametrics' in existing_slugs else 'NO ✗'}", flush=True)

if __name__ == "__main__":
    main()
