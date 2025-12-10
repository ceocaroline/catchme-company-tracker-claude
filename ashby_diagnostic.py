import os
import requests
import time
from datetime import datetime

# Configuration
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID')
BASE_URL = "jobs.ashbyhq.com"

print("="*60, flush=True)
print("DIAGNOSTIC TEST - Finding the Problem", flush=True)
print("="*60, flush=True)
print(f"Current time: {datetime.now()}", flush=True)

# Test 1: Check API credentials
print("\n🔍 TEST 1: Checking API Credentials...", flush=True)
if GOOGLE_API_KEY:
    print(f"  ✓ Google API Key found (length: {len(GOOGLE_API_KEY)})", flush=True)
else:
    print("  ✗ Google API Key MISSING!", flush=True)
    
if GOOGLE_CSE_ID:
    print(f"  ✓ Google CSE ID found (length: {len(GOOGLE_CSE_ID)})", flush=True)
else:
    print("  ✗ Google CSE ID MISSING!", flush=True)

# Test 2: Make a simple Google API call
print("\n🔍 TEST 2: Testing Google Custom Search API...", flush=True)
print("  Making API call to Google...", flush=True)

url = "https://www.googleapis.com/customsearch/v1"
params = {
    'key': GOOGLE_API_KEY,
    'cx': GOOGLE_CSE_ID,
    'q': f'site:{BASE_URL}',
    'start': 1,
    'num': 10
}

try:
    print("  Sending request...", flush=True)
    start_time = time.time()
    response = requests.get(url, params=params, timeout=30)
    elapsed = time.time() - start_time
    
    print(f"  Response received in {elapsed:.2f} seconds", flush=True)
    print(f"  Status code: {response.status_code}", flush=True)
    
    if response.status_code == 200:
        print("  ✓ API call successful!", flush=True)
        data = response.json()
        
        if 'items' in data:
            print(f"  ✓ Found {len(data['items'])} results", flush=True)
            print(f"  First result: {data['items'][0].get('link', 'N/A')}", flush=True)
        else:
            print("  ⚠ No items in response", flush=True)
            print(f"  Response keys: {list(data.keys())}", flush=True)
    else:
        print(f"  ✗ API call failed with status {response.status_code}", flush=True)
        print(f"  Response: {response.text[:500]}", flush=True)
        
except requests.exceptions.Timeout:
    print("  ✗ Request TIMED OUT after 30 seconds!", flush=True)
except Exception as e:
    print(f"  ✗ Error: {type(e).__name__}: {str(e)}", flush=True)

# Test 3: Check API quota
print("\n🔍 TEST 3: Testing API Quota Status...", flush=True)
try:
    # Make a second call to see if we're rate limited
    print("  Making second API call...", flush=True)
    response2 = requests.get(url, params=params, timeout=30)
    print(f"  Status code: {response2.status_code}", flush=True)
    
    if response2.status_code == 429:
        print("  ✗ RATE LIMITED! Too many requests", flush=True)
    elif response2.status_code == 403:
        print("  ✗ FORBIDDEN! Check API key permissions", flush=True)
    elif response2.status_code == 200:
        print("  ✓ Second call successful - no rate limiting", flush=True)
    else:
        print(f"  ⚠ Unexpected status: {response2.status_code}", flush=True)
        
except Exception as e:
    print(f"  ✗ Error on second call: {type(e).__name__}: {str(e)}", flush=True)

# Test 4: Test with different query
print("\n🔍 TEST 4: Testing with prefix search...", flush=True)
params_prefix = params.copy()
params_prefix['q'] = f'site:{BASE_URL} a'

try:
    print("  Searching for 'site:jobs.ashbyhq.com a'...", flush=True)
    response3 = requests.get(url, params=params_prefix, timeout=30)
    print(f"  Status code: {response3.status_code}", flush=True)
    
    if response3.status_code == 200:
        data3 = response3.json()
        if 'items' in data3:
            print(f"  ✓ Found {len(data3['items'])} results with prefix 'a'", flush=True)
        else:
            print("  ⚠ No results with prefix 'a'", flush=True)
    else:
        print(f"  ✗ Failed with status {response3.status_code}", flush=True)
        
except Exception as e:
    print(f"  ✗ Error: {type(e).__name__}: {str(e)}", flush=True)

# Test 5: Test rapid calls (simulate actual script behavior)
print("\n🔍 TEST 5: Testing rapid sequential calls...", flush=True)
print("  Making 5 rapid calls with 0.5s delay...", flush=True)

for i in range(5):
    try:
        test_params = params.copy()
        test_params['q'] = f'site:{BASE_URL} {chr(97+i)}'  # a, b, c, d, e
        
        print(f"  Call {i+1}: Searching '{test_params['q']}'...", flush=True)
        start = time.time()
        resp = requests.get(url, params=test_params, timeout=30)
        elapsed = time.time() - start
        
        print(f"    Status: {resp.status_code}, Time: {elapsed:.2f}s", flush=True)
        
        if resp.status_code != 200:
            print(f"    ✗ Failed! Response: {resp.text[:200]}", flush=True)
            break
        else:
            data = resp.json()
            items = len(data.get('items', []))
            print(f"    ✓ Success! Found {items} items", flush=True)
        
        time.sleep(0.5)
        
    except Exception as e:
        print(f"    ✗ Error: {type(e).__name__}: {str(e)}", flush=True)
        break

print("\n" + "="*60, flush=True)
print("DIAGNOSTIC COMPLETE", flush=True)
print("="*60, flush=True)
print("\nIf all tests passed, the issue is likely:", flush=True)
print("  1. Script hanging during loop iteration", flush=True)
print("  2. Output buffering issue (even with flush=True)", flush=True)
print("  3. Memory issue with large loop", flush=True)
print("\nIf tests failed, the issue is:", flush=True)
print("  1. API credentials invalid", flush=True)
print("  2. Rate limiting / quota exceeded", flush=True)
print("  3. Network connectivity", flush=True)
