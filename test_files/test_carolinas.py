#!/usr/bin/env python3
"""Test script for Carolinas scraper"""

import os
import sys
sys.path.insert(0, '/Users/maddisenmohnsen/Desktop/Scrapers/wso_sheets_scraper')

from dotenv import load_dotenv
load_dotenv()

from scraper_carolinas import WSORecordsCarolinasScraper

def main():
    wso_name = "Carolinas"
    sheet_url = "https://docs.google.com/spreadsheets/d/1rKFzpkLCT-FE2SzM0qpUOoZ788YHl7dg/view?gid=1785893123#gid=1785893123"
    
    print(f"\n🧪 Testing Carolinas Scraper")
    print(f"WSO: {wso_name}")
    print(f"Sheet: {sheet_url}\n")
    
    scraper = WSORecordsCarolinasScraper(wso_name, sheet_url)
    
    print("Scraping sheet...")
    records = scraper.scrape_sheet()
    
    print(f"\n✓ Successfully fetched {len(records)} records\n")
    
    # Show first 10
    print("Sample records (first 10):")
    print("=" * 80)
    for i, record in enumerate(records[:10], 1):
        print(f"{i}. {record['age_category']} | {record['gender']} | {record['weight_class']}")
        print(f"   Snatch: {record.get('snatch_record')}, C&J: {record.get('cj_record')}, Total: {record.get('total_record')}")
    
    if len(records) > 10:
        print(f"\n... and {len(records) - 10} more records")
    
    # Group by age category
    from collections import defaultdict
    by_age = defaultdict(int)
    for r in records:
        by_age[r['age_category']] += 1
    
    print("\nBreakdown by age category:")
    for age_cat in sorted(by_age.keys()):
        print(f"  {age_cat}: {by_age[age_cat]} records")
    
    # Save to JSON
    import json
    with open('test_carolinas_data.json', 'w') as f:
        json.dump(records, f, indent=2)
    print(f"\n✓ Saved to test_carolinas_data.json")

if __name__ == "__main__":
    main()
