#!/usr/bin/env python3
"""
WSO Records Scraper - TN-KY Format

For Tennessee-Kentucky WSO which uses a horizontal layout where:
- Weight classes are columns
- Each section has age_category + gender header
- Three rows per lift type (value, name, date)

KNOWN ISSUES (As of creation - 2025):
=====================================================
This scraper is INCOMPLETE and needs debugging before production use.
TN-KY updates rarely, so manual updates may be more practical.

Current Status:
- ✅ Parses Junior and Senior categories (32 records)
- ❌ Missing Youth categories (U13 "13 & Under", U17 "14-17 YO")
- ❌ Missing Masters categories (all age groups: 35-39, 40-44, 45-49, etc.)

Problems to Fix:
1. Section header parsing is inconsistent:
   - Row 1: "TN-KY WSO RECORDS YOUTH: MEN " with age in NEXT column "13 & Under 44 KG"
   - Row 16: "YOUTH: WOMEN " with age in column 3 "13 & Under"
   - Row 49: "JUNIORS: MEN" (plural, not "JUNIOR")
   - The age info appears in different positions depending on the section
   
2. Age category mapping issues:
   - "JUNIORS: MEN 15-20 years old" should map to "Junior"
   - "14-17 YO" should map to "U17" (not U15)
   - Multiple Masters sections (35-39, 40-44, 45-49, 50-54, 55-59, 60-64, etc.)
   
3. Weight class extraction from merged headers:
   - First row has: "TN-KY WSO RECORDS YOUTH: MEN " with "13 & Under 44 KG" in next column
   - Weight classes are embedded in the age descriptor column
   
4. The CSV export format is very messy due to merged cells in the original sheet

Debugging Tips:
- Check rows with: curl <csv_url> | grep -n "YOUTH\|SENIOR\|MASTER\|JUNIOR"
- Section headers appear around rows: 1, 16, 27, 38, 49, 60, 71, 82, 94, 105, 116...
- Each section has 11 rows (1 header + 1 weight row + 9 data rows)
- May need to look 2-3 columns ahead to find age group info

Recommendation: 
Since TN-KY updates infrequently, consider manual updates or wait until 
they have significant changes before investing time in full automation.
"""

import os
import sys
import json
import argparse
import re
from typing import List, Dict, Any
from datetime import datetime
from collections import defaultdict

import requests
from supabase import create_client, Client


class WSORecordsTNKYScraper:
    """Scraper for TN-KY WSO weightlifting records with horizontal layout."""
    
    def __init__(self, wso_name: str, sheet_url: str):
        """Initialize the scraper with WSO name and sheet URL."""
        self.wso_name = wso_name
        self.sheet_url = sheet_url
        self.changes = {"inserted": [], "updated": []}
        
        # Initialize clients
        self.supabase_client = None
        self.discord_webhook_url = None
        
    def setup_supabase_client(self):
        """Set up Supabase client."""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set")
        
        self.supabase_client: Client = create_client(supabase_url, supabase_key)
        print("✓ Supabase client initialized")
    
    def setup_discord(self):
        """Set up Discord webhook URL."""
        self.discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if not self.discord_webhook_url:
            raise ValueError("DISCORD_WEBHOOK_URL environment variable not set")
        print("✓ Discord webhook configured")
    
    def _normalize_age_category(self, section_header: str) -> tuple:
        """
        Parse section header to extract age category and gender.
        
        Examples:
        - "YOUTH: MEN 13 & Under" -> ("U13", "Men")
        - "YOUTH: WOMEN 14-17 YO" -> ("U17", "Women") 
        - "SENIORS: MEN 15 years old <" -> ("Senior", "Men")
        - "MASTERS: MEN 35-39 years old" -> ("Masters 35", "Men")
        """
        header = section_header.strip().upper()
        
        # Extract gender
        if "WOMEN" in header:
            gender = "Women"
        elif "MEN" in header:
            gender = "Men"
        else:
            return None, None
        
        # Extract age category
        # Youth 13 & Under -> U13
        if "13" in header and "UNDER" in header:
            return "U13", gender
        
        # Youth 14-17 -> U17
        if "14-17" in header or "14 - 17" in header:
            return "U17", gender
        
        # Seniors (15 years old <)
        if "SENIOR" in header:
            return "Senior", gender
        
        # Junior
        if "JUNIOR" in header:
            return "Junior", gender
        
        # Masters (35-39, 40-44, etc.)
        if "MASTER" in header:
            # Extract age range
            match = re.search(r'(\d+)\s*-\s*(\d+)', header)
            if match:
                lower_age = match.group(1)
                return f"Masters {lower_age}", gender
        
        return None, None
    
    def scrape_sheet(self) -> List[Dict[str, Any]]:
        """
        Scrape data from TN-KY Google Sheet in horizontal format.
        
        Returns:
            List of records with structure matching DB schema
        """
        # Extract sheet ID from URL
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', self.sheet_url)
        if not match:
            raise ValueError("Invalid Google Sheets URL")
        
        sheet_id = match.group(1)
        
        # Extract gid if present
        gid_match = re.search(r'[#&]gid=(\d+)', self.sheet_url)
        if gid_match:
            sheet_name = gid_match.group(1)
        else:
            sheet_name = "0"
        
        # Fetch CSV data
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={sheet_name}"
        response = requests.get(csv_url)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch sheet: {response.status_code}")
        
        # Parse CSV into rows
        import csv
        import io
        csv_reader = csv.reader(io.StringIO(response.text))
        rows = list(csv_reader)
        
        # Process rows
        records = []
        current_age_category = None
        current_gender = None
        
        i = 0
        while i < len(rows):
            row = rows[i]
            
            # Check if this is a section header (contains age group info)
            if row and row[0]:
                first_cell = row[0].strip()
                
                # Try to parse as section header
                age_cat, gender = self._normalize_age_category(first_cell)
                if age_cat and gender:
                    current_age_category = age_cat
                    current_gender = gender
                    
                    # Next row should have weight classes
                    if i + 1 < len(rows):
                        i += 1
                        weight_row = rows[i]
                        weight_classes = self._parse_weight_classes(weight_row)
                        
                        # Process the data rows (SNATCH, Name, Date, C&J, Name, Date, TOTAL, Name, Date)
                        if i + 9 < len(rows):
                            snatch_data = self._parse_lift_rows(rows[i+1:i+4], weight_classes)
                            cj_data = self._parse_lift_rows(rows[i+4:i+7], weight_classes)
                            total_data = self._parse_lift_rows(rows[i+7:i+10], weight_classes)
                            
                            # Combine into records
                            for weight_class in weight_classes:
                                if weight_class:
                                    record = {
                                        'wso': self.wso_name,
                                        'age_category': current_age_category,
                                        'gender': current_gender,
                                        'weight_class': weight_class,
                                        'snatch_record': snatch_data.get(weight_class),
                                        'cj_record': cj_data.get(weight_class),
                                        'total_record': total_data.get(weight_class)
                                    }
                                    records.append(record)
                            
                            i += 9  # Skip the 9 data rows we just processed
            
            i += 1
        
        return records
    
    def _parse_weight_classes(self, row: List[str]) -> List[str]:
        """Extract weight classes from header row."""
        weight_classes = []
        for cell in row[1:]:  # Skip first column
            cell = cell.strip()
            if cell and 'KG' in cell.upper():
                # Extract just the weight class (e.g., "40 KG" -> "40")
                weight = re.sub(r'[^\d+]', '', cell)
                if weight:
                    weight_classes.append(weight)
                else:
                    weight_classes.append(None)
            else:
                weight_classes.append(None)
        return weight_classes
    
    def _parse_lift_rows(self, three_rows: List[List[str]], weight_classes: List[str]) -> Dict[str, int]:
        """
        Parse three rows (value, name, date) for a lift type.
        Returns dict of {weight_class: value}
        """
        if len(three_rows) < 3:
            return {}
        
        value_row = three_rows[0]
        result = {}
        
        for idx, weight_class in enumerate(weight_classes):
            if weight_class and idx + 1 < len(value_row):
                value_str = value_row[idx + 1].strip()
                if value_str:
                    try:
                        value = int(float(value_str))
                        result[weight_class] = value
                    except ValueError:
                        pass
        
        return result
    
    def upsert_records(self, records: List[Dict[str, Any]]) -> None:
        """Upsert records to Supabase (same as other scrapers)."""
        for record in records:
            # Query for existing record
            existing = self.supabase_client.table("wso_records").select("*").match({
                "wso": record["wso"],
                "age_category": record["age_category"],
                "gender": record["gender"],
                "weight_class": record["weight_class"]
            }).execute()
            
            if existing.data and len(existing.data) > 0:
                # Record exists - check if update needed
                existing_record = existing.data[0]
                record_id = existing_record["id"]
                
                changes = {}
                if existing_record.get("snatch_record") != record.get("snatch_record"):
                    changes["snatch_record"] = {
                        "old": existing_record.get("snatch_record"),
                        "new": record.get("snatch_record")
                    }
                if existing_record.get("cj_record") != record.get("cj_record"):
                    changes["cj_record"] = {
                        "old": existing_record.get("cj_record"),
                        "new": record.get("cj_record")
                    }
                if existing_record.get("total_record") != record.get("total_record"):
                    changes["total_record"] = {
                        "old": existing_record.get("total_record"),
                        "new": record.get("total_record")
                    }
                
                if changes:
                    self.supabase_client.table("wso_records").update(record).eq("id", record_id).execute()
                    self.changes["updated"].append({
                        "wso": record["wso"],
                        "age_category": record["age_category"],
                        "gender": record["gender"],
                        "weight_class": record["weight_class"],
                        "changes": changes
                    })
                    print(f"  ✓ Updated: {record['age_category']} {record['gender']} {record['weight_class']}")
            else:
                # Insert new record
                self.supabase_client.table("wso_records").insert(record).execute()
                self.changes["inserted"].append({
                    "wso": record["wso"],
                    "age_category": record["age_category"],
                    "gender": record["gender"],
                    "weight_class": record["weight_class"],
                    "snatch_record": record.get("snatch_record"),
                    "cj_record": record.get("cj_record"),
                    "total_record": record.get("total_record")
                })
                print(f"  ✓ Inserted: {record['age_category']} {record['gender']} {record['weight_class']}")
    
    def send_discord_notification(self) -> None:
        """Send Discord notification (same as other scrapers)."""
        total_inserted = len(self.changes["inserted"])
        total_updated = len(self.changes["updated"])
        
        if total_inserted == 0 and total_updated == 0:
            embed = {
                "title": f"📊 {self.wso_name} WSO Records - No Changes",
                "description": "Scraper ran successfully. No new records or updates.",
                "color": 3447003,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "WSO Records Scraper"}
            }
            payload = {"embeds": [embed]}
        else:
            description_parts = [
                f"**Summary:**",
                f"• {total_inserted} new record(s) inserted",
                f"• {total_updated} record(s) updated"
            ]
            
            fields = []
            
            if total_inserted > 0:
                inserted_text = []
                for record in self.changes["inserted"][:10]:
                    lifts = []
                    if record.get("snatch_record"):
                        lifts.append(f"Snatch: {record['snatch_record']}kg")
                    if record.get("cj_record"):
                        lifts.append(f"C&J: {record['cj_record']}kg")
                    if record.get("total_record"):
                        lifts.append(f"Total: {record['total_record']}kg")
                    
                    lifts_str = ", ".join(lifts) if lifts else "No records"
                    inserted_text.append(
                        f"• **{record['age_category']}** | {record['gender']} | {record['weight_class']}\n  {lifts_str}"
                    )
                
                if total_inserted > 10:
                    inserted_text.append(f"_...and {total_inserted - 10} more_")
                
                fields.append({
                    "name": "🆕 New Records",
                    "value": "\n".join(inserted_text),
                    "inline": False
                })
            
            if total_updated > 0:
                updated_text = []
                for record in self.changes["updated"][:10]:
                    changes_str = []
                    for field, change in record["changes"].items():
                        field_name = field.replace("_record", "").replace("cj", "C&J").title()
                        old = f"{change['old']}kg" if change['old'] else "None"
                        new = f"{change['new']}kg" if change['new'] else "None"
                        changes_str.append(f"{field_name}: {old} → {new}")
                    
                    updated_text.append(
                        f"• **{record['age_category']}** | {record['gender']} | {record['weight_class']}\n  {', '.join(changes_str)}"
                    )
                
                if total_updated > 10:
                    updated_text.append(f"_...and {total_updated - 10} more_")
                
                fields.append({
                    "name": "📝 Updated Records",
                    "value": "\n".join(updated_text),
                    "inline": False
                })
            
            embed = {
                "title": f"📊 {self.wso_name} WSO Records Update",
                "description": "\n".join(description_parts),
                "color": 3066993,
                "fields": fields,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "WSO Records Scraper"}
            }
            payload = {"embeds": [embed]}
        
        try:
            response = requests.post(self.discord_webhook_url, json=payload)
            response.raise_for_status()
            print("✓ Discord notification sent")
        except Exception as e:
            print(f"✗ Failed to send Discord notification: {e}")
    
    def run(self) -> None:
        """Main execution flow."""
        print(f"Starting scraper for {self.wso_name}")
        print(f"Sheet URL: {self.sheet_url}")
        
        self.setup_supabase_client()
        self.setup_discord()
        
        print("Scraping Google Sheet...")
        records = self.scrape_sheet()
        print(f"Found {len(records)} records")
        
        print("Upserting records to Supabase...")
        self.upsert_records(records)
        
        print("Sending Discord notification...")
        self.send_discord_notification()
        
        print("Done!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="WSO Records Scraper (TN-KY Format)")
    parser.add_argument("--wso", required=True, help="WSO name (should be 'Tennessee-Kentucky')")
    parser.add_argument("--sheet-url", required=True, help="Google Sheet URL")
    
    args = parser.parse_args()
    
    scraper = WSORecordsTNKYScraper(args.wso, args.sheet_url)
    scraper.run()


if __name__ == "__main__":
    main()
