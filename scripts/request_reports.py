"""
Script to request new release reports from Mercado Pago.
"""
import os
import sys
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()
MP_TOKEN = os.getenv("MP_ACCESS_TOKEN")
BASE_URL = "https://api.mercadopago.com/v1"

def request_fresh_report():
    print("🚀 Requesting fresh Release Report for P62...")
    
    # We want from the last report date to today
    # But usually, requesting the last 7 days is safe
    end_date = datetime.now()
    begin_date = end_date - timedelta(days=15)
    
    url = f"{BASE_URL}/account/release_report"
    headers = {
        "Authorization": f"Bearer {MP_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Format dates for API
    begin_str = begin_date.strftime("%Y-%m-%dT00:00:00Z")
    end_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
    
    payload = {
        "begin_date": begin_str,
        "end_date": end_str
    }
    
    print(f"📅 Window: {begin_str} to {end_str}")
    
    resp = requests.post(url, headers=headers, json=payload)
    
    if resp.status_code in [200, 201]:
        print("✅ Fresh report requested successfully!")
        print("💡 Note: reports take a few minutes to generate. Wait 5 mins then run ingest_release_reports.py.")
    else:
        print(f"❌ Failed to request report: {resp.status_code}")
        print(resp.text)

if __name__ == "__main__":
    request_fresh_report()
