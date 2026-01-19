"""
Ingestion Job: European Football Data
-------------------------------------
Extracts match data for top 5 European leagues from external JSON feeds.
Persists raw data to the Bronze layer in Azure Data Lake Storage (ADLS).

Layer: Bronze
Source: External API
Sink: ADLS /mnt/bronze/
"""

import requests
import json
import logging

# Configure logging
logger = logging.getLogger("IngestJob")
logger.setLevel(logging.INFO)

# Configuration: Mapping League Names to Source URLs
LEAGUE_CONFIG = {
    "EPL": "https://fixturedownload.com/feed/json/epl-2025",
    "Bundesliga": "https://fixturedownload.com/feed/json/bundesliga-2025",
    "Serie_A": "https://fixturedownload.com/feed/json/serie-a-2025",
    "Ligue_1": "https://fixturedownload.com/feed/json/ligue-1-2025",
    "La_Liga": "https://fixturedownload.com/feed/json/la-liga-2025"
}

BASE_STORAGE_PATH = "/mnt/bronze"

def fetch_and_save_league(league_name: str, url: str) -> bool:
    """
    Fetches data from a specific URL and saves it to the Bronze layer.
    
    Args:
        league_name (str): The identifier for the league (e.g., 'EPL').
        url (str): The API endpoint.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        logger.info(f"Starting ingestion for {league_name}...")
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Construct output path
            output_path = f"{BASE_STORAGE_PATH}/{league_name}_raw.json"
            
            # Write to DBFS (Data Lake)
            dbutils.fs.put(output_path, json.dumps(data), overwrite=True)
            
            logger.info(f"Successfully ingested {league_name}. Records: {len(data)}")
            return True
        else:
            logger.error(f"Failed to fetch {league_name}. HTTP Status: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Exception occurred while processing {league_name}: {str(e)}")
        return False

# Main Execution Loop
if __name__ == "__main__":
    success_count = 0
    total_leagues = len(LEAGUE_CONFIG)

    for league, source_url in LEAGUE_CONFIG.items():
        if fetch_and_save_league(league, source_url):
            success_count += 1
            
    print(f"Ingestion Complete. Success: {success_count}/{total_leagues}")