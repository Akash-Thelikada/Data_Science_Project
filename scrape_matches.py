"""
Scrape match history from click-tt.ch for Swiss Table Tennis players.
Simple version: loop through ranking IDs and fetch match data.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re

# ============================================================
# STEP 1: Load players and get their ranking IDs
# ============================================================

print("Loading player data...")

# Load players with ELO_KLASSIERUNG between 11 and 21
female_df = pd.read_csv('data/elo-rankings_female_20251218.csv', sep=';', encoding='latin-1')
female_df = female_df[(female_df['ELO_KLASSIERUNG'] >= 11) & (female_df['ELO_KLASSIERUNG'] <= 21)]

male_df = pd.read_csv('data/elo-rankings_male_20251218.csv', sep=';', encoding='latin-1')
male_df = male_df[(male_df['ELO_KLASSIERUNG'] >= 11) & (male_df['ELO_KLASSIERUNG'] <= 21)]

all_players = pd.concat([female_df, male_df], ignore_index=True)
print(f"Found {len(all_players)} players with ELO_KLASSIERUNG 11-21")

# Get ranking IDs by searching each player's name once
print("\nCollecting ranking IDs...")
search_url = "https://www.click-tt.ch/cgi-bin/WebObjects/nuLigaTTCH.woa/wa/eloFilter"
ranking_ids = []

for i, player in all_players.iterrows():
    if (i + 1) % 100 == 0:
        print(f"  {i + 1}/{len(all_players)}...")
    
    try:
        response = requests.get(search_url, params={
            "federation": "STT",
            "rankingDate": "18.12.2025",
            "lastname": player['NACHNAME'],
            "firstname": player['VORNAME']
        }, timeout=10)
        soup = BeautifulSoup(response.text, 'lxml')
        
        for link in soup.find_all('a'):
            href = link.get('href', '')
            match = re.search(r'ranking=(\d+)', href)
            if match:
                ranking_ids.append(match.group(1))
                break
    except:
        pass
    
    time.sleep(0.2)

ranking_ids = list(set(ranking_ids))  # Remove duplicates
print(f"Found {len(ranking_ids)} unique ranking IDs")

# ============================================================
# STEP 2: Fetch matches for each ranking ID (simple loop)
# ============================================================

print("\nScraping matches...")

# Base URL - only the ranking ID changes
base_url = "https://www.click-tt.ch/cgi-bin/WebObjects/nuLigaTTCH.woa/wa/eloFilter?federation=STT&rankingDate=18.12.2025&ranking="

all_matches = []

for i, ranking_id in enumerate(ranking_ids):
    
    if (i + 1) % 50 == 0:
        print(f"  {i + 1}/{len(ranking_ids)} players... ({len(all_matches)} matches)")
    
    # Simple URL: base + ranking_id
    url = base_url + ranking_id
    
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'lxml')
    except:
        continue
    
    # Find match rows in tables
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 7:
                continue
            
            texts = [c.get_text(strip=True) for c in cells]
            
            # First cell must be a date
            if not re.match(r'\d{2}\.\d{2}\.\d{4}', texts[0]):
                continue
            
            try:
                player_elo = float(texts[2].replace(',', '.'))
                opponent_elo = float(texts[4].replace(',', '.'))
                expected_prob = float(texts[5].replace(',', '.'))
                elo_delta = float(texts[6].replace(',', '.'))
                win = 1 if elo_delta > 0 else 0
                
                all_matches.append({
                    'player_elo': player_elo,
                    'opponent_elo': opponent_elo,
                    'win': win,
                    'expected_prob': expected_prob,
                    'elo_delta': elo_delta
                })
            except:
                continue
    
    time.sleep(0.2)

# ============================================================
# STEP 3: Save to CSV
# ============================================================

print(f"\nTotal matches: {len(all_matches)}")

df = pd.DataFrame(all_matches)
df = df.drop_duplicates()
df.to_csv('data/historical_matches.csv', index=False)

print(f"Saved {len(df)} unique matches to data/historical_matches.csv")

if len(df) > 0:
    print(f"\nSummary:")
    print(f"  Player Elo: {df['player_elo'].min():.0f} - {df['player_elo'].max():.0f}")
    print(f"  Opponent Elo: {df['opponent_elo'].min():.0f} - {df['opponent_elo'].max():.0f}")
    print(f"  Win rate: {df['win'].mean()*100:.1f}%")
