"""
Swiss Table Tennis Elo Rating Simulation
Monte Carlo simulation for Braden Hasler's Elo evolution over one month.
Compares Swiss Elo formula vs ML model predictions.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.models import (
    train_ml_model,
    swiss_elo_probability,
    ml_probability,
    simulate_match_elo,
    simulate_match_ml,
    K_FACTOR,
    ELO_SCALE
)

# Set random seed for reproducibility
np.random.seed(42)

# ============================================================
# PARAMETERS
# ============================================================

# Focal player
PLAYER_NAME = "Braden Hasler"
PLAYER_ELO = 1468

# Number of Monte Carlo simulations
N_SIMULATIONS = 10000

# ============================================================
# TRAIN ML MODEL
# ============================================================

print("Training ML model...")
train_ml_model('data/raw/historical_matches.csv')

# ============================================================
# LEAGUE OPPONENTS DATA
# ============================================================

# Load league encounters from CSV
league_df = pd.read_csv('data/raw/league_encounters.csv')

# Compute encounter probability (interventions / 8)
league_df['encounter_prob'] = league_df['interventions'] / 8

# Split into teams
mandement_df = league_df[league_df['team'] == 'Mandement']
bulle_df = league_df[league_df['team'] == 'Bulle']

# Convert to list of dicts for simulation
mandement_team = mandement_df.to_dict('records')
bulle_team = bulle_df.to_dict('records')

# ============================================================
# NATIONAL OPPONENTS DATA
# ============================================================

# Load national opponents (players with ELO_KLASSIERUNG 11-21)
female_df = pd.read_csv('data/raw/elo-rankings_female_20251218.csv', sep=';', encoding='latin-1')
female_df = female_df[(female_df['ELO_KLASSIERUNG'] >= 11) & (female_df['ELO_KLASSIERUNG'] <= 21)]

male_df = pd.read_csv('data/raw/elo-rankings_male_20251218.csv', sep=';', encoding='latin-1')
male_df = male_df[(male_df['ELO_KLASSIERUNG'] >= 11) & (male_df['ELO_KLASSIERUNG'] <= 21)]

national_opponents = pd.concat([female_df, male_df], ignore_index=True)

national_elos = national_opponents['ELO_WERT'].values

print(f"Loaded {len(national_elos)} national opponents")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def draw_league_opponents(team, n_matches):
    """
    Draw n_matches opponents based on intervention probability.
    Each player appears with probability = interventions/8.
    Higher intervention players are checked first.
    """
    # Sort by interventions (highest first)
    sorted_team = sorted(team, key=lambda p: -p["interventions"])
    
    selected = []
    for p in sorted_team:
        prob = p["interventions"] / 8
        if np.random.random() < prob:
            selected.append(p["elo"])
            if len(selected) == n_matches:
                break
    
    return selected


def draw_national_opponents(n_matches):
    """
    Draw n_matches opponents from national pool with equiprobability.
    No repeat opponents allowed.
    """
    indices = np.random.choice(len(national_elos), size=n_matches, replace=False)
    return national_elos[indices]




# ============================================================
# MONTE CARLO SIMULATION
# ============================================================

print(f"\nRunning {N_SIMULATIONS} Monte Carlo simulations...")
print(f"Focal player: {PLAYER_NAME} (Elo: {PLAYER_ELO})")

final_elos_elo = []  # Using Swiss Elo formula
final_elos_ml = []   # Using ML model

# Store all simulated matches
all_matches = []

for sim in range(N_SIMULATIONS):
    
    total_delta_elo = 0
    total_delta_ml = 0
    match_num = 0
    
    # --- League encounters: 6 matches ---
    # 3 matches vs Mandement
    mandement_opponents = draw_league_opponents(mandement_team, 3)
    for opp_elo in mandement_opponents:
        match_num += 1
        win_elo, delta_elo = simulate_match_elo(PLAYER_ELO, opp_elo)
        win_ml, delta_ml = simulate_match_ml(PLAYER_ELO, opp_elo)
        total_delta_elo += delta_elo
        total_delta_ml += delta_ml
        all_matches.append({
            'simulation': sim + 1,
            'match': match_num,
            'match_type': 'league',
            'opponent_team': 'Mandement',
            'player_elo': PLAYER_ELO,
            'opponent_elo': opp_elo,
            'win_elo': win_elo,
            'delta_elo': delta_elo,
            'win_ml': win_ml,
            'delta_ml': delta_ml
        })
    
    # 3 matches vs Bulle
    bulle_opponents = draw_league_opponents(bulle_team, 3)
    for opp_elo in bulle_opponents:
        match_num += 1
        win_elo, delta_elo = simulate_match_elo(PLAYER_ELO, opp_elo)
        win_ml, delta_ml = simulate_match_ml(PLAYER_ELO, opp_elo)
        total_delta_elo += delta_elo
        total_delta_ml += delta_ml
        all_matches.append({
            'simulation': sim + 1,
            'match': match_num,
            'match_type': 'league',
            'opponent_team': 'Bulle',
            'player_elo': PLAYER_ELO,
            'opponent_elo': opp_elo,
            'win_elo': win_elo,
            'delta_elo': delta_elo,
            'win_ml': win_ml,
            'delta_ml': delta_ml
        })
    
    # --- National encounters: 7 matches ---
    national_opps = draw_national_opponents(7)
    for opp_elo in national_opps:
        match_num += 1
        win_elo, delta_elo = simulate_match_elo(PLAYER_ELO, opp_elo)
        win_ml, delta_ml = simulate_match_ml(PLAYER_ELO, opp_elo)
        total_delta_elo += delta_elo
        total_delta_ml += delta_ml
        all_matches.append({
            'simulation': sim + 1,
            'match': match_num,
            'match_type': 'national',
            'opponent_team': 'National',
            'player_elo': PLAYER_ELO,
            'opponent_elo': opp_elo,
            'win_elo': win_elo,
            'delta_elo': delta_elo,
            'win_ml': win_ml,
            'delta_ml': delta_ml
        })
    
    # Calculate final Elos
    final_elos_elo.append(PLAYER_ELO + total_delta_elo)
    final_elos_ml.append(PLAYER_ELO + total_delta_ml)

final_elos_elo = np.array(final_elos_elo)
final_elos_ml = np.array(final_elos_ml)

# Save all simulated matches to CSV
matches_df = pd.DataFrame(all_matches)
matches_df.to_csv('results/simulated_matches.csv', index=False)
print(f"Saved {len(matches_df)} simulated matches to results/simulated_matches.csv")

# ============================================================
# RESULTS
# ============================================================

print("\n" + "="*50)
print("SIMULATION RESULTS")
print("="*50)

print(f"\nStarting Elo: {PLAYER_ELO}")

print("\n--- Swiss Elo Formula ---")
print(f"Mean final Elo: {final_elos_elo.mean():.1f}")
print(f"Std deviation: {final_elos_elo.std():.1f}")
print(f"Range: [{final_elos_elo.min():.1f}, {final_elos_elo.max():.1f}]")
print(f"90% interval: [{np.percentile(final_elos_elo, 5):.1f}, {np.percentile(final_elos_elo, 95):.1f}]")

print("\n--- ML Model ---")
print(f"Mean final Elo: {final_elos_ml.mean():.1f}")
print(f"Std deviation: {final_elos_ml.std():.1f}")
print(f"Range: [{final_elos_ml.min():.1f}, {final_elos_ml.max():.1f}]")
print(f"90% interval: [{np.percentile(final_elos_ml, 5):.1f}, {np.percentile(final_elos_ml, 95):.1f}]")

# ============================================================
# VISUALIZATION
# ============================================================

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Plot 1: Swiss Elo distribution
axes[0].hist(final_elos_elo, bins=50, edgecolor='black', alpha=0.7, color='blue')
axes[0].axvline(PLAYER_ELO, color='red', linestyle='--', linewidth=2, label=f'Starting ({PLAYER_ELO})')
axes[0].axvline(final_elos_elo.mean(), color='green', linestyle='-', linewidth=2, label=f'Mean ({final_elos_elo.mean():.1f})')
axes[0].set_xlabel('Final Elo')
axes[0].set_ylabel('Frequency')
axes[0].set_title('Swiss Elo Formula')
axes[0].legend()

# Plot 2: ML model distribution
axes[1].hist(final_elos_ml, bins=50, edgecolor='black', alpha=0.7, color='orange')
axes[1].axvline(PLAYER_ELO, color='red', linestyle='--', linewidth=2, label=f'Starting ({PLAYER_ELO})')
axes[1].axvline(final_elos_ml.mean(), color='green', linestyle='-', linewidth=2, label=f'Mean ({final_elos_ml.mean():.1f})')
axes[1].set_xlabel('Final Elo')
axes[1].set_ylabel('Frequency')
axes[1].set_title('ML Model')
axes[1].legend()

plt.suptitle(f'{PLAYER_NAME} - Final Elo Distribution ({N_SIMULATIONS} simulations)', fontsize=14)
plt.tight_layout()
plt.savefig('results/elo_distribution.png', dpi=150)
plt.show()

print("\nPlot saved to: results/elo_distribution.png")
