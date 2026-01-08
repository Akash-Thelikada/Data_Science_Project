"""
Evaluation module - Visualization and statistics
Generates plots for historical matches and probability comparison.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression

# ============================================================
# PLOT 1: Historical Matches Scatter (Win/Loss)
# ============================================================

print("Generating historical matches scatter plot...")

# Load data
df = pd.read_csv('data/raw/historical_matches.csv')

# Separate wins and losses
wins = df[df['win'] == 1]
losses = df[df['win'] == 0]

# Plot
plt.figure(figsize=(10, 8))
plt.scatter(losses['player_elo'], losses['opponent_elo'], c='red', alpha=0.5, s=10, label=f'Loss ({len(losses)})')
plt.scatter(wins['player_elo'], wins['opponent_elo'], c='green', alpha=0.5, s=10, label=f'Win ({len(wins)})')
plt.plot([400, 2000], [400, 2000], 'k--', alpha=0.3, label='Equal Elo')
plt.xlabel('Player Elo')
plt.ylabel('Opponent Elo')
plt.title('Historical Matches: Player vs Opponent Elo')
plt.legend()
plt.tight_layout()
plt.savefig('results/matches_scatter.png', dpi=150)
plt.show()

print("  Saved: results/matches_scatter.png")

# ============================================================
# PLOT 2: Probability Comparison (Swiss Elo vs ML)
# ============================================================

print("Generating probability comparison plot...")

# Train ML model
X = (df['player_elo'] - df['opponent_elo']).values.reshape(-1, 1)
y = df['win'].values
ml_model = LogisticRegression()
ml_model.fit(X, y)

# Fixed player Elo (Braden Hasler)
player_elo = 1468

# Range of opponent Elos
opponent_elos = np.linspace(800, 2000, 200)

# Calculate probabilities
swiss_probs = []
ml_probs = []

for opp_elo in opponent_elos:
    # Swiss formula
    p_swiss = 1 / (1 + 10 ** ((opp_elo - player_elo) / 200))
    swiss_probs.append(p_swiss)
    
    # ML model
    elo_diff = player_elo - opp_elo
    p_ml = ml_model.predict_proba([[elo_diff]])[0][1]
    ml_probs.append(p_ml)

# Plot
plt.figure(figsize=(10, 6))
plt.plot(opponent_elos, swiss_probs, 'b-', linewidth=2, label='Swiss Elo Formula')
plt.plot(opponent_elos, ml_probs, 'r-', linewidth=2, label='ML Model')
plt.axhline(0.5, color='gray', linestyle='--', alpha=0.5, label='50% win probability')
plt.axvline(player_elo, color='green', linestyle='--', alpha=0.5, label=f'Player Elo ({player_elo})')
plt.xlabel('Opponent Elo')
plt.ylabel('Win Probability')
plt.title(f'Win Probability vs Opponent Elo (Player Elo = {player_elo})')
plt.legend()
plt.grid(True, alpha=0.3)
plt.xlim(800, 2000)
plt.ylim(0, 1)
plt.tight_layout()
plt.savefig('results/probability_comparison.png', dpi=150)
plt.show()

print("  Saved: results/probability_comparison.png")

print("\nDone!")
