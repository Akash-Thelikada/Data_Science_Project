"""
Models module - Elo calculations and probability functions
Contains Swiss Table Tennis formulas and ML model.
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

# ============================================================
# CONSTANTS
# ============================================================

K_FACTOR = 15  # Swiss Table Tennis K-factor
ELO_SCALE = 200  # Swiss Table Tennis uses 200

# ============================================================
# SWISS TABLE TENNIS FORMULAS
# ============================================================

def swiss_elo_probability(player_elo, opponent_elo):
    """
    Calculate win probability using Swiss Table Tennis formula.
    P = 1 / (1 + 10^((opponent_elo - player_elo) / 200))
    """
    return 1 / (1 + 10 ** ((opponent_elo - player_elo) / ELO_SCALE))


def calculate_delta_elo(result, probability):
    """
    Calculate Elo change after a match.
    delta_elo = K * (result - P)
    
    Parameters:
    - result: 1 for win, 0 for loss
    - probability: expected win probability (from Swiss formula)
    
    Returns: Elo change (positive = gained, negative = lost)
    """
    return K_FACTOR * (result - probability)

def calculate_final_elo(initial_elo, delta_elos):
    """
    Calculate final Elo after a series of matches.
    final_elo = initial_elo + sum(delta_elos)
    
    Parameters:
    - initial_elo: Elo at start of period
    - delta_elos: list of Elo changes from each match
    
    Returns: Final Elo
    """
    return initial_elo + sum(delta_elos)


# ============================================================
# ML MODEL
# ============================================================

# Global variable to store trained model
_ml_model = None


def train_ml_model(matches_file='data/raw/historical_matches.csv'):
    """
    Train logistic regression model on historical match data.
    Feature: Elo difference (player - opponent)
    Target: Win (1) or Loss (0)
    """
    global _ml_model
    
    # Load data
    df = pd.read_csv(matches_file)
    
    # Feature: Elo difference
    X = (df['player_elo'] - df['opponent_elo']).values.reshape(-1, 1)
    y = df['win'].values
    
    # Calculate Swiss Elo accuracy
    swiss_probs = 1 / (1 + 10 ** ((df['opponent_elo'] - df['player_elo']) / ELO_SCALE))
    swiss_predictions = (swiss_probs >= 0.5).astype(int)
    swiss_accuracy = (swiss_predictions == y).mean()
    print(f"Swiss Elo formula accuracy: {swiss_accuracy:.3f}")
    
    # Train ML model
    _ml_model = LogisticRegression()
    _ml_model.fit(X, y)
    
    accuracy = _ml_model.score(X, y)
    print(f"ML model trained on {len(df)} matches (accuracy: {accuracy:.3f})")
    
    return _ml_model

def ml_probability(player_elo, opponent_elo):
    """
    Calculate win probability using ML model.
    Must call train_ml_model() first.
    """
    global _ml_model
    
    if _ml_model is None:
        train_ml_model()
    
    elo_diff = player_elo - opponent_elo
    return _ml_model.predict_proba([[elo_diff]])[0][1]

def get_ml_model():
    """Get the trained ML model."""
    global _ml_model
    
    if _ml_model is None:
        train_ml_model()
    
    return _ml_model


# ============================================================
# MATCH SIMULATION
# ============================================================

def simulate_match_elo(player_elo, opponent_elo):
    """
    Simulate a match using Swiss Elo formula for win probability.
    
    Returns: (win, delta_elo)
    - win: 1 if player wins, 0 if loses
    - delta_elo: Elo change for this match
    """
    p = swiss_elo_probability(player_elo, opponent_elo)
    win = 1 if np.random.random() < p else 0
    delta = calculate_delta_elo(win, p)
    return win, delta

def simulate_match_ml(player_elo, opponent_elo):
    """
    Simulate a match using ML model for win probability.
    Delta Elo is still calculated with Swiss formula (official rule).
    
    Returns: (win, delta_elo)
    - win: 1 if player wins, 0 if loses
    - delta_elo: Elo change for this match
    """
    p_ml = ml_probability(player_elo, opponent_elo)
    p_elo = swiss_elo_probability(player_elo, opponent_elo)
    win = 1 if np.random.random() < p_ml else 0
    delta = calculate_delta_elo(win, p_elo)
    return win, delta


