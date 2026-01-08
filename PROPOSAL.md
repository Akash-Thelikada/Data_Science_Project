# Project Proposal: Swiss Table Tennis Elo Rating Simulation

## Research Question

How uncertain is the short-term evolution of a Swiss table tennis player's Elo ranking over a typical month of competition, and how sensitive is this uncertainty to the choice of win-probability model (official Elo-based probabilities versus a data-driven machine learning alternative)?

## Context and Motivation

In Swiss table tennis, each competitive player is assigned a numerical rating called **Elo**. This rating represents a player's competitive strength:

- A player with more Elo points is considered stronger than a player with fewer points
- The difference in Elo points between two players reflects how likely one player is to win
- When a player wins or loses a match, their Elo score changes according to a predefined rule

While the Elo update rule is **deterministic** once a match result is known, real competition is inherently **uncertain**:

- Players face different opponents
- Match outcomes fluctuate
- A single month of competition can lead to very different ranking trajectories

As a competitive player, I am not interested in predicting a single "most likely" future Elo value. Instead, I want to understand the **range of plausible outcomes**:

> *How much could my Elo realistically increase or decrease over a typical month of competition?*

## Core Idea

The key idea is to treat a month of competition as a **random experiment** rather than a fixed trajectory.

Instead of asking: *"What will my Elo be next month?"*

The project asks: *"If this month were replayed many times under realistic conditions, what distribution of Elo outcomes would we observe?"*

To answer this question, the project uses **Monte Carlo simulation**: the same month is simulated 1000 times, each time with different random match outcomes. This produces a **distribution** of final Elo points, rather than a single predicted value.

## Methodology

### Step 1 - Initialization

- A focal player is defined with an initial Elo rating at the start of the month
- This Elo rating remains **fixed** throughout the simulated month
- All variability arises from randomness in opponent selection and match outcomes

### Step 2 - Competition Structure

Each simulated month consists of:

| Competition | Matches | Description |
|-------------|---------|-------------|
| League Encounter 1 | 3 | Against first scheduled club |
| League Encounter 2 | 3 | Against second scheduled club |
| Tournament Group Stage | 3 | Group matches |
| Tournament Placement | 4 | Placement matches |
| **TOTAL** | **13** | Matches per month |

### Step 3 - Opponent Selection

**League matches:**
- Opponents drawn from scheduled club rosters
- Selection probability proportional to observed participation rates

**Tournament matches:**
- Opponents sampled uniformly from A/B series player pool
- No-rematch constraint within tournament

### Step 4 - Win Probability Assignment

Two alternative models are compared:

1. **Elo-Based Probability Model**
   
   $$P(\text{win}) = \frac{1}{1 + 10^{(E_{opponent} - E_{player})/200}}$$

2. **Machine Learning Probability Model**
   - Logistic regression trained on historical match data
   - Uses same inputs (player Elo, opponent Elo)
   - Learns relationship from data rather than assuming formula

### Step 5 - Match Outcome Simulation

- Match outcome simulated as binary random variable (win/loss)
- Outcome determined by random draw based on assigned probability

### Step 6 - Elo Update Calculation

After each match:

$$\Delta Elo = K \times (result - P)$$

Where:
- K = 15 (Swiss Table Tennis standard)
- result = 1 (win) or 0 (loss)
- P = official Elo-based probability (always used for updates)

**Important:** Elo changes do NOT affect subsequent match probabilities within the month.

### Step 7 - End-of-Month Elo

$$Elo_{end} = Elo_{start} + \sum_{m=1}^{13} \Delta Elo_m$$

### Step 8 - Monte Carlo Repetition

Steps 1-7 repeated 1000 times, generating an empirical distribution of outcomes.

### Step 9 - Analysis

From the distribution:
- Summary statistics (mean, variance, quantiles)
- Comparison across probability models
- Risk profile interpretation

## Expected Results

The distribution of end-of-month Elo ratings is expected to be:

- **Approximately unimodal** (one peak)
- **Centered around initial Elo** (if facing similar-strength opponents)
- **With substantial dispersion** due to 13 matches

Key comparisons between probability models:
- Mean Elo change (expected performance)
- Dispersion (ranking volatility)
- Tail behavior (extreme outcomes)

## Interpretation Framework

The simulated Elo distributions are interpreted as **risk profiles**:

| Statistic | Interpretation |
|-----------|----------------|
| Mean change | Expected performance |
| Standard deviation | Ranking volatility |
| 5th percentile | Downside risk |
| 95th percentile | Upside potential |

## Simplifications and Limitations

The following factors are intentionally excluded:

- Physical condition, fatigue, injuries
- Psychological effects
- Strategic behavior during tournaments
- Long-term form dynamics
- Set scores and margin of victory

These exclusions keep the model transparent and interpretable.

## What This Project Does NOT Do

- Does **not** predict a single future Elo value
- Does **not** claim one probability model is "better"
- Does **not** account for momentum effects within a month

## What This Project DOES Do

- Quantifies **uncertainty** in short-term Elo evolution
- Compares **sensitivity** to probability modeling assumptions
- Provides a **risk profile** for ranking changes

## Technical Implementation

### Simple Code Philosophy

All code is written to be:
- **Simple**: No complex classes or nested structures
- **Explainable**: Every line can be explained
- **Transparent**: Clear variable names and extensive comments

### Key Files

| File | Purpose |
|------|---------|
| `main.py` | Linear simulation script (entry point) |
| `src/models.py` | Three simple functions for probabilities/updates |
| `src/evaluation.py` | Histogram visualization |
| `data/raw/*.csv` | Input data (replaceable with real data) |

## Reproducibility

- Random seed: 42
- All dependencies in `environment.yml`
- Single command execution: `python main.py`
