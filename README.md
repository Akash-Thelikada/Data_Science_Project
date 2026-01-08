# Swiss Table Tennis Elo Rating Simulation

A Python project for simulating and analyzing Swiss table tennis Elo rating changes over a competitive month using Monte Carlo simulation.

## Research Question

> How uncertain is the short-term evolution of a Swiss table tennis player's Elo ranking over a typical month of competition, and how sensitive is this uncertainty to the choice of win-probability model?

## Project Overview

This project treats a month of competition as a **random experiment** rather than a fixed trajectory. Instead of predicting a single future Elo value, we simulate the same month **1000 times** to understand the **distribution of possible outcomes**.

### Key Idea

The simulation compares two probability models:

1. **Elo-Based Model**: Uses the official Swiss Table Tennis Elo formula
2. **ML-Based Model**: Uses logistic regression trained on historical match data

Both models use the same Elo update rule - they only differ in how match outcomes are simulated.

## Project Structure

```
├── README.md                      # This file - setup and usage
├── PROPOSAL.md                    # Detailed methodology
├── environment.yml                # Conda dependencies
├── requirements.txt               # Pip dependencies
├── main.py                        # Entry point - run this file
├── src/
│   ├── __init__.py               # Package imports
│   ├── models.py                 # Probability functions (simple!)
│   └── evaluation.py             # Visualization functions
├── data/
│   └── raw/
│       ├── league_club1.csv      # First opponent club roster
│       ├── league_club2.csv      # Second opponent club roster
│       ├── tournament_pool.csv   # Tournament opponent pool (A/B series)
│       └── historical_matches.csv # Training data for ML model
├── results/
│   ├── elo_comparison.png        # Comparison histogram
│   └── simulation_results.csv    # Raw simulation data
└── notebooks/                     # Optional exploration
```

## Competition Structure

Each simulated month consists of:

| Competition | Matches | Description |
|-------------|---------|-------------|
| League Encounter 1 | 3 | Against Club 1 roster |
| League Encounter 2 | 3 | Against Club 2 roster |
| Tournament Group | 3 | Group stage matches |
| Tournament Placement | 4 | Placement matches |
| **TOTAL** | **13** | Matches per month |

## Installation

### Option 1: Using Conda (Recommended)

```bash
conda env create -f environment.yml
conda activate elo-simulation
```

### Option 2: Using pip

```bash
pip install -r requirements.txt
```

**Required packages:** numpy, pandas, matplotlib, scikit-learn

## Usage

### 1. Prepare Your Data (Optional)

Replace the synthetic data files in `data/raw/` with your real data:

- `league_club1.csv`: Your first opponent club's roster
- `league_club2.csv`: Your second opponent club's roster
- `tournament_pool.csv`: A/B series players from Swiss ranking
- `historical_matches.csv`: Historical match results for ML training

### 2. Set Your Initial Elo

Edit `main.py` and change:

```python
INITIAL_ELO = 1500  # <-- CHANGE THIS TO YOUR ACTUAL ELO
```

### 3. Run the Simulation

```bash
python main.py
```

### Output

The simulation produces:

1. **Console output**: Summary statistics comparing both models
2. **`results/elo_comparison.png`**: Side-by-side histogram comparison
3. **`results/simulation_results.csv`**: Raw data for further analysis

## Configuration

Edit these values in `main.py`:

```python
RANDOM_SEED = 42           # For reproducibility
NUM_SIMULATIONS = 1000     # Number of Monte Carlo runs
INITIAL_ELO = 1500         # Your starting Elo (REPLACE!)
K_FACTOR = 15              # Swiss TT K-factor
```

## Key Formulas

### Win Probability (Swiss Elo Formula)

$$P(\text{win}) = \frac{1}{1 + 10^{(E_{opponent} - E_{player})/200}}$$

Note: Swiss Table Tennis uses scale=200, not 400 like chess.

### Elo Update Rule

$$\Delta Elo = K \times (result - P)$$

Where:
- K = 15 (Swiss TT standard)
- result = 1 (win) or 0 (loss)
- P = expected score from Elo formula

## Important Methodological Notes

1. **Fixed Elo**: The player's Elo is held constant throughout the month. All probability calculations use the initial Elo rating.

2. **Elo Updates**: Changes are accumulated and applied at month end. Individual match Elo changes do NOT affect subsequent match probabilities.

3. **Two Models, Same Update Rule**: The Elo-based and ML-based models differ only in how match outcomes are simulated. Both use the official Elo update formula.

4. **No Rematches**: Tournament opponents are sampled without replacement.

## Data File Formats

### League Club Rosters (`league_club1.csv`, `league_club2.csv`)

```csv
name,elo,participation_rate
Mueller Thomas,1620,0.85
Schneider Hans,1580,0.70
```

### Tournament Pool (`tournament_pool.csv`)

```csv
name,elo
Ackermann Beat,1680
Bader Simon,1655
```

### Historical Matches (`historical_matches.csv`)

```csv
player_elo,opponent_elo,result
1500,1450,1
1500,1550,0
```

## Reproducibility

- All random operations use `RANDOM_SEED = 42`
- Same seed produces identical results every run
- Dependencies listed in `environment.yml` and `requirements.txt`

## License

This project is for educational purposes.
