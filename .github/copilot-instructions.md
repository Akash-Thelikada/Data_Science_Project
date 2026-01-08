# Swiss Table Tennis Elo Rating Simulation Project

## Project Overview
This is a Python data science project for simulating and analyzing Swiss table tennis Elo rating changes over a competitive month.

## Project Structure
```
├── README.md              # Setup and usage instructions
├── PROPOSAL.md            # Project proposal
├── environment.yml        # Conda dependencies
├── requirements.txt       # Pip dependencies
├── main.py               # Entry point
├── src/                  # Source code modules
│   ├── __init__.py
│   ├── data_loader.py    # Data loading and preprocessing
│   ├── models.py         # Elo model definitions
│   └── evaluation.py     # Evaluation and visualization
├── data/
│   └── raw/              # Original data
├── results/              # Output figures and metrics
└── notebooks/            # Jupyter notebooks for exploration
```

## Development Guidelines
- Use `random_state=42` for reproducibility in all random operations
- Run `python main.py` to execute the simulation
- All visualizations are saved to the `results/` folder

## Running the Project
1. Create conda environment: `conda env create -f environment.yml`
2. Activate environment: `conda activate elo-simulation`
3. Run: `python main.py`

## Key Concepts
- **Elo Rating**: Numerical rating representing player competitive strength
- **Elo Update Rule**: Deterministic rule for updating ratings after matches
- **Monte Carlo Simulation**: Used to explore range of plausible Elo outcomes
