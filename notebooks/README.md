# Notebooks

This directory is intended for exploratory data analysis (EDA) and documentation notebooks.

## Planned notebooks

| Notebook | Description |
|---|---|
| `01_eda_nyc_tlc.ipynb` | Exploratory analysis of NYC TLC Yellow Taxi data |
| `02_feature_engineering.ipynb` | Feature engineering walkthrough |
| `03_model_evaluation.ipynb` | Model evaluation and comparison |

## How to run

```bash
pip install -r requirements-dev.txt
jupyter notebook
```

Notebooks can be run after generating sample data:

```bash
python data/sample/generate_sample.py --rows 5000 --output data/sample/yellow_taxi_sample.csv
```
