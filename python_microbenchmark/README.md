# Microbenchmarks

Contains:
- bench.py for running the benchmark
- charts.py for making charts

Inspiration for how things would look once finished: https://krausest.github.io/js-framework-benchmark/2023/table_chrome_117.0.5938.62.html

## Purpose

The goal of this benchmark is to test the differences in the various options for each statement and various ways to do the same thing and have it visualised.
This is not fully implemented yet.

## Requirements

- Python
- [Poetry package manager](https://python-poetry.org/docs/#installation)
- [SurrealDB](https://surrealdb.com/install) 


## Usage

Start up a SurrealDB server:
```bash
surreal start  --user root --pass root  --allow-all
```

Install the Python dependencies

```bash
poetry install
```

Run the bench:

```bash
poetry run python bench.py 
```

Generate the charts:

```bash
poetry run python charts.py
```