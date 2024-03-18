# bench surreal deal - MongoDB

## Purpose

The goal of this benchmark is show performance differences with MongoDB across a variety of complex queries.

## Requirements

- Python
- [Poetry package manager](https://python-poetry.org/docs/#installation)
- [SurrealDB](https://surrealdb.com/install) 
- [MongoDB](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-os-x/)

## Usage

Start up a SurrealDB server:
```bash
surreal start  --user root --pass root  --allow-all
```

Start up a MongoDB server:
```bash
brew services start mongodb-community@7.0
```

Install the Python dependencies

```bash
poetry install
```

Run each bench:

```bash
poetry run python run_surrealdb_bench.py 
```

```bash
poetry run python run_mongodb_bench.py
```

To generate the report run
```bash
poetry run python create_bench_report.py
```

## Structure

- define_ files are where functions for each query are made
- run_ files are for running the bench
- create_bench_report takes the data from the run_ files and generates a markdown report
- bench_utils are helper functions to do various things