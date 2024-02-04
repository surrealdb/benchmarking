import pathlib
import json

from bench_utils import calculate_latency_metrics, plot_box, create_markdown_table

# create report

# Structure of report
# - label
# - SDB & MDB box plots
# - Query
# - Table headers - | Metric | SurrealDB | MongoDB | Difference


output_files_path = pathlib.Path(__file__).parents[1] / "output_files"

# Get SurrealDB results
# with open(output_files_path / pathlib.Path("surrealdb_bench_output.json"), "r") as file:
#     surrealdb_bench_results = json.read(file)

# Get MongoDB results
with open(output_files_path / pathlib.Path("mongodb_bench_output.json"), "r") as file:
    mongodb_bench_results = json.load(file)


sdb_test = calculate_latency_metrics(mongodb_bench_results["total_write_duration"])
mdb_test = calculate_latency_metrics(mongodb_bench_results["total_read_duration"])


report = f"""
# Surreal bench

This benchmark compares SurrealDB and MongoDB performance across a variety of CRUD queries.

## Aggregate results

| Metric | SurrealDB | MongoDB | Difference
P99 throughput (QPS)
P99 latency (ms)
P99 read latency (ms)
P99 write latency (ms) 

## Detailed results

### Insert
both throughput and latency?

**Shape of the data:** box plot on an independent scale, showing the below info visually

SDB: {plot_box(mongodb_bench_results["total_write_duration"])}
MDB: {plot_box(mongodb_bench_results["total_read_duration"])}

**Metrics table:**

{create_markdown_table(sdb_test, mdb_test)}


"""
print(report)