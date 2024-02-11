import pathlib
import json

from bench_utils import create_markdown_metrics_table

# create report

# Structure of report
# - label
# - SDB & MDB box plots
# - Query
# - Table headers - | Metric | SurrealDB | MongoDB | Difference


output_files_path = pathlib.Path(__file__).parents[1] / "output_files"

# Get SurrealDB results
with open(output_files_path / pathlib.Path("surrealdb_bench_output.json"), "r") as file:
    sdb_results = json.load(file)

# Get MongoDB results
with open(output_files_path / pathlib.Path("mongodb_bench_output.json"), "r") as file:
    mdb_results = json.load(file)


report = f"""
# Surreal bench

This benchmark compares SurrealDB and MongoDB performance across a variety of CRUD queries.

## Aggregate results

| Metric | SurrealDB | MongoDB | Difference
P99 throughput (QPS)
P99 latency (ms)
P99 read latency (ms)
P99 write latency (ms) 


# overall latency

{create_markdown_metrics_table(
    sdb_results["total_time_duration"], 
    mdb_results["total_time_duration"]
)}

{create_markdown_metrics_table(
    sdb_results["total_read_duration"], 
    mdb_results["total_read_duration"]
)}


sdb_results["total_write_duration"],
mdb_results["total_write_duration"]

sdb_results["total_time_duration"],
mdb_results["total_time_duration"]

sdb_results["total_queries_count"],
mdb_results["total_queries_count"]

sdb_results["total_throughput_qps"],
mdb_results["total_throughput_qps"]

sdb_results["wall_time_duration"],
mdb_results["wall_time_duration"]

sdb_results["index_duration"],
mdb_results["index_duration"]

## Grouped results

### Read latency

{create_markdown_metrics_table(
    sdb_results["read_filter_duration"],
    mdb_results["read_filter_duration"]
)}


{create_markdown_metrics_table(
    sdb_results["read_relationships_duration"],
    mdb_results["read_relationships_duration"]
)}

{create_markdown_metrics_table(
    sdb_results["read_aggregation_duration"],
    mdb_results["read_aggregation_duration"]
)}

{create_markdown_metrics_table(
    sdb_results["update_duration"],
    mdb_results["update_duration"]
)}

{create_markdown_metrics_table(
    sdb_results["delete_duration"],
    mdb_results["delete_duration"]
)}

{create_markdown_metrics_table(
    sdb_results["transactions_duration"],
    mdb_results["transactions_duration"]
)}

{create_markdown_metrics_table(
    sdb_results["insert_duration"],
    mdb_results["insert_duration"]
)}

## Detailed results

### Insert

{create_markdown_metrics_table(
    sdb_results["insert_person"],
    mdb_results["insert_person"]
)}

{create_markdown_metrics_table(
    sdb_results["insert_artist"],
    mdb_results["insert_artist"]
)}

{create_markdown_metrics_table(
    sdb_results["insert_order"],
    mdb_results["insert_order"]
)}

{create_markdown_metrics_table(
    sdb_results["insert_review"],
    mdb_results["insert_review"]
)}

{create_markdown_metrics_table(
    sdb_results["q4_index"],
    mdb_results["q4_index"]
)}

{create_markdown_metrics_table(
    sdb_results["q5_index"],
    mdb_results["q5_index"]
)}

{create_markdown_metrics_table(
    sdb_results["q8_index"],
    mdb_results["q8_index"]
)}

{create_markdown_metrics_table(
    sdb_results["q10_index"],
    mdb_results["q10_index"]
)}

{create_markdown_metrics_table(
    sdb_results["q1"],
    mdb_results["q1"]
)}

{create_markdown_metrics_table(
    sdb_results["q2"],
    mdb_results["q2"]
)}

{create_markdown_metrics_table(
    sdb_results["q3"],
    mdb_results["q3"]
)}

{create_markdown_metrics_table(
    sdb_results["q4"],
    mdb_results["q4"]
)}

{create_markdown_metrics_table(
    sdb_results["q5"],
    mdb_results["q5"]
)}

{create_markdown_metrics_table(
    sdb_results["q6"],
    mdb_results["q6"]
)}

{create_markdown_metrics_table(
    sdb_results["update_one"],
    mdb_results["update_one"]
)}

{create_markdown_metrics_table(
    sdb_results["update_many"],
    mdb_results["update_many"]
)}

{create_markdown_metrics_table(
    sdb_results["delete_one"],
    mdb_results["delete_one"]
)}

{create_markdown_metrics_table(
    sdb_results["delete_many"],
    mdb_results["delete_many"]
)}

{create_markdown_metrics_table(
    sdb_results["tx1_insert_update"],
    mdb_results["tx1_insert_update"]
)}

{create_markdown_metrics_table(
    sdb_results["tx2_insert"],
    mdb_results["tx2_insert"]
)}

"""
print(report)


# sdb_results["delete_query_count"],
# mdb_results["delete_query_count"]

# sdb_results["update_query_count"],
# mdb_results["update_query_count"]

# sdb_results["index_query_count"],
# mdb_results["index_query_count"]

# sdb_results["insert_query_count"],
# mdb_results["insert_query_count"]

# sdb_results["read_query_count"],
# mdb_results["read_query_count"]

# sdb_results["transactions_count"],
# mdb_results["transactions_count"]

