import pathlib
import json

from bench_utils import create_markdown_summary_table, create_markdown_metrics_table, table_definition

# create report

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

{print(table_definition)}

### Overall results

{create_markdown_summary_table(
    sdb_results, 
    mdb_results
)}

Total number of queries per run: {sdb_results["total_queries_count"][0]}

### Overall throughput

{create_markdown_metrics_table(
    sdb_results["total_throughput_qps"],
    mdb_results["total_throughput_qps"]
, metric="throughput", unit="s")}

### Overall latency

{create_markdown_metrics_table(
    sdb_results["total_time_duration"], 
    mdb_results["total_time_duration"]
)}

### Overall write latency

{create_markdown_metrics_table(
    sdb_results["total_write_duration"],
    mdb_results["total_write_duration"]
)}

### Overall read latency

{create_markdown_metrics_table(
    sdb_results["total_read_duration"], 
    mdb_results["total_read_duration"]
)}

## Results by category
Each category consists of 2 or more queries

### Write latency

#### Insert

{create_markdown_metrics_table(
    sdb_results["insert_duration"],
    mdb_results["insert_duration"]
)}

#### Update

{create_markdown_metrics_table(
    sdb_results["update_duration"],
    mdb_results["update_duration"]
)}

#### Delete

{create_markdown_metrics_table(
    sdb_results["delete_duration"],
    mdb_results["delete_duration"]
)}

#### Index

{create_markdown_metrics_table(
    sdb_results["index_duration"],
    mdb_results["index_duration"]
)}

#### Transaction

{create_markdown_metrics_table(
    sdb_results["transactions_duration"],
    mdb_results["transactions_duration"]
)}

### Read latency

#### Read filter & order

{create_markdown_metrics_table(
    sdb_results["read_filter_duration"],
    mdb_results["read_filter_duration"]
)}

#### Read relationships

{create_markdown_metrics_table(
    sdb_results["read_relationships_duration"],
    mdb_results["read_relationships_duration"]
)}

#### Read aggregation

{create_markdown_metrics_table(
    sdb_results["read_aggregation_duration"],
    mdb_results["read_aggregation_duration"]
)}

## Results by query

### Insert person

{create_markdown_metrics_table(
    sdb_results["insert_person"],
    mdb_results["insert_person"]
)}

### Insert artist

{create_markdown_metrics_table(
    sdb_results["insert_artist"],
    mdb_results["insert_artist"]
)}

### Insert product

{create_markdown_metrics_table(
    sdb_results["insert_product"],
    mdb_results["insert_product"]
)}

### Insert order

{create_markdown_metrics_table(
    sdb_results["insert_order"],
    mdb_results["insert_order"]
)}

### Insert review

{create_markdown_metrics_table(
    sdb_results["insert_review"],
    mdb_results["insert_review"]
)}

### Update one

{create_markdown_metrics_table(
    sdb_results["update_one"],
    mdb_results["update_one"],
unit="us")}

### Update many
**NOTE:** we currently only have an index working on Select, coming later for Update/delete
Therefore this query cannot make use of the index that was created for it.

{create_markdown_metrics_table(
    sdb_results["update_many"],
    mdb_results["update_many"]
)}

### Delete one

{create_markdown_metrics_table(
    sdb_results["delete_one"],
    mdb_results["delete_one"],
unit="us")}

### Delete many
**NOTE:** we currently only have an index working on Select, coming later for Update/delete
Therefore this query cannot make use of the index that was created for it.

{create_markdown_metrics_table(
    sdb_results["delete_many"],
    mdb_results["delete_many"]
)}

### Transaction insert & update

{create_markdown_metrics_table(
    sdb_results["tx1_insert_update"],
    mdb_results["tx1_insert_update"],
unit="us")}

### Transaction insert x 2

{create_markdown_metrics_table(
    sdb_results["tx2_insert"],
    mdb_results["tx2_insert"],
unit="us")}


### Index person_country

{create_markdown_metrics_table(
    sdb_results["q4_index"],
    mdb_results["q4_index"]
)}

### Index order_count

{create_markdown_metrics_table(
    sdb_results["q5_index"],
    mdb_results["q5_index"]
)}

### Index product_category

{create_markdown_metrics_table(
    sdb_results["q8_index"],
    mdb_results["q8_index"]
)}

### Index product_price

{create_markdown_metrics_table(
    sdb_results["q10_index"],
    mdb_results["q10_index"]
)}

### lookup vs record links

{create_markdown_metrics_table(
    sdb_results["q1"],
    mdb_results["q1"]
)}

### lookup vs graph - one connection

{create_markdown_metrics_table(
    sdb_results["q2"],
    mdb_results["q2"]
)}

### lookup vs graph (and link) - two connections

{create_markdown_metrics_table(
    sdb_results["q3"],
    mdb_results["q3"]
)}

### Projection with filter

{create_markdown_metrics_table(
    sdb_results["q4"],
    mdb_results["q4"]
)}

### Projection with order by

{create_markdown_metrics_table(
    sdb_results["q13"],
    mdb_results["q13"]
)}

### Count with filter

{create_markdown_metrics_table(
    sdb_results["q5"],
    mdb_results["q5"]
)}

### Count with relationship

{create_markdown_metrics_table(
    sdb_results["q6"],
    mdb_results["q6"]
)}

"""
print(report)

# Output the results
export_path = pathlib.Path(__file__).parents[1] / "output_files"

with open(export_path / pathlib.Path("Bench_report_output.md"), "w") as file:
    file.write(report)

### Total wall time
# {create_markdown_metrics_table(
#     sdb_results["wall_time_duration"],
#     mdb_results["wall_time_duration"]
# )}
