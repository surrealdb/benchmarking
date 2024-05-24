import pathlib
import json

from tabulate import tabulate

from bench_utils import create_markdown_summary_table, create_markdown_metrics_table, table_definition

# create report

output_files_path = pathlib.Path(__file__).parents[0] / "output_files"

# Get SurrealDB results
with open(output_files_path / pathlib.Path("surrealdb_bench_output.json"), "r") as file:
    sdb_results = json.load(file)

# Get ArangoDB results
with open(output_files_path / pathlib.Path("arangodb_bench_output.json"), "r") as file:
    adb_results = json.load(file)

report = f"""
# Surreal bench

This benchmark compares SurrealDB and ArangoDB performance across a variety of CRUD queries.

It consists of the following tables:

{tabulate(
    {
        "Table name": table_definition.keys(),
        "Number of records": table_definition.values()
    }, headers="keys", tablefmt="github")}

### Overall results

{create_markdown_summary_table(
    sdb_results, 
    adb_results
)}

Total number of queries per run: {sdb_results["total_queries_count"][0]}

### Overall throughput

{create_markdown_metrics_table(
    sdb_results["total_throughput_qps"],
    adb_results["total_throughput_qps"]
, metric="throughput", unit="s")}

### Overall latency

{create_markdown_metrics_table(
    sdb_results["total_time_duration"], 
    adb_results["total_time_duration"]
)}

### Overall write latency

{create_markdown_metrics_table(
    sdb_results["total_write_duration"],
    adb_results["total_write_duration"]
)}

### Overall read latency

{create_markdown_metrics_table(
    sdb_results["total_read_duration"], 
    adb_results["total_read_duration"]
)}

## Results by category
Each category consists of 2 or more queries

### Write latency

#### Insert

{create_markdown_metrics_table(
    sdb_results["insert_duration"],
    adb_results["insert_duration"]
)}

#### Update

{create_markdown_metrics_table(
    sdb_results["update_duration"],
    adb_results["update_duration"]
)}

#### Delete

{create_markdown_metrics_table(
    sdb_results["delete_duration"],
    adb_results["delete_duration"]
)}

#### Index

{create_markdown_metrics_table(
    sdb_results["index_duration"],
    adb_results["index_duration"]
)}

#### Transaction

{create_markdown_metrics_table(
    sdb_results["transactions_duration"],
    adb_results["transactions_duration"]
)}

### Read latency

#### Read filter & order

{create_markdown_metrics_table(
    sdb_results["read_filter_duration"],
    adb_results["read_filter_duration"]
)}

#### Read relationships

{create_markdown_metrics_table(
    sdb_results["read_relationships_duration"],
    adb_results["read_relationships_duration"]
)}

#### Read aggregation

{create_markdown_metrics_table(
    sdb_results["read_aggregation_duration"],
    adb_results["read_aggregation_duration"]
)}

## Results by query

### Insert person

{create_markdown_metrics_table(
    sdb_results["insert_person"],
    adb_results["insert_person"]
)}

### Insert artist

{create_markdown_metrics_table(
    sdb_results["insert_artist"],
    adb_results["insert_artist"]
)}

### Insert product

{create_markdown_metrics_table(
    sdb_results["insert_product"],
    adb_results["insert_product"]
)}

### Insert order

{create_markdown_metrics_table(
    sdb_results["insert_order"],
    adb_results["insert_order"]
)}

### Insert review

{create_markdown_metrics_table(
    sdb_results["insert_review"],
    adb_results["insert_review"]
)}

### Update one

{create_markdown_metrics_table(
    sdb_results["update_one"],
    adb_results["update_one"],
unit="us")}

### Update many
**NOTE:** we currently only have an index working on Select, coming later for Update/delete
Therefore this query cannot make use of the index that was created for it.

{create_markdown_metrics_table(
    sdb_results["update_many"],
    adb_results["update_many"]
)}

### Delete one

{create_markdown_metrics_table(
    sdb_results["delete_one"],
    adb_results["delete_one"],
unit="us")}

### Delete many
**NOTE:** we currently only have an index working on Select, coming later for Update/delete
Therefore this query cannot make use of the index that was created for it.

{create_markdown_metrics_table(
    sdb_results["delete_many"],
    adb_results["delete_many"]
)}

### Transaction insert & update

{create_markdown_metrics_table(
    sdb_results["tx1_insert_update"],
    adb_results["tx1_insert_update"],
unit="us")}

### Transaction insert x 2

{create_markdown_metrics_table(
    sdb_results["tx2_insert"],
    adb_results["tx2_insert"],
unit="us")}


### Index person_country

{create_markdown_metrics_table(
    sdb_results["q4_index"],
    adb_results["q4_index"]
)}

### Index order_count

{create_markdown_metrics_table(
    sdb_results["q5_index"],
    adb_results["q5_index"]
)}

### Index product_category

{create_markdown_metrics_table(
    sdb_results["q8_index"],
    adb_results["q8_index"]
)}

### Index product_price

{create_markdown_metrics_table(
    sdb_results["q10_index"],
    adb_results["q10_index"]
)}

### lookup vs record links

{create_markdown_metrics_table(
    sdb_results["q1"],
    adb_results["q1"]
)}

### lookup vs graph - one connection

{create_markdown_metrics_table(
    sdb_results["q2"],
    adb_results["q2"]
)}

### lookup vs graph (and link) - two connections

{create_markdown_metrics_table(
    sdb_results["q3"],
    adb_results["q3"]
)}

### Projection with filter

{create_markdown_metrics_table(
    sdb_results["q4"],
    adb_results["q4"]
)}

### Projection with order by

{create_markdown_metrics_table(
    sdb_results["q13"],
    adb_results["q13"]
)}

### Count with filter

{create_markdown_metrics_table(
    sdb_results["q5"],
    adb_results["q5"]
)}

### Count with relationship

{create_markdown_metrics_table(
    sdb_results["q6"],
    adb_results["q6"]
)}

"""
print(report)

# Output the results
export_path = pathlib.Path(__file__).parents[0] / "output_files"

with open(export_path / pathlib.Path("Bench_report_output.md"), "w") as file:
    file.write(report)

### Total wall time
# {create_markdown_metrics_table(
#     sdb_results["wall_time_duration"],
#     adb_results["wall_time_duration"]
# )}
