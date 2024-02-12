from time import perf_counter_ns
import pathlib
import json

from surrealdb import SurrealDB

import define_surrealdb_bench as sdb

from bench_utils import format_time, throughput_calc

# create the data

## surrealdb
person_data = sdb.sdb_generate_person_data()
artist_data = sdb.sdb_generate_artist_data()
product_data = sdb.sdb_generate_product_data(artist_data)
order_data = sdb.sdb_generate_order_data(person_data, product_data)
review_data = sdb.sdb_generate_review_data(person_data, product_data, artist_data)

# run surrealdb bench

SURREALDB_URI = "ws://localhost:8000/test/test"

db = SurrealDB(SURREALDB_URI)

db.signin({
    "username": "root",
    "password": "root",
})

runs = 10

# currently not needed but might be useful later
# bench_run_output_list_each = []

bench_run_output_list_combined = {
        "insert_person": [],
        "insert_artist": [],
        "insert_product": [],
        "insert_order": [],
        "insert_review": [],
        "insert_duration": [],
        "insert_query_count": [],
        "q4_index": [],
        "q5_index": [],
        "q8_index": [],
        "q10_index": [],
        "index_duration": [],
        "index_query_count": [],
        "q1": [],
        "q2": [],
        "q3": [],
        "q4": [],
        "q5": [],
        "q6": [],
        "q13": [],
        "read_filter_duration": [],
        "read_relationships_duration": [],
        "read_aggregation_duration": [],
        "read_query_count": [],
        "update_one": [],
        "update_many": [],
        "update_duration": [],
        "update_query_count":[],
        "delete_one": [],
        "delete_many": [],
        "delete_duration": [],
        "delete_query_count": [],
        "tx1_insert_update": [],
        "tx2_insert": [],
        "transactions_duration": [],
        "transactions_count": [],
        "total_read_duration": [],
        "total_write_duration": [],
        "total_time_duration": [],
        "total_queries_count": [],
        "total_throughput_qps": [],
        "wall_time_duration": []
        }

for run in range(runs):
    # in case it exists drop the database
    db.query("REMOVE DATABASE test")

    print(f"Run #{run+1}")
    wall_time_start = perf_counter_ns()

    ## insert the data
    insert_start = perf_counter_ns()

    insert_person = sdb.sdb_insert_person(person_data)
    insert_artist = sdb.sdb_insert_artist(artist_data)
    insert_product = sdb.sdb_insert_product(product_data)
    insert_order = sdb.sdb_insert_order(order_data)
    insert_review = sdb.sdb_insert_review(review_data)

    insert_end = perf_counter_ns()
    insert_duration = insert_end - insert_start
    print("insert latency(ms):", format_time(insert_duration, unit="ms", precision=2, unit_label=True))

    ## create indexes
    index_start = perf_counter_ns()

    q4_index = sdb.sdb_q4_index(db=db)
    q5_index = sdb.sdb_q5_index(db=db)
    q8_index = sdb.sdb_q8_index(db=db)
    q10_index = sdb.sdb_q10_index(db=db)

    index_end = perf_counter_ns()
    index_duration = index_end - index_start
    print("index latency(ms):", format_time(index_duration, unit="ms", precision=2, unit_label=True))

    ## read
    read_filter_start = perf_counter_ns()

    ### filter & order
    q4 = sdb.sdb_q4(db=db)
    q13 = sdb.sdb_q13(db=db)

    read_filter_end = perf_counter_ns()
    read_filter_duration = read_filter_end - read_filter_start
    print("read filter & order latency(ms):", format_time(read_filter_duration, unit="ms", precision=2, unit_label=True))

    read_relationships_start = perf_counter_ns()

    ### relationships
    q1 = sdb.sdb_q1(db=db)
    q2 = sdb.sdb_q2(db=db)
    q3 = sdb.sdb_q3(db=db)

    read_relationships_end = perf_counter_ns()
    read_relationships_duration = read_relationships_end - read_relationships_start
    print("read relationships latency(ms):", format_time(read_relationships_duration, unit="ms", precision=2, unit_label=True))

    read_aggregation_start = perf_counter_ns()
    ### aggregation
    q5 = sdb.sdb_q5(db=db)
    q6 = sdb.sdb_q6(db=db)

    read_aggregation_end = perf_counter_ns()
    read_aggregation_duration = read_aggregation_end - read_aggregation_start
    print("read aggregation latency(ms):", format_time(read_aggregation_duration, unit="ms", precision=2, unit_label=True))

    ## update 
    update_start = perf_counter_ns()

    update_one = sdb.sdb_q9(db=db)
    update_many = sdb.sdb_q10(db=db)

    update_end = perf_counter_ns()
    update_duration = update_end - update_start
    print("update latency(ms):", format_time(update_duration, unit="ms", precision=2, unit_label=True))

    ## delete
    delete_start = perf_counter_ns()

    delete_one = sdb.sdb_q7(db=db)
    delete_many = sdb.sdb_q8(db=db)

    delete_end = perf_counter_ns()
    delete_duration = delete_end - delete_start
    print("delete latency(ms):", format_time(delete_duration, unit="ms", precision=2, unit_label=True))

    ## transactions
    transactions_start = perf_counter_ns()

    tx1_insert_update = sdb.sdb_q11(db=db)
    tx2_insert = sdb.sdb_q12(db=db)

    transactions_end = perf_counter_ns()
    transactions_duration = transactions_end - transactions_start
    print("transaction latency(ms):", format_time(transactions_duration, unit="ms", precision=2, unit_label=True))

    wall_time_end = perf_counter_ns()

    wall_time_duration = wall_time_end - wall_time_start

    total_read_duration = read_filter_duration + read_relationships_duration + read_aggregation_duration
    total_write_duration = insert_duration + index_duration  + update_duration + delete_duration + transactions_duration
    total_time_duration = total_read_duration + total_write_duration
    print("total read latency(ms):", format_time(total_read_duration, unit="ms", precision=2, unit_label=True))
    print("total write latency(ms):", format_time(total_write_duration, unit="ms", precision=2, unit_label=True))
    print("total latency(ms):", format_time(total_time_duration, unit="ms", precision=2, unit_label=True))

    insert_query_count = 5
    index_query_count = 4
    read_query_count = 6
    update_query_count = 2
    delete_query_count = 2
    transactions_count = 2

    total_queries_count = insert_query_count + index_query_count + read_query_count + update_query_count + delete_query_count + transactions_count
    total_throughput_qps = throughput_calc(total_queries_count, total_time_duration)
    print("throughput(qps): ", round(total_throughput_qps, 1))

    # create JSON output

    bench_run_output_list_combined["insert_person"].append(insert_person)
    bench_run_output_list_combined["insert_artist"].append(insert_artist)
    bench_run_output_list_combined["insert_product"].append(insert_product)
    bench_run_output_list_combined["insert_order"].append(insert_order)
    bench_run_output_list_combined["insert_review"].append(insert_review)
    bench_run_output_list_combined["insert_duration"].append(insert_duration)
    bench_run_output_list_combined["insert_query_count"].append(insert_query_count)
    bench_run_output_list_combined["q4_index"].append(q4_index)
    bench_run_output_list_combined["q5_index"].append(q5_index)
    bench_run_output_list_combined["q8_index"].append(q8_index)
    bench_run_output_list_combined["q10_index"].append(q10_index)
    bench_run_output_list_combined["index_duration"].append(index_duration)
    bench_run_output_list_combined["index_query_count"].append(index_query_count)
    bench_run_output_list_combined["q1"].append(q1)
    bench_run_output_list_combined["q2"].append(q2)
    bench_run_output_list_combined["q3"].append(q3)
    bench_run_output_list_combined["q4"].append(q4)
    bench_run_output_list_combined["q5"].append(q5)
    bench_run_output_list_combined["q6"].append(q6)
    bench_run_output_list_combined["q13"].append(q13)
    bench_run_output_list_combined["read_filter_duration"].append(read_filter_duration)
    bench_run_output_list_combined["read_relationships_duration"].append(read_relationships_duration)
    bench_run_output_list_combined["read_aggregation_duration"].append(read_aggregation_duration)
    bench_run_output_list_combined["read_query_count"].append(read_query_count)
    bench_run_output_list_combined["update_one"].append(update_one)
    bench_run_output_list_combined["update_many"].append(update_many)
    bench_run_output_list_combined["update_duration"].append(update_duration)
    bench_run_output_list_combined["update_query_count"].append(update_query_count)
    bench_run_output_list_combined["delete_one"].append(delete_one)
    bench_run_output_list_combined["delete_many"].append(delete_many)
    bench_run_output_list_combined["delete_duration"].append(delete_duration)
    bench_run_output_list_combined["delete_query_count"].append(delete_query_count)
    bench_run_output_list_combined["tx1_insert_update"].append(tx1_insert_update)
    bench_run_output_list_combined["tx2_insert"].append(tx2_insert)
    bench_run_output_list_combined["transactions_duration"].append(transactions_duration)
    bench_run_output_list_combined["transactions_count"].append(transactions_count)
    bench_run_output_list_combined["total_read_duration"].append(total_read_duration)
    bench_run_output_list_combined["total_write_duration"].append(total_write_duration)
    bench_run_output_list_combined["total_time_duration"].append(total_time_duration)
    bench_run_output_list_combined["total_queries_count"].append(total_queries_count)
    bench_run_output_list_combined["total_throughput_qps"].append(total_throughput_qps)
    bench_run_output_list_combined["wall_time_duration"].append(wall_time_duration)

    # currently not needed but might be useful later
    # bench_run_record = {
    #     "run_number": run+1,  
    #     "insert_person": insert_person,
    #     "insert_artist": insert_artist,
    #     "insert_product": insert_product,
    #     "insert_order": insert_order,
    #     "insert_review": insert_review,
    #     "insert_duration": insert_duration,
    #     "insert_query_count": insert_query_count,
    #     "q4_index": q4_index,
    #     "q5_index": q5_index,
    #     "q8_index": q8_index,
    #     "q10_index": q10_index,
    #     "index_duration": index_duration,
    #     "index_query_count": index_query_count,
    #     "q1": q1,
    #     "q2": q2,
    #     "q3": q3,
    #     "q4": q4,
    #     "q5": q5,
    #     "q6": q6,
    #     "read_filter_duration": read_filter_duration,
    #     "read_relationships_duration": read_relationships_duration,
    #     "read_aggregation_duration": read_aggregation_duration,
    #     "read_query_count": read_query_count,
    #     "update_one": update_one,
    #     "update_many": update_many,
    #     "update_duration": update_duration,
    #     "update_query_count": update_query_count,
    #     "delete_one": delete_one,
    #     "delete_many": delete_many,
    #     "delete_duration": delete_duration,
    #     "delete_query_count": delete_query_count,
    #     "tx1_insert_update": tx1_insert_update,
    #     "tx2_insert": tx2_insert,
    #     "transactions_duration": transactions_duration,
    #     "transactions_count": transactions_count,
    #     "total_read_duration": total_read_duration,
    #     "total_write_duration": total_write_duration,
    #     "total_time_duration": total_time_duration,
    #     "total_queries_count": total_queries_count,
    #     "total_throughput_qps": total_throughput_qps,
    #     "wall_time_duration": wall_time_duration
    #     }
    # bench_run_output_list_each.append(bench_run_record)

# Output the results
export_path = pathlib.Path(__file__).parents[1] / "output_files"

with open(export_path / pathlib.Path("surrealdb_bench_output.json"), "w") as file:
    json.dump(bench_run_output_list_combined, file, ensure_ascii=False)