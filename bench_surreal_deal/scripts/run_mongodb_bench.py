from time import perf_counter_ns
import pathlib
import json

from pymongo import MongoClient

import define_mongodb_bench as mdb

from bench_utils import format_time

# create the data

## mongodb
person_data = mdb.mdb_generate_person_data()
artist_data = mdb.mdb_generate_artist_data()
product_data = mdb.mdb_generate_product_data(artist_data)
order_data = mdb.mdb_generate_order_data(person_data, product_data)
review_data = mdb.mdb_generate_review_data(person_data, product_data, artist_data)

# run mongodb bench

MONGODB_URI = "mongodb://localhost:27017/"

connection = MongoClient(MONGODB_URI, uuidRepresentation='standard')

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
    connection.drop_database('surreal_bench')

    print(f"Run #{run+1}")
    wall_time_start = perf_counter_ns()

    ## insert the data
    insert_start = perf_counter_ns()

    insert_person = mdb.mdb_insert_person(person_data)
    insert_artist = mdb.mdb_insert_artist(artist_data)
    insert_product = mdb.mdb_insert_product(product_data)
    insert_order = mdb.mdb_insert_order(order_data)
    insert_review = mdb.mdb_insert_review(review_data)

    insert_end = perf_counter_ns()
    insert_duration = insert_end - insert_start
    print("insert latency(ms):", format_time(insert_duration, unit="ms", precision=2, unit_label=True))

    ## create indexes
    index_start = perf_counter_ns()

    q4_index = mdb.mdb_q4_index(client=connection)
    q5_index = mdb.mdb_q5_index(client=connection)
    q8_index = mdb.mdb_q8_index(client=connection)
    q10_index = mdb.mdb_q10_index(client=connection)

    index_end = perf_counter_ns()
    index_duration = index_end - index_start
    print("index latency(ms):", format_time(index_duration, unit="ms", precision=2, unit_label=True))

    ## read
    read_filter_start = perf_counter_ns()

    ### filter & order
    q4 = mdb.mdb_q4(client=connection)
    q13 = mdb.mdb_q13(client=connection)

    read_filter_end = perf_counter_ns()
    read_filter_duration = read_filter_end - read_filter_start
    print("read filter & order latency(ms):", format_time(read_filter_duration, unit="ms", precision=2, unit_label=True))

    read_relationships_start = perf_counter_ns()

    ### relationships
    q1 = mdb.mdb_q1(client=connection)
    q2 = mdb.mdb_q2(client=connection)
    q3 = mdb.mdb_q3(client=connection)

    read_relationships_end = perf_counter_ns()
    read_relationships_duration = read_relationships_end - read_relationships_start
    print("read relationships latency(ms):", format_time(read_relationships_duration, unit="ms", precision=2, unit_label=True))

    read_aggregation_start = perf_counter_ns()
    ### aggregation
    q5 = mdb.mdb_q5(client=connection)
    q6 = mdb.mdb_q6(client=connection)

    read_aggregation_end = perf_counter_ns()
    read_aggregation_duration = read_aggregation_end - read_aggregation_start
    print("read aggregation latency(ms):", format_time(read_aggregation_duration, unit="ms", precision=2, unit_label=True))

    ## update 
    update_start = perf_counter_ns()

    update_one = mdb.mdb_q9(client=connection)
    update_many = mdb.mdb_q10(client=connection)

    update_end = perf_counter_ns()
    update_duration = update_end - update_start
    print("update latency(ms):", format_time(update_duration, unit="ms", precision=2, unit_label=True))

    ## delete
    delete_start = perf_counter_ns()

    delete_one = mdb.mdb_q7(client=connection)
    delete_many = mdb.mdb_q8(client=connection)

    delete_end = perf_counter_ns()
    delete_duration = delete_end - delete_start
    print("delete latency(ms):", format_time(delete_duration, unit="ms", precision=2, unit_label=True))

    ## transactions
    transactions_start = perf_counter_ns()

    tx1_insert_update = mdb.mdb_q11(client=connection)
    tx2_insert = mdb.mdb_q12(client=connection)

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
    total_throughput_qps = total_queries_count / format_time(total_time_duration, unit="s", precision=2, unit_label=False)
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

with open(export_path / pathlib.Path("mongodb_bench_output.json"), "w") as file:
    json.dump(bench_run_output_list_combined, file, ensure_ascii=False)