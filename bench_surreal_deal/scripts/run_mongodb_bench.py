from time import perf_counter_ns
import pathlib
import json

from pymongo import MongoClient

import mongodb_bench as mdb

from bench_utils import format_time, throughput_calc

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

bench_run_output_list = []

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
    print("insert latency(ms):", format_time(insert_duration,unit="ms", precision=2))

    ## create indexes
    index_start = perf_counter_ns()

    q4_index = mdb.mdb_q4_index(client=connection)
    q5_index = mdb.mdb_q5_index(client=connection)
    q8_index = mdb.mdb_q8_index(client=connection)
    q10_index = mdb.mdb_q10_index(client=connection)

    index_end = perf_counter_ns()
    index_duration = index_end - index_start
    print("index latency(ms):", format_time(index_duration, unit="ms", precision=2))

    ## read
    read_filter_start = perf_counter_ns()

    ### filter
    q4 = mdb.mdb_q4(client=connection)

    read_filter_end = perf_counter_ns()
    read_filter_duration = read_filter_end - read_filter_start
    print("read filter latency(ms):", format_time(read_filter_duration, unit="ms", precision=2))

    read_relationships_start = perf_counter_ns()

    ### relationships
    q1 = mdb.mdb_q1(client=connection)
    q2 = mdb.mdb_q2(client=connection)
    q3 = mdb.mdb_q3(client=connection)

    read_relationships_end = perf_counter_ns()
    read_relationships_duration = read_relationships_end - read_relationships_start
    print("read relationships latency(ms):", format_time(read_relationships_duration, unit="ms", precision=2))

    read_aggregation_start = perf_counter_ns()
    ### aggregation
    q5 = mdb.mdb_q5(client=connection)
    q6 = mdb.mdb_q6(client=connection)

    read_aggregation_end = perf_counter_ns()
    read_aggregation_duration = read_aggregation_end - read_aggregation_start
    print("read aggregation latency(ms):", format_time(read_aggregation_duration, unit="ms", precision=2))

    ## update 
    update_start = perf_counter_ns()

    update_one = mdb.mdb_q9(client=connection)
    update_many = mdb.mdb_q10(client=connection)

    update_end = perf_counter_ns()
    update_duration = update_end - update_start
    print("update latency(ms):", format_time(update_duration, unit="ms", precision=2))

    ## delete
    delete_start = perf_counter_ns()

    delete_one = mdb.mdb_q7(client=connection)
    delete_many = mdb.mdb_q8(client=connection)

    delete_end = perf_counter_ns()
    delete_duration = delete_end - delete_start
    print("delete latency(ms):", format_time(delete_duration, unit="ms", precision=2))

    ## transactions
    transactions_start = perf_counter_ns()

    tx1_insert_update = mdb.mdb_q11(client=connection)
    tx2_insert = mdb.mdb_q12(client=connection)

    transactions_end = perf_counter_ns()
    transactions_duration = transactions_end - transactions_start
    print("transaction latency(ms):", format_time(transactions_duration, unit="ms", precision=2))

    wall_time_end = perf_counter_ns()

    wall_time_duration = wall_time_end - wall_time_start

    total_read_duration = read_filter_duration + read_relationships_duration + read_aggregation_duration
    total_write_duration = insert_duration + index_duration  + update_duration + delete_duration + transactions_duration
    total_time_duration = total_read_duration + total_write_duration
    print("total read latency(ms):", format_time(total_read_duration, unit="ms", precision=2))
    print("total write latency(ms):", format_time(total_write_duration, unit="ms", precision=2))
    print("total latency(ms):", format_time(total_time_duration, unit="ms", precision=2))

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

    bench_run_record = {
        "insert_person": insert_person,
        "insert_artist": insert_artist,
        "insert_product": insert_product,
        "insert_order": insert_order,
        "insert_review": insert_review,
        "insert_duration": insert_duration,
        "insert_query_count": insert_query_count,
        "q4_index": q4_index,
        "q5_index": q5_index,
        "q8_index": q8_index,
        "q10_index": q10_index,
        "index_duration": index_duration,
        "index_query_count": index_query_count,
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "q4": q4,
        "q5": q5,
        "q6": q6,
        "read_filter_duration": read_filter_duration,
        "read_relationships_duration": read_relationships_duration,
        "read_aggregation_duration": read_aggregation_duration,
        "read_query_count": read_query_count,
        "update_one": update_one,
        "update_many": update_many,
        "update_duration": update_duration,
        "update_query_count": update_query_count,
        "delete_one": delete_one,
        "delete_many": delete_many,
        "delete_duration": delete_duration,
        "delete_query_count": delete_query_count,
        "tx1_insert_update": tx1_insert_update,
        "tx2_insert": tx2_insert,
        "transactions_duration": transactions_duration,
        "transactions_count": transactions_count,
        "total_read_duration": total_read_duration,
        "total_write_duration": total_write_duration,
        "total_time_duration": total_time_duration,
        "total_queries_count": total_queries_count,
        "total_throughput_qps": total_throughput_qps,
        "wall_time_duration": wall_time_duration
        }

    bench_run_output_list.append(bench_run_record)

# Output the results
export_path = pathlib.Path(__file__).parents[0]

with open(export_path / pathlib.Path("output.json"), "w") as write_file:
    json.dump(bench_run_output_list, write_file, ensure_ascii=False)