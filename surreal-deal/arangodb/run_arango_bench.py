from time import perf_counter_ns
import pathlib
import json

from arango import ArangoClient

import define_arangodb_bench as adb

from bench_utils import format_time, throughput_calc

# Read the data

data_path = pathlib.Path(__file__).parents[0] / "output_files" / "arangodb_data"

with open(data_path / pathlib.Path("person_data.json"), "r") as read_file:
    person_data = json.load(read_file)

with open(data_path / pathlib.Path("artist_data.json"), "r") as read_file:
    artist_data = json.load(read_file)

with open(data_path / pathlib.Path("product_data.json"), "r") as read_file:
    product_data = json.load(read_file)

with open(data_path / pathlib.Path("order_data.json"), "r") as read_file:
    order_data = json.load(read_file)

with open(data_path / pathlib.Path("review_data.json"), "r") as read_file:
    review_data = json.load(read_file)

print("data loaded")

# run surrealdb bench

# ArangoDB connection settings
DB_NAME = "sureal-arango"
DB_URL = "http://localhost:8529"
DB_USER = "root"
DB_PASSWORD = "openSesame"

# Initialize ArangoDB client
client = ArangoClient(hosts=DB_URL)
sys_db = client.db('_system', username=DB_USER, password=DB_PASSWORD)

# # Create the database if it does not exist
# if not sys_db.has_database(DB_NAME):
#     sys_db.create_database(DB_NAME)

# Connect to the database
db = client.db(DB_NAME, username=DB_USER, password=DB_PASSWORD)

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
    # Create the database if it does not exist, otherwise delete
    if sys_db.has_database(DB_NAME):
        sys_db.delete_database(DB_NAME)
        sys_db.create_database(DB_NAME)
    else:
        sys_db.create_database(DB_NAME)

    print(f"Run #{run+1}")
    wall_time_start = perf_counter_ns()

    ## insert the data
    insert_start = perf_counter_ns()

    # Define collection and edge names
    collections = ["Persons", "Artists", "Products", "Orders", "Reviews"]
    edge_collections = ["PersonToOrder", "ProductToOrder", "ArtistToProduct", "PersonToReview", "ProductToReview", "ArtistToReview"]

    # Create collections
    for collection in collections:
        if db.has_collection(collection):
            db.delete_collection(collection)
            db.create_collection(collection)
        else:
            db.create_collection(collection)

    # Create edge collections
    for edge_collection in edge_collections:
        if db.has_collection(edge_collection):
            db.delete_collection(edge_collection)
            db.create_collection(edge_collection, edge=True)
        else:
            db.create_collection(edge_collection, edge=True)

    insert_person = adb.adb_insert_person(person_data, db=db)
    insert_artist = adb.adb_insert_artist(artist_data, db=db)
    insert_product = adb.adb_insert_product(product_data, db=db)
    insert_order = adb.adb_insert_order(order_data, db=db)
    insert_review = adb.adb_insert_review(review_data, db=db)
    insert_other = adb.adb_insert_other(order_data, product_data, review_data, db=db)

    insert_end = perf_counter_ns()
    insert_duration = insert_end - insert_start
    print("insert latency(ms):", format_time(insert_duration, unit="ms", precision=2, unit_label=True))

    ## create indexes
    index_start = perf_counter_ns()

    q4_index = adb.adb_q4_index(db=db)
    q5_index = adb.adb_q5_index(db=db)
    q8_index = adb.adb_q8_index(db=db)
    q10_index = adb.adb_q10_index(db=db)

    index_end = perf_counter_ns()
    index_duration = index_end - index_start
    print("index latency(ms):", format_time(index_duration, unit="ms", precision=2, unit_label=True))

    ## read
    read_filter_start = perf_counter_ns()

    ### filter & order
    q4 = adb.adb_q4(db=db)
    q13 = adb.adb_q13(db=db)

    read_filter_end = perf_counter_ns()
    read_filter_duration = read_filter_end - read_filter_start
    print("read filter & order latency(ms):", format_time(read_filter_duration, unit="ms", precision=2, unit_label=True))

    read_relationships_start = perf_counter_ns()

    ### relationships
    q1 = adb.adb_q1(db=db)
    q2 = adb.adb_q2(db=db)
    q3 = adb.adb_q3(db=db)

    read_relationships_end = perf_counter_ns()
    read_relationships_duration = read_relationships_end - read_relationships_start
    print("read relationships latency(ms):", format_time(read_relationships_duration, unit="ms", precision=2, unit_label=True))

    read_aggregation_start = perf_counter_ns()
    ### aggregation
    q5 = adb.adb_q5(db=db)
    q6 = adb.adb_q6(db=db)

    read_aggregation_end = perf_counter_ns()
    read_aggregation_duration = read_aggregation_end - read_aggregation_start
    print("read aggregation latency(ms):", format_time(read_aggregation_duration, unit="ms", precision=2, unit_label=True))

    ## update 
    update_start = perf_counter_ns()

    update_one = adb.adb_q9(db=db)
    update_many = adb.adb_q10(db=db)

    update_end = perf_counter_ns()
    update_duration = update_end - update_start
    print("update latency(ms):", format_time(update_duration, unit="ms", precision=2, unit_label=True))

    ## delete
    delete_start = perf_counter_ns()

    delete_one = adb.adb_q7(db=db)
    delete_many = adb.adb_q8(db=db)

    delete_end = perf_counter_ns()
    delete_duration = delete_end - delete_start
    print("delete latency(ms):", format_time(delete_duration, unit="ms", precision=2, unit_label=True))

    ## transactions
    transactions_start = perf_counter_ns()

    tx1_insert_update = adb.adb_q11(db=db)
    tx2_insert = adb.adb_q12(db=db)

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
export_path = pathlib.Path(__file__).parents[0] / "output_files"

with open(export_path / pathlib.Path("arangodb_bench_output.json"), "w") as file:
    json.dump(bench_run_output_list_combined, file, ensure_ascii=False)