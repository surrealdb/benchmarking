from time import perf_counter_ns

from pymongo import MongoClient

import mongodb_bench as mdb

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

# in case it exists drop the database
connection.drop_database('surreal_bench')

iterations_count = 1

total_time_start = perf_counter_ns()

## insert the data
insert_start = perf_counter_ns()

insert_person = mdb.mdb_insert_person(person_data)
insert_artist = mdb.mdb_insert_artist(artist_data)
insert_product = mdb.mdb_insert_product(product_data)
insert_order = mdb.mdb_insert_order(order_data)
insert_review = mdb.mdb_insert_review(review_data)

insert_end = perf_counter_ns()
insert_duration = insert_end - insert_start
print("insert duration(ms):", format_time(insert_duration,unit="ms", precision=2))

## create indexes
index_start = perf_counter_ns()

q4_index = mdb.mdb_q4_index(client=connection)
q5_index = mdb.mdb_q5_index(client=connection)
q8_index = mdb.mdb_q8_index(client=connection)
q10_index = mdb.mdb_q10_index(client=connection)

index_end = perf_counter_ns()
index_duration = index_end - index_start
print("index duration(ms):", format_time(index_duration, unit="ms", precision=2))

## read
read_start = perf_counter_ns()

q1 = mdb.mdb_q1(iterations=iterations_count, client=connection)
q2 = mdb.mdb_q2(iterations=iterations_count, client=connection)
q3 = mdb.mdb_q3(iterations=iterations_count, client=connection)
q4 = mdb.mdb_q4(iterations=iterations_count, client=connection)
q5 = mdb.mdb_q5(iterations=iterations_count, client=connection)
q6 = mdb.mdb_q6(iterations=iterations_count, client=connection)

read_end = perf_counter_ns()
read_duration = read_end - read_start
print("read duration(ms):", format_time(read_duration, unit="ms", precision=2))

## update 
update_start = perf_counter_ns()

update_one = mdb.mdb_q9(iterations=iterations_count, client=connection)
update_many = mdb.mdb_q10(client=connection)

update_end = perf_counter_ns()
update_duration = update_end - update_start
print("update duration(ms):", format_time(update_duration, unit="ms", precision=2))

## delete
delete_start = perf_counter_ns()

delete_one = mdb.mdb_q7(iterations=iterations_count, client=connection)
delete_many = mdb.mdb_q8(client=connection)

delete_end = perf_counter_ns()
delete_duration = delete_end - delete_start
print("delete duration(ms):", format_time(delete_duration, unit="ms", precision=2))


## transactions
transactions_start = perf_counter_ns()

tx1_insert_update = mdb.mdb_q11(client=connection)
tx2_insert = mdb.mdb_q12(client=connection)

transactions_end = perf_counter_ns()
transactions_duration = transactions_end - transactions_start
print("transaction duration(ms):", format_time(transactions_duration, unit="ms", precision=2))

total_time_end = perf_counter_ns()

total_time_duration = total_time_end - total_time_start
print("total duration(ms):", format_time(total_time_duration, unit="ms", precision=2))

insert_query_count = 5
index_query_count = 4
read_query_count = 6 * iterations_count
update_query_count = 2 * iterations_count
delete_query_count = 2 * iterations_count
transactions_count = 2 * iterations_count

total_queries_count = insert_query_count + index_query_count + read_query_count + update_query_count + delete_query_count + transactions_count
print(total_queries_count)
print(format_time(total_time_duration, unit="s", unit_label=False, precision=2))
throughput_qps = total_queries_count / format_time(total_time_duration, unit="s", precision=2, unit_label=False)

print(throughput_qps)

# create report

# first step just get the data in a JSON output

# secornd step could be to make a report with both outputs

## insert

insert_person
insert_artist
insert_product
insert_order
insert_review

insert_duration

## index
q4_index
q5_index
q8_index
q10_index

index_duration

## read
q1
q2
q3
q4
q5
q6

read_duration

## update
update_one
update_many

update_duration

## delete
delete_one
delete_many

delete_duration

## transactions
tx1_insert_update
tx2_insert

transactions_duration

total_time_duration