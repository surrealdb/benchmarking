from time import perf_counter_ns

from pymongo import MongoClient

import mongodb_bench as mdb

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


## insert the data
insert_start = perf_counter_ns()

insert_person = mdb.mdb_insert_person(person_data)
insert_artist = mdb.mdb_insert_artist(artist_data)
insert_product = mdb.mdb_insert_product(product_data)
insert_order = mdb.mdb_insert_order(order_data)
insert_review = mdb.mdb_insert_review(review_data)

insert_end = perf_counter_ns()
insert_duration = insert_end - insert_start


## create indexes
index_start = perf_counter_ns()

q4_index = mdb.mdb_q4_index(client=connection)
q5_index = mdb.mdb_q5_index(client=connection)
q8_index = mdb.mdb_q8_index(client=connection)
q10_index = mdb.mdb_q10_index(client=connection)

index_end = perf_counter_ns()
index_duration = index_end - index_start


## read
read_start = perf_counter_ns()

q1 = mdb.mdb_q1(iterations=1, client=connection)
q2 = mdb.mdb_q2(iterations=1, client=connection)
q3 = mdb.mdb_q3(iterations=1, client=connection)
q4 = mdb.mdb_q4(iterations=1, client=connection)
q5 = mdb.mdb_q5(iterations=1, client=connection)
q6 = mdb.mdb_q6(iterations=1, client=connection)

read_end = perf_counter_ns()
read_duration = read_end - read_start


## update 
update_start = perf_counter_ns()

update_one = mdb.mdb_q9(iterations=1, client=connection)
update_many = mdb.mdb_q10(iterations=1, client=connection)

update_end = perf_counter_ns()
update_duration = update_end - update_start


## delete
delete_start = perf_counter_ns()

delete_one = mdb.mdb_q7(iterations=1, client=connection)
delete_many = mdb.mdb_q8(iterations=1, client=connection)

delete_end = perf_counter_ns()
delete_duration = delete_end - delete_start


## transactions
transactions_start = perf_counter_ns()

tx1_insert_update = mdb.mdb_q11(iterations=1, client=connection)
tx2_insert = mdb.mdb_q12(iterations=1, client=connection)

transactions_end = perf_counter_ns()
transactions_duration = transactions_end - transactions_start

