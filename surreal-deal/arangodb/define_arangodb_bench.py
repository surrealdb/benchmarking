# import required modules
from random import Random
from uuid import uuid4
from time import perf_counter_ns

from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis import Datetime
from mimesis.enums import TimestampFormat

from arango import ArangoClient

from bench_utils import table_definition, generate_uuid4, get_gen_uuid4_unique_list, insert_relate_statement


# Load the data

def adb_insert_person(person_data, iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        persons_collection = db.collection('Persons')
        persons_collection.import_bulk(person_data)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def adb_insert_product(product_data, iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        products_collection = db.collection('Products')
        products_collection.import_bulk(product_data)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


def adb_insert_order(order_data, iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        orders_collection = db.collection('Orders')
        orders_collection.import_bulk(order_data)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def adb_insert_artist(artist_data, iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        artists_collection = db.collection('Artists')
        artists_collection.import_bulk(artist_data)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


def adb_insert_review(review_data, iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        reviews_collection = db.collection('Reviews')
        reviews_collection.import_bulk(review_data)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def adb_insert_other(orders, products, reviews, iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        person_to_order = db.collection('PersonToOrder')
        product_to_order = db.collection('ProductToOrder')
        artist_to_product = db.collection('ArtistToProduct')
        person_to_review = db.collection('PersonToReview')
        product_to_review = db.collection('ProductToReview')
        artist_to_review = db.collection('ArtistToReview')

        person_to_order_edges = [{'_from': f'Persons/{order["in"]}', '_to': f'Orders/{order["_key"]}'} for order in orders]
        product_to_order_edges = [{'_from': f'Products/{order["out"]}', '_to': f'Orders/{order["_key"]}'} for order in orders]
        artist_to_product_edges = [{'_from': f'Artists/{product["artist"]}', '_to': f'Products/{product["_key"]}'} for product in products]
        person_to_review_edges = [{'_from': f'Persons/{review["person"]}', '_to': f'Reviews/{review["_key"]}'} for review in reviews]
        product_to_review_edges = [{'_from': f'Products/{review["product"]}', '_to': f'Reviews/{review["_key"]}'} for review in reviews]
        artist_to_review_edges = [{'_from': f'Artists/{review["artist"]}', '_to': f'Reviews/{review["_key"]}'} for review in reviews]

        person_to_order.import_bulk(person_to_order_edges)
        product_to_order.import_bulk(product_to_order_edges)
        artist_to_product.import_bulk(artist_to_product_edges)
        person_to_review.import_bulk(person_to_review_edges)
        product_to_review.import_bulk(product_to_review_edges)
        artist_to_review.import_bulk(artist_to_review_edges)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

# Run the queries

## Q1-Q3 - comparing relationships, returning 3 fields from 3 tables (2x relationships)

### Q1: lookup vs record links

def adb_q1(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR review IN Reviews
            LET person = DOCUMENT(CONCAT('Persons/', review.person))
            LET product = DOCUMENT(CONCAT('Products/', review.product))
            RETURN {
                rating: review.rating,
                review_text: review.review_text,
                review_date: review.review_date,
                person_name: person.name,
                person_email: person.email,
                person_phone: person.phone,
                product_name: product.name,
                product_category: product.category,
                product_image_url: product.image_url
            }
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q2: lookup vs graph - one connection

def adb_q2(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute("""
        FOR order IN Orders
            LET person = FIRST(FOR v IN 1..1 INBOUND order._id PersonToOrder RETURN v)
            LET product = FIRST(FOR v IN 1..1 INBOUND order._id ProductToOrder RETURN v)
            RETURN {
                price: order.price,
                order_date: order.order_date,
                product_name: order.product_name,
                person_name: person.name,
                person_email: person.email,
                person_phone: person.phone,
                product_category: product.category,
                product_description: product.description,
                product_image_url: product.image_url
            }
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q2 variant: lookup vs graph - using in/out instead of arrow


### Q3: lookup vs graph (and link) - two connections

def adb_q3(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute("""
        FOR order IN Orders
            LET product = FIRST(FOR v IN 1..1 INBOUND order._id ProductToOrder RETURN v)
            LET artist = FIRST(FOR v IN 1..1 INBOUND product._id ArtistToProduct RETURN v)
            RETURN {
                price: order.price,
                order_date: order.order_date,
                product_name: order.product_name,
                product_category: product.category,
                product_description: product.description,
                product_image_url: product.image_url,
                artist_name: artist.name,
                artist_email: artist.email,
                artist_phone: artist.phone
            }
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q4: Name and email for all customers in England

def adb_q4_index(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        person_collection = db.collection("Persons")
        person_collection.add_persistent_index(fields=["address.country"])

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def adb_q4(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR person IN Persons
            FILTER person.address.country == "England"
            RETURN { name: person.name, email: person.email }
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q5: standard count

def adb_q5_index(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        order_collection = db.collection("Orders")
        order_collection.add_persistent_index(fields=["order_status", "order_date"])

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def adb_q5(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR order IN Orders
            FILTER order.order_status IN ["delivered", "processing", "shipped"]
            AND order.order_date < "2023-04-01T00:00:00.000Z"
            COLLECT WITH COUNT INTO length
            RETURN length
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q6: Count with relationship

def adb_q6(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR order IN Orders
            FILTER order.order_status IN ["delivered", "processing", "shipped"]
            AND order.order_date < "2024-04-01T00:00:00.000Z"
            LET product = FIRST(FOR v IN 1..1 INBOUND order._id ProductToOrder RETURN v)
            LET artist = FIRST(FOR v IN 1..1 INBOUND product._id ArtistToProduct RETURN v)
            FILTER artist.address.country == "England"
            COLLECT WITH COUNT INTO length
            RETURN length
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


### Q7: Delete a specific review

def adb_q7(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    # setup
    review_ids_for_deletion = get_gen_uuid4_unique_list(total_num=table_definition['review_amount'], list_num=iterations, seed=50)

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        bind_vars = {
            "key": str(review_ids_for_deletion.pop())
        }

        list(db.aql.execute(""" 
        REMOVE { _key: @key } IN Reviews
        """, bind_vars=bind_vars))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q8: Delete reviews from a particular category

def adb_q8_index(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        review_collection = db.collection("Reviews")
        review_collection.add_persistent_index(fields=["product.category"])

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def adb_q8(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR review IN Reviews
            LET product = DOCUMENT(CONCAT("Products/", review.product))
            FILTER product.category == "charcoal"
            REMOVE review IN Reviews
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q9: Update a customer address

def adb_q9(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    # setup
    person_ids_for_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=iterations, seed=10)

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        bind_vars = {
            "key": str(person_ids_for_update.pop())
        }


        list(db.aql.execute("""
        UPDATE { _key: @key } 
        WITH { 
            address: {
                address_line_1: "497 Ballycander",
                address_line_2: null,
                city: "Bromyard",
                country: "Wales",
                post_code: "ZX8N 4VJ",
                coordinates: [68.772592, -35.491877]
            } 
        } 
        IN Persons
        """, bind_vars=bind_vars))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q10: Update discounts for products

def adb_q10_index(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        product_collection = db.collection("Products")
        product_collection.add_persistent_index(fields=["price"])

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


def adb_q10(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR product IN Products
            LET numericPrice = TO_NUMBER(REGEX_REPLACE(product.price, '[^0-9.]', ''))
            FILTER numericPrice < 1000
            UPDATE product WITH { discount: 0.2 } IN Products
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q11: Transaction - order from a new customer
# TODO check that this actually does a transaction

def adb_q11(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    # setup
    new_person_id = str(uuid4())

    product_ids_for_insert_and_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=10, seed=10)

    product_id = str(product_ids_for_insert_and_update.pop())

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        bind_vars = {
            "person_key": new_person_id,
            "product_key": product_id,
        }

        list(db.aql.execute("""
        LET new_customer = {
            "_key": @person_key,
            "first_name": "Karyl",
            "last_name": "Langley",
            "name": "Karyl Langley",
            "company_name": null,
            "email": "dee1961@gmail.com",
            "phone": "+44 47 3516 5895",
            "address": {
                "address_line_1": "510 Henalta",
                "address_line_2": null,
                "city": "Lyme Regis",
                "country": "Northern Ireland",
                "post_code": "TO6Q 8CM",
                "coordinates": [-34.345071, 118.564172]
            }
        }

        LET insert_customer = (
            INSERT new_customer INTO Persons
            RETURN NEW
        )

        LET new_order = {
            "_from": CONCAT("Persons/", new_customer._key),
            "_to": CONCAT("Products/", @product_key),
            "currency": "£",
            "discount": 0.1,
            "order_date": DATE_NOW(),
            "order_status": "pending",
            "payment_method": "PayPal",
            "price": 1000,
            "product_name": "Sample Product",
            "quantity": 1,
            "shipping_address": new_customer.address
        }

        LET insert_order = (
            INSERT new_order INTO Orders
            RETURN NEW
        )

        LET update_product = (
            FOR product IN Products
                FILTER product._key == "@product_key"
                UPDATE product WITH { "quantity": product.quantity - 1 } IN Products
        )

        RETURN {
            customer: insert_customer,
            order: insert_order,
            product_update: update_product
        }
        """, bind_vars=bind_vars))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q12: Transaction - New Artist creates their first product
# TODO check that this actually does a transaction

def adb_q12(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    # setup
    new_artist_id = str(uuid4())
    new_product_id = str(uuid4())

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        bind_vars = {
            "artist_key": new_artist_id,
            "product_key": new_product_id,
        }

        list(db.aql.execute("""
        LET new_artist = {
            "_key": @artist_key,
            "first_name": "Anderson",
            "last_name": "West",
            "name": "Anderson West",
            "company_name": "Atkins(ws) (ATK)",
            "email": "six1933@gmail.com",
            "phone": "056 5881 1126",
            "address": {
                "address_line_1": "639 Connaugh",
                "address_line_2": null,
                "city": "Ripon",
                "country": "Scotland",
                "post_code": "CG3U 4TH",
                "coordinates": [4.273648, -112.907273]
            }
        }

        LET insert_artist = (
            INSERT new_artist INTO Artists
            RETURN NEW
        )

        LET new_product = {
            "_key": @product_key,
            "name": "managed edt allocated pda",
            "description": "counseling dildo greek pan works interest xhtml wrong dennis available cl specific next tower webcam peace magic",
            "category": "watercolor",
            "price": 15735.96,
            "currency": "£",
            "discount": null,
            "quantity": 1,
            "image_url": "https://source.unsplash.com/1920x1080",
            "artist": CONCAT("Artists/", new_artist._key),
            "creation_history": {
                "quantity": 1,
                "created_at": DATE_NOW()
            }
        }

        LET insert_product = (
            INSERT new_product INTO Products
            RETURN NEW
        )

        RETURN {
            artist: insert_artist,
            product: insert_product
        }
        """, bind_vars=bind_vars))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q13: Order by

def adb_q13(iterations=1, db=ArangoClient(hosts="http://localhost:8529")):
    """
    Run arangodb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        list(db.aql.execute(""" 
        FOR person IN Persons
            SORT person.name
            RETURN { name: person.name, email: person.email }
        """))

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list