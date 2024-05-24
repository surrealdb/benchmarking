# import required modules
from uuid import uuid4
from time import perf_counter_ns

from surrealdb import SurrealDB

from bench_utils import table_definition, get_gen_uuid4_unique_list, insert_relate_statement

# Load the data

def sdb_insert_person(person_data, iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(f"INSERT INTO person {person_data}")

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def sdb_insert_product(product_data, iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(f"INSERT INTO product {product_data}")

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


def sdb_insert_order(order_data, iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        insert_relate_statement(order_data, db)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def sdb_insert_artist(artist_data, iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(f"INSERT INTO artist {artist_data}")

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


def sdb_insert_review(review_data, iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(f"INSERT INTO review {review_data}")

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

def sdb_q1(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        SELECT
            rating,
            review_text,
            review_date,
            person.name,
            person.email,
            person.phone,
            product.name,
            product.category,
            product.image_url
        FROM review;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q2: lookup vs graph - one connection

def sdb_q2(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query("""
        SELECT
            price,
            order_date,
            product_name,
            <-person.name,
            <-person.email,
            <-person.phone,
            ->product.category,
            ->product.description,
            ->product.image_url
        FROM order;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q2 variant: lookup vs graph - using in/out instead of arrow

def sdb_q2_variant(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        SELECT
            price,
            order_date,
            product_name,
            in.person.name,
            in.person.email,
            in.person.phone,
            out.category,
            out.description,
            out.image_url
        FROM order;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q3: lookup vs graph (and link) - two connections

def sdb_q3(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query("""
        SELECT
            price,
            order_date,
            product_name,
            ->product.category,
            ->product.description,
            ->product.image_url,
            ->product.artist.name,
            ->product.artist.email,
            ->product.artist.phone
        FROM order;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q4: Name and email for all customers in England

def sdb_q4_index(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        DEFINE INDEX person_country ON TABLE person COLUMNS address.country;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def sdb_q4(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        SELECT name, email 
        FROM person 
        WHERE address.country = "England";	
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q5: standard count

def sdb_q5_index(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        DEFINE INDEX order_count ON TABLE order COLUMNS order_status, order_date;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def sdb_q5(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        SELECT count() FROM order
        WHERE order_status IN ["delivered", "processing", "shipped"]
        AND order_date < "2023-04-01T00:00:00.000000Z"
        GROUP ALL;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q6: Count with relationship

def sdb_q6(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        SELECT count() FROM order
        WHERE order_status IN ["delivered", "processing", "shipped"]
        AND order_date < "2023-04-01T00:00:00.000000Z"
        AND ->product.artist.address.country ?= "England"
        GROUP ALL;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


### Q7: Delete a specific review

def sdb_q7(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    # setup
    review_ids_for_deletion = get_gen_uuid4_unique_list(total_num=table_definition['review_amount'], list_num=iterations, seed=50)

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(f""" 
        DELETE {str(review_ids_for_deletion.pop())}
        RETURN NONE;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q8: Delete reviews from a particular category

def sdb_q8_index(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        # TODO we currently only have index working on Select, coming later for Update/delete
        db.query(""" 
        DEFINE INDEX product_category ON TABLE review COLUMNS product.category;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

def sdb_q8(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        DELETE review
        WHERE product.category = "charcoal"
        RETURN NONE;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q9: Update a customer address

def sdb_q9(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    # setup
    person_ids_for_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=iterations, seed=10)

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(f"UPDATE {str(person_ids_for_update.pop())}"+"""
        SET address = {
            'address_line_1': '497 Ballycander',
            'address_line_2': None,
            'city': 'Bromyard',
            'country': 'Wales',
            'post_code': 'ZX8N 4VJ',
            'coordinates': [68.772592, -35.491877]
            }
        RETURN NONE;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q10: Update discounts for products

def sdb_q10_index(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        # TODO we currently only have index working on Select, coming later for Update/delete
        db.query(""" 
        DEFINE INDEX product_price ON TABLE product COLUMNS price;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list


def sdb_q10(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        UPDATE product
        SET discount = 0.2
        WHERE price < 1000
        RETURN NONE;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q11: Transaction - order from a new customer

def sdb_q11(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    # setup
    new_person_id = str(uuid4())

    product_ids_for_insert_and_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=10, seed=10)

    product_id = str(product_ids_for_insert_and_update.pop())

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        # Transaction - order from a new customer
        BEGIN TRANSACTION;
        -- insert into the person table
        CREATE person CONTENT {
            """+f"'id': {new_person_id},"+"""
            'first_name': 'Karyl',
            'last_name': 'Langley',
            'name': 'Karyl Langley',
            'company_name': None,
            'email': 'dee1961@gmail.com',
            'phone': '+44 47 3516 5895',
            'address': {
                'address_line_1': '510 Henalta',
                'address_line_2': None,
                'city': 'Lyme Regis',
                'country': 'Northern Ireland',
                'post_code': 'TO6Q 8CM',
                'coordinates': [-34.345071, 118.564172]
                }
            }
        RETURN NONE;

        -- relate into the order table
        """+f"RELATE {new_person_id} -> order:uuid() -> {product_id}"+"""
        CONTENT {
                "currency": "£",
                "discount": ->product.discount,
                "order_date": time::now(),
                "order_status": "pending",
                "payment_method": "PayPal",
                "price": ->product.price,
                "product_name": ->product.name,
                "quantity": 1,
                "shipping_address": <-person.address
            };

        -- update the product table to reduce the quantity"""+
        f"""
        UPDATE {product_id} SET quantity -= 1 RETURN NONE;
        COMMIT TRANSACTION;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q12: Transaction - New Artist creates their first product
# Transaction - New Artist creates their first product

def sdb_q12(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    # setup
    new_artist_id = str(uuid4())
    new_product_id = str(uuid4())

    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        BEGIN TRANSACTION;
        -- insert into the artist table
        CREATE artist CONTENT {"""+
                f"'id': 'artist:⟨{new_artist_id}⟩',"+"""
                'first_name': 'Anderson',
                'last_name': 'West',
                'name': 'Anderson West',
                'company_name': 'Atkins(ws) (ATK)',
                'email': 'six1933@gmail.com',
                'phone': '056 5881 1126',
                'address': {
                        'address_line_1': '639 Connaugh',
                        'address_line_2': None,
                        'city': 'Ripon',
                        'country': 'Scotland',
                        'post_code': 'CG3U 4TH',
                        'coordinates': [4.273648, -112.907273]
                        }
                }
        RETURN NONE;
        
        -- insert into the product table
        CREATE product CONTENT {"""+
                f"'id': 'product:⟨{new_product_id}⟩',"+"""
                'name': 'managed edt allocated pda',
                'description': 'counseling dildo greek pan works interest xhtml wrong dennis available cl specific next tower webcam peace magic',
                'category': 'watercolor',
                'price': 15735.96,
                'currency': '£',
                'discount': None,
                'quantity': 1,
                'image_url': 'https://source.unsplash.com/1920x1080?',"""
                f"'artist': 'artist:⟨{new_artist_id}⟩',"+"""
                "creation_history": {
                        "quantity": 1,
                        "created_at": time::now()
                        }       
                }
        RETURN NONE;
        COMMIT TRANSACTION;
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list

### Q13: Order by

def sdb_q13(iterations=1, db=SurrealDB("ws://localhost:8000/test/test")):
    """
    Run surrealdb query
    """
    result_list = []
    for _ in range(iterations):
        start_time = perf_counter_ns()

		# query to be run
        db.query(""" 
        SELECT name, email 
        FROM person
        ORDER BY name
        """)

        end_time = perf_counter_ns()
        duration = end_time - start_time
        result_list.append(duration)
    if len(result_list) < 2:
        return result_list[0]
    else:
        return result_list