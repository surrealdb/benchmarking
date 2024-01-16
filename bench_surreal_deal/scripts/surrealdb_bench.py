## Create the data
# %%
# import required modules
from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis.enums import TimestampFormat
from mimesis import Datetime

from random import Random
from uuid import UUID, uuid4

from bench_utils import generate_uuid4, get_gen_uuid4_unique_list, table_definition, insert_relate_statement

# Create table data

## person

person_id = generate_uuid4(table_definition['person_amount'], seed=10)

f = Field(locale=Locale.EN_GB, seed=10)

def person_generator() -> dict:
    first_name = f('first_name')
    last_name = f('last_name')
    return {
        "id":"person:"+f"⟨{str(next(person_id))}⟩",
        "first_name":first_name,
        "last_name":last_name,
        "name":first_name + " " + last_name,
        "company_name":f("company", key=maybe(None, probability=0.9)),
        "email":f("email"),
        "phone":f("phone_number"),
        "address":{
            "address_line_1":f("street_number")+" "+f("street_name"),
            "address_line_2":f('choice', items=['apt. 10','Suite. 23'], key=maybe(None, probability=0.9)),
            "city":f("city"),
            "country":f('choice', items=['England','Scotland', 'Wales', 'Northern Ireland']),
            "post_code":f("postal_code"),
            "coordinates":[f('latitude'), f('longitude')]
        }
    }

person_schema = Schema(
    schema=person_generator,
    iterations=table_definition['person_amount']
)
person_data = person_schema.create()

person_id_count = range(table_definition['person_amount']-1)

print("person data created")

## artist

artist_id = generate_uuid4(table_definition['artist_amount'], seed=20)

f = Field(locale=Locale.EN_GB, seed=20)

def artist_generator() -> dict:
    first_name = f("first_name")
    last_name = f("last_name")
    return {
        "id":"artist:"+f"⟨{str(next(artist_id))}⟩",
        "first_name":first_name,
        "last_name":last_name,
        "name":first_name + " " + last_name,
        "company_name":f("company", key=maybe(None, probability=0.5)),
        "email":f("email"),
        "phone":f("phone_number"),
        "address":{
            "address_line_1":f("street_number")+" "+f("street_name"),
            "address_line_2":f('choice', items=['apt. 10','Suite. 23'], key=maybe(None, probability=0.9)),
            "city":f("city"),
            "country":f('choice', items=['England','Scotland', 'Wales', 'Northern Ireland']),
            "post_code":f("postal_code"),
            "coordinates":[f('latitude'), f('longitude')]
        }
    }


artist_schema = Schema(
    schema=artist_generator,
    iterations=table_definition['artist_amount']
)

artist_data = artist_schema.create()

artist_id_count = range(table_definition['artist_amount']-1)

print("artist data created")

## product 

product_id = generate_uuid4(table_definition['product_amount'], seed=30)

f = Field(locale=Locale.EN_GB, seed=30)
dt = Datetime(seed=30)

rnd = Random()
rnd.seed(30)

def product_generator() -> dict:
    created_at = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    quantity = rnd.randint(1, 20)
    return {
        "id":"product:"+f"⟨{str(next(product_id))}⟩",
        "name":' '.join(f('words', quantity=2)),
        "description":' '.join(f('words', quantity=rnd.randint(8, 25))),
        "category":f('choice', items=["oil paint", "watercolor", "acrylic paint", "charcoal", "pencil", "ink", "pastel", "collage", "digital art", "mixed media"]),
        "price":f('price', minimum=500, maximum=25000),
        "currency":f('currency_symbol'),
        "discount":f('float_number', start=0.2, end=0.8, precision=1, key=maybe(None, probability=0.8)),
        "quantity":rnd.randint(1, 20), 
        "image_url":f('stock_image_url'),
        "artist":artist_data[f('choice', items=artist_id_count)]['id'],
        "creation_history": {
            "created_at":created_at,
            "quantity":quantity
        }
    }

product_schema = Schema(
    schema=product_generator,
    iterations=table_definition['product_amount']
)

product_data = product_schema.create()

product_id_count = range(table_definition['product_amount']-1)

print("product data created")

## order

order_id = generate_uuid4(table_definition['order_amount'], seed=40)

f = Field(locale=Locale.EN_GB, seed=40)
dt = Datetime(seed=40)

rnd = Random()
rnd.seed(40)

def order_generator() -> dict:
    person_number = f('choice', items=person_id_count)
    product_number = f('choice', items=product_id_count)
    shipping_address = person_data[person_number]['address']
    order_date = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    return {
        "id":f"order:"+f"⟨{str(next(order_id))}⟩",
        "in":person_data[person_number]['id'],
        "out":product_data[product_number]['id'],
        "product_name":product_data[product_number]['name'],
        "currency":product_data[product_number]['currency'],
        "discount":product_data[product_number]['discount'],
        "price":product_data[product_number]['price'],
        "quantity":rnd.randint(1, 3),
        "order_date":order_date,
        "shipping_address":shipping_address,
        "payment_method":f('choice', items=['credit card','debit card', 'PayPal']),
        "order_status":f('choice', items=['pending','processing', 'shipped', 'delivered'], key=maybe(None, probability=0.1))
    }

order_schema = Schema(
    schema=order_generator,
    iterations=table_definition['order_amount']
)

order_data = order_schema.create()

print("order data created")

## review

review_id = generate_uuid4(table_definition['review_amount'], seed=50)

f = Field(locale=Locale.EN_GB, seed=50)
dt = Datetime(seed=50)

rnd = Random()
rnd.seed(50)

def review_generator() -> dict:
    review_date = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    return {
        "id":"review:"+f"⟨{str(next(review_id))}⟩",
        "person":person_data[f('choice', items=person_id_count)]['id'],
        "product":product_data[f('choice', items=product_id_count)]['id'],
        "artist":artist_data[f('choice', items=artist_id_count)]['id'],
        "rating":f('choice', items=[1,2,3,4,5]),
        "review_text":' '.join(f('words', quantity=rnd.randint(8,50))),
        "review_date": review_date
    }

review_schema = Schema(
    schema=review_generator,
    iterations=table_definition['review_amount']
)

review_data = review_schema.create()

print("review data created")

## Load the data

from surrealdb import SurrealDB

db = SurrealDB("ws://localhost:8000/test/test")

db.signin({
    "username": "root",
    "password": "root",
})

# %%
db.query(f"INSERT INTO person {person_data}")

db.query(f"INSERT INTO product {product_data}")

insert_relate_statement(order_data)

db.query(f"INSERT INTO artist {artist_data}")

db.query(f"INSERT INTO review {review_data}")

db.query(""" 
DEFINE INDEX _id_ ON TABLE person COLUMNS id;
DEFINE INDEX _id_ ON TABLE product COLUMNS id;
DEFINE INDEX _id_ ON TABLE artist COLUMNS id;
DEFINE INDEX _id_ ON TABLE review COLUMNS id;
""")

## Run the queries
# %%
## Q1-Q3 - comparing relationships, returning 3 fields from 3 tables (2x relationships)

### Q1: lookup vs record links

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
# %%
### Q2: lookup vs graph - one connection

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
# %%
### Q2 variant: lookup vs graph - using in/out instead of arrow

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
# %%
### Q3: lookup vs graph (and link) - two connections

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
# %%
### Q4: Name and email for all customers in England

db.query(""" 
DEFINE INDEX person_country ON TABLE person COLUMNS address.country;
""")
# %%
db.query(""" 
SELECT name, email 
FROM person 
WHERE address.country = "England";	
""")
# %%
### Q5: standard count

db.query(""" 
DEFINE INDEX order_count ON TABLE order COLUMNS order_status, order_date;
""")
# %%
db.query(""" 
SELECT count() FROM order
WHERE order_status IN ["delivered", "processing", "shipped"]
AND time::month(<datetime>order_date) <4 
GROUP ALL;
""")
# %%
### Q6: Count the number of confirmed orders in Q1 by artists in England

db.query(""" 
SELECT count() FROM order
WHERE order_status IN ["delivered", "processing", "shipped"]
AND time::month(<datetime>order_date) <4 
AND ->product.artist.address.country ?= "England"
GROUP ALL;
""")

# %%
### Q7: Delete a specific review

review_ids_for_deletion = get_gen_uuid4_unique_list(total_num=table_definition['review_amount'], list_num=10, seed=50)

db.query(f""" 
DELETE {str(review_ids_for_deletion.pop())}
RETURN NONE;
""")
# %%
### Q8: Delete reviews from a particular category

# TODO check if this index would work
db.query(""" 
DEFINE INDEX product_category ON TABLE review COLUMNS product.category;
""")
# %%
db.query(""" 
DELETE review
WHERE product.category = "charcoal"
RETURN NONE;
""")

### Q9: Update a customer address
# %%
person_ids_for_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=10, seed=10)

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
# %%
### Q10: Update discounts for products

db.query(""" 
DEFINE INDEX product_price ON TABLE product COLUMNS price;
""")
# %%
db.query(""" 
UPDATE product
SET discount = 0.2
WHERE price < 1000
RETURN NONE;
""")
# %%
### Q11: Transaction - order from a new customer
# TODO add variant with record links to compare directly against mongo

new_person_id = str(uuid4())

product_ids_for_insert_and_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=10, seed=10)

product_id = str(product_ids_for_insert_and_update.pop())

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

### Q10: "Transaction"* - New Artist creates their first product
# Transaction - New Artist creates their first product

new_artist_id = str(uuid4())
new_product_id = str(uuid4())

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