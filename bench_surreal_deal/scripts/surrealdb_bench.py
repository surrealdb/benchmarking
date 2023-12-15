# import required modules
from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis.enums import TimestampFormat
from mimesis import Datetime

dt = Datetime()
f = Field(locale=Locale.EN_GB, seed=42)


# Add table definitions

table_definition = {
    "person":{
        "amount":1000
    },
    "product":{
        "amount":1000
    },
    "order":{
        "amount":10000
    },
    "artist":{
        "amount":500
    },
    "review":{
        "amount":2000
    },
    "create":{
        "amount":1000 # should be same as product
    }
}

# Create table data

## person

def person_generator() -> dict:
    first_name = f('first_name')
    last_name = f('last_name')
    return {
        "id":"person:"+f"⟨{f('uuid')}⟩",
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
    iterations=table_definition['person']['amount']
)
person_data = person_schema.create()

person_record_id_count = len(person_data)-1

print("person data created")

## product

def product_generator() -> dict:
    return {
        "id":"product:"+f"⟨{f('uuid')}⟩",
        "name":' '.join(f('words', quantity=2)),
        "description":' '.join(f('words', quantity=f('integer_number', start=8, end=25))),
        "category":f('choice', items=["oil paint", "watercolor", "acrylic paint", "charcoal", "pencil", "ink", "pastel", "collage", "digital art", "mixed media"]),
        "price":f('price', minimum=500, maximum=25000),
        "currency":f('currency_symbol'),
        "discount":f('float_number', start=0.2, end=0.8, precision=1, key=maybe(None, probability=0.8)),
        "quantity":f('integer_number', start=0, end=20), 
        "image_url":f('stock_image_url')
    }

product_schema = Schema(
    schema=product_generator,
    iterations=table_definition['product']['amount']
)

product_data = product_schema.create()

product_record_id_count = len(product_data)-1

print("product data created")

## order

def order_generator() -> dict:
    person_number = f('integer_number', start=0, end=person_record_id_count)
    product_number = f('integer_number', start=0, end=product_record_id_count)
    shipping_address = person_data[person_number]['address']
    order_date = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    return {
        "id":f"order:"+f"⟨{f('uuid')}⟩",
        "in":person_data[person_number]['id'],
        "out":product_data[product_number]['id'],
        "product_name":product_data[product_number]['name'],
        "currency":product_data[product_number]['currency'],
        "discount":product_data[product_number]['discount'],
        "price":product_data[product_number]['price'],
        "quantity":f('integer_number', start=1, end=3),
        "order_date":order_date,
        "shipping_address":shipping_address,
        "payment_method":f('choice', items=['credit card','debit card', 'PayPal']),
        "order_status":f('choice', items=['pending','processing', 'shipped', 'delivered'], key=maybe(None, probability=0.1))
    }

order_schema = Schema(
    schema=order_generator,
    iterations=table_definition['order']['amount']
)

order_data = order_schema.create()

print("order data created")

## artist

def artist_generator() -> dict:
    first_name = f("first_name")
    last_name = f("last_name")
    return {
        "id":"artist:"+f"⟨{f('uuid')}⟩",
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
    iterations=table_definition['artist']['amount']
)

artist_data = artist_schema.create()

artist_record_id_count = len(artist_data)-1

print("artist data created")

## review

def review_generator() -> dict:
    return {
        "id":"review:"+f"⟨{f('uuid')}⟩",
        "person":person_data[f('integer_number', start=0, end=person_record_id_count)]['id'],
        "product":product_data[f('integer_number', start=0, end=product_record_id_count)]['id'],
        "artist":artist_data[f('integer_number', start=0, end=artist_record_id_count)]['id'],
        "rating":f('choice', items=[1,2,3,4,5]),
        "review_text":' '.join(f('words', quantity=f('integer_number', start=8, end=50)))
    }

review_schema = Schema(
    schema=review_generator,
    iterations=table_definition['review']['amount']
)

review_data = review_schema.create()

print("review data created")

## create
# TODO possibly take this one out and just do order->product->artist?
# That would mean though that order is both an edge and a node.. but would make querying more efficent
def create_generator() -> dict:
    product_number = f('increment')-1
    artist_number = f('integer_number', start=0, end=artist_record_id_count)
    created_at = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    return {
        "id":"create:"+f"⟨{f('uuid')}⟩",
        "in":artist_data[artist_number]['id'],
        "out":product_data[product_number]['id'],
        "created_at":created_at,
        "quantity":product_data[product_number]['quantity']
    }

create_schema = Schema(
    schema=create_generator,
    iterations=table_definition['create']['amount']
)

create_data = create_schema.create()

print("create data created")

# load data

from surrealdb import SurrealDB

db = SurrealDB("ws://localhost:8000/test/test")

db.signin({
    "username": "root",
    "password": "root",
})


def insert_relate_statement(table_data:list[dict]) -> str:
    """
    Inserting data through relate statement
    """

    table_record_id = -1
    for record in table_data:
        table_record_id += 1
        db.query(
    f"RELATE {table_data[table_record_id]['in']} -> {table_data[table_record_id]['id']} -> {table_data[table_record_id]['out']} CONTENT {record};"
            )


# TODO need to flag this as a bug, no error and doesn't insert data
# db.query(f"INSERT INTO person [{person_data}]") 


person_insert = db.query(f"INSERT INTO person {person_data}")
print(f"{len(person_insert)} of {len(person_data)} records inserted")

product_insert = db.query(f"INSERT INTO product {product_data}")
print(f"{len(product_insert)} of {len(product_data)} records inserted")

order_insert = insert_relate_statement(order_data)
print(f" of {len(order_data)} records inserted")

artist_insert = db.query(f"INSERT INTO artist {artist_data}")
print(f"{len(artist_insert)} of {len(artist_data)} records inserted")

review_insert = db.query(f"INSERT INTO review {review_data}")
print(f"{len(review_insert)} of {len(review_data)} records inserted")

create_insert = insert_relate_statement(create_data)
print(f" of {len(create_data)} records inserted")