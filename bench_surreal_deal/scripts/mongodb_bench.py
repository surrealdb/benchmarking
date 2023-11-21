# import required modules
from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis.enums import TimestampFormat
from mimesis import Datetime

dt = Datetime()
f = Field(locale=Locale.EN_GB, seed=42)

table_definition = {
    "person":{
        "amount":1000
    },
    "product":{
        "amount":1000
    },
    "order":{
        "amount":10000,
    },
    "artist":{
        "amount":500
    },
    "review":{
        "amount":2000
    },
    "create":{
        "amount":1000
    }
}

# create data

## person

def person_generator() -> dict:
    first_name = f('first_name')
    last_name = f('last_name')
    return {
        "_id":f("uuid"),
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

person_id_count = len(person_data)-1

print("person data created")

## artist

def artist_generator() -> dict:
    first_name = f("first_name")
    last_name = f("last_name")
    return {
        "_id":f("uuid"), 
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

artist_id_count = len(artist_data)-1

print("artist data created")

## product

def product_generator() -> dict:
    created_at = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    quantity = f('integer_number', start=0, end=20)
    return {
        "_id":f("uuid"),
        "name":' '.join(f('words', quantity=2)),
        "description":' '.join(f('words', quantity=f('integer_number', start=8, end=25))),
        "category":f('choice', items=["oil paint", "watercolor", "acrylic paint", "charcoal", "pencil", "ink", "pastel", "collage", "digital art", "mixed media"]),
        "price":f('price', minimum=500, maximum=25000),
        "currency":f('currency_symbol'),
        "discount":f('float_number', start=0.2, end=0.8, precision=1, key=maybe(None, probability=0.8)),
        "quantity":quantity, 
        "image_url":f('stock_image_url'),
        "artist":artist_data[f('integer_number', start=0, end=artist_id_count)]['_id'],
        "creation_history": {
            "created_at":created_at,
            "quantity":quantity
        }
    }

product_schema = Schema(
    schema=product_generator,
    iterations=table_definition['product']['amount']
)

product_data = product_schema.create()

product_id_count = len(product_data)-1

print("product data created")

## order

def order_generator() -> dict:
    person_number = f('integer_number', start=0, end=person_id_count)
    product_number = f('integer_number', start=0, end=product_id_count)
    shipping_address = person_data[person_number]['address']
    order_date = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)
    return {
        "_id":f("uuid"),
        "person":person_data[person_number]['_id'],
        "product":product_data[product_number]['_id'],
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

## review

def review_generator() -> dict:
    return {
        "_id":f("uuid"),
        "person":person_data[f('integer_number', start=0, end=person_id_count)]['_id'],
        "product":product_data[f('integer_number', start=0, end=product_id_count)]['_id'],
        "artist":artist_data[f('integer_number', start=0, end=artist_id_count)]['_id'],
        "rating":f('choice', items=[1,2,3,4,5]),
        "review_text":' '.join(f('words', quantity=f('integer_number', start=8, end=50)))
    }

review_schema = Schema(
    schema=review_generator,
    iterations=table_definition['review']['amount']
)

review_data = review_schema.create()

print("review data created")


# load data

from pymongo import MongoClient

MONGODB_URI = "mongodb://localhost:27017/"

client = MongoClient(MONGODB_URI)
db = client["surreal_deal"]


# Create collections
person = db["person"]
product = db["product"]
order = db["order"]
artist = db["artist"]
review = db["review"]

# Insert data
person.insert_many(person_data)
print("person data loaded")

product.insert_many(product_data)
print("product data loaded")

order.insert_many(order_data)
print("order data loaded")

artist.insert_many(artist_data)
print("artist data loaded")

review.insert_many(review_data)
print("review data loaded")