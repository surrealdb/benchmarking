# import required modules
import pathlib
from random import Random
import json

from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis import Datetime
from mimesis.enums import TimestampFormat

from bench_utils import table_definition, generate_uuid4

# create the data

## person

def adb_generate_person_data(amount=table_definition['person_amount']):

    person_id = generate_uuid4(amount, seed=10)

    f = Field(locale=Locale.EN_GB, seed=10)

    def person_generator() -> dict:
        first_name = f('first_name')
        last_name = f('last_name')
        return {
            "_key": str(next(person_id)),
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
        iterations=amount
    )
    person_data = person_schema.create()
    print("person data created")

    return person_data

## artist

def adb_generate_artist_data(amount=table_definition['artist_amount']):

    artist_id = generate_uuid4(amount, seed=20)

    f = Field(locale=Locale.EN_GB, seed=20)

    def artist_generator() -> dict:
        first_name = f("first_name")
        last_name = f("last_name")
        return {
            "_key": str(next(artist_id)),
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
        iterations=amount
    )

    artist_data = artist_schema.create()
    print("artist data created")

    return artist_data

## product

def adb_generate_product_data(artist_data, amount=table_definition['product_amount']):

    product_id = generate_uuid4(amount, seed=30)
    artist_id_count = range(table_definition['artist_amount']-1)

    f = Field(locale=Locale.EN_GB, seed=30)
    dt = Datetime(seed=30)

    rnd = Random()
    rnd.seed(30)

    def product_generator() -> dict:
        created_at = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)+"Z"
        quantity = rnd.randint(1, 20)
        return {
            "_key": str(next(product_id)),
            "name":' '.join(f('words', quantity=2)),
            "description":' '.join(f('words', quantity=rnd.randint(8, 25))),
            "category":f('choice', items=["oil paint", "watercolor", "acrylic paint", "charcoal", "pencil", "ink", "pastel", "collage", "digital art", "mixed media"]),
            "price":f('price', minimum=500, maximum=25000),
            "currency":f('currency_symbol'),
            "discount":f('float_number', start=0.2, end=0.8, precision=1, key=maybe(None, probability=0.8)),
            "quantity":rnd.randint(1, 20), 
            "image_url":f('stock_image_url'),
            "artist":artist_data[f('choice', items=artist_id_count)]['_key'],
            "creation_history": {
                "created_at":created_at,
                "quantity":quantity
            }
        }

    product_schema = Schema(
        schema=product_generator,
        iterations=amount
    )

    product_data = product_schema.create()
    print("product data created")

    return product_data

## order

def adb_generate_order_data(person_data, product_data, amount=table_definition['order_amount']):

    order_id = generate_uuid4(amount, seed=40)

    person_id_count = range(table_definition['person_amount']-1)

    product_id_count = range(table_definition['product_amount']-1)

    f = Field(locale=Locale.EN_GB, seed=40)
    dt = Datetime(seed=40)

    rnd = Random()
    rnd.seed(40)

    def order_generator() -> dict:
        person_number = f('choice', items=person_id_count)
        product_number = f('choice', items=product_id_count)
        shipping_address = person_data[person_number]['address']
        order_date = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)+"Z"
        return {
            "_key": str(next(order_id)),
            "in":person_data[person_number]['_key'],
            "out":product_data[product_number]['_key'],
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
        iterations=amount
    )

    order_data = order_schema.create()
    print("order data created")

    return order_data

## review

def adb_generate_review_data(person_data, product_data, artist_data, amount=table_definition['review_amount']):

    review_id = generate_uuid4(amount, seed=50)

    f = Field(locale=Locale.EN_GB, seed=50)
    dt = Datetime(seed=50)

    rnd = Random()
    rnd.seed(50)

    person_id_count = range(table_definition['person_amount']-1)

    artist_id_count = range(table_definition['artist_amount']-1)

    product_id_count = range(table_definition['product_amount']-1)

    def review_generator() -> dict:
        review_date = dt.timestamp(TimestampFormat.ISO_8601, start=2023, end=2023)+"Z"
        return {
            "_key": str(next(review_id)),
            "person":person_data[f('choice', items=person_id_count)]['_key'],
            "product":product_data[f('choice', items=product_id_count)]['_key'],
            "artist":artist_data[f('choice', items=artist_id_count)]['_key'],
            "rating":f('choice', items=[1,2,3,4,5]),
            "review_text":' '.join(f('words', quantity=rnd.randint(8,50))),
            "review_date": review_date
        }

    review_schema = Schema(
        schema=review_generator,
        iterations=amount
    )

    review_data = review_schema.create()
    print("review data created")

    return review_data

# Generate the data and export

## Generating data

person_data = adb_generate_person_data()
artist_data = adb_generate_artist_data()
product_data = adb_generate_product_data(artist_data)
order_data = adb_generate_order_data(person_data, product_data)
review_data = adb_generate_review_data(person_data, product_data, artist_data)

## writing to JSON files

file_path = pathlib.Path(__file__).parents[0] / "output_files" / "arangodb_data"

with open(file_path / pathlib.Path("person_data.json"), "w") as file:
    json.dump(person_data, file, ensure_ascii=False)

with open(file_path / pathlib.Path("artist_data.json"), "w") as file:
    json.dump(artist_data, file, ensure_ascii=False)

with open(file_path / pathlib.Path("product_data.json"), "w") as file:
    json.dump(product_data, file, ensure_ascii=False)

with open(file_path / pathlib.Path("order_data.json"), "w") as file:
    json.dump(order_data, file, ensure_ascii=False)

with open(file_path / pathlib.Path("review_data.json"), "w") as file:
    json.dump(review_data, file, ensure_ascii=False)