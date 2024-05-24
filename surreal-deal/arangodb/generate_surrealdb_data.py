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

# Create docment base for SurrealDB export file

## define tables

def define_tables(table_definition:dict) -> str:
    doc = ''
    sf = 'schemafull'
    sl = 'schemaless'
    for name in table_definition.keys():
        doc += (
f"""
-- ------------------------------
-- TABLE:{name}
-- ------------------------------

DEFINE TABLE {name} {sf if 'schema' in table_definition[name] else sl} PERMISSIONS {'none' if 'permissions' not in table_definition[name] else table_definition[name]['permissions']};
"""
        )
        if 'schema' in table_definition[name]:
            for field, value in table_definition[name]['schema'].items():
                doc += (
f"""DEFINE FIELD {field} ON TABLE {name} {"FLEXIBLE TYPE" if value == "object" else "TYPE"} {value}
        ASSERT $value != NONE;\n"""
                )
    return doc

## generate SurrealQL update/relate statements

def generate_table_update_statement(table_name:str, table_data:list[dict]) -> str:
    """
    Adding table data to the doc using the update statement
    """
    doc = (
f"""
-- ------------------------------
-- TABLE DATA:{table_name}
-- ------------------------------

"""
    )
    for record in table_data:
        doc += (
f"UPDATE {record['id']} CONTENT {record};\n"
        )
    return doc

def generate_table_relate_statement(table_name:str, node_table_data:list[dict]) -> str:
    """
    Adding table data to the doc using the relate statement
    """
    doc = (
f"""
-- ------------------------------
-- TABLE DATA:{table_name}
-- ------------------------------

"""
    )
    node_record_id = -1
    for record in node_table_data:
        node_record_id += 1
        doc += (
f"""RELATE {node_table_data[node_record_id]['in']} -> {node_table_data[node_record_id]['id']} -> {node_table_data[node_record_id]['out']} CONTENT {record};\n"""
        )
    return doc

doc = (
"""
-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

-- ------------------------------
-- TRANSACTION
-- ------------------------------

BEGIN TRANSACTION;
"""
)


# create the data

## person

def sdb_generate_person_data(amount=table_definition['person_amount']):

    person_id = generate_uuid4(amount, seed=10)

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
        iterations=amount
    )
    person_data = person_schema.create()
    print("person data created")

    return person_data

## artist

def sdb_generate_artist_data(amount=table_definition['artist_amount']):

    artist_id = generate_uuid4(amount, seed=20)

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
        iterations=amount
    )

    artist_data = artist_schema.create()
    print("artist data created")

    return artist_data

## product

def sdb_generate_product_data(artist_data, amount=table_definition['product_amount']):

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
        iterations=amount
    )

    product_data = product_schema.create()
    print("product data created")

    return product_data

## order

def sdb_generate_order_data(person_data, product_data, amount=table_definition['order_amount']):

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
        iterations=amount
    )

    order_data = order_schema.create()
    print("order data created")

    return order_data

## review

def sdb_generate_review_data(person_data, product_data, artist_data, amount=table_definition['review_amount']):

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
        iterations=amount
    )

    review_data = review_schema.create()
    print("review data created")

    return review_data



# Generate the data and export

## Generating data

person_data = sdb_generate_person_data()
artist_data = sdb_generate_artist_data()
product_data = sdb_generate_product_data(artist_data)
order_data = sdb_generate_order_data(person_data, product_data)
review_data = sdb_generate_review_data(person_data, product_data, artist_data)

## Writing data to a SurrealDB export file

doc += generate_table_update_statement("person", person_data)
doc += generate_table_update_statement('product', product_data)
doc += generate_table_relate_statement('order', order_data)
doc += generate_table_update_statement('artist', artist_data)
doc += generate_table_update_statement('review', review_data)

doc += (
"""
-- ------------------------------
-- TRANSACTION
-- ------------------------------

COMMIT TRANSACTION;

"""
)

file_path = pathlib.Path(__file__).parents[0] / "output_files" / "surrealdb_data"

with open(file_path / pathlib.Path("surreal_deal.surql"), 'w') as f:
    f.write(doc)

## writing to JSON files

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