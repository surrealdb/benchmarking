## Create the data

# import required modules
from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis import Datetime

from random import Random
from uuid import UUID, uuid4
import time

from bench_utils import generate_uuid4, get_gen_uuid4_unique_list, table_definition, format_time

# create data

## person

person_id = generate_uuid4(table_definition['person_amount'], seed=10)

f = Field(locale=Locale.EN_GB, seed=10)

def person_generator() -> dict:
    first_name = f('first_name')
    last_name = f('last_name')
    return {
        "_id":next(person_id),
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
        "_id":next(artist_id), 
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
    created_at = dt.datetime(start=2023, end=2023)
    quantity = rnd.randint(1, 20)
    return {
        "_id":next(product_id),
        "name":' '.join(f('words', quantity=2)),
        "description":' '.join(f('words', quantity=rnd.randint(8, 25))),
        "category":f('choice', items=["oil paint", "watercolor", "acrylic paint", "charcoal", "pencil", "ink", "pastel", "collage", "digital art", "mixed media"]),
        "price":f('price', minimum=500, maximum=25000),
        "currency":f('currency_symbol'),
        "discount":f('float_number', start=0.2, end=0.8, precision=1, key=maybe(None, probability=0.8)),
        "quantity":quantity, 
        "image_url":f('stock_image_url'),
        "artist":artist_data[f('choice', items=artist_id_count)]['_id'],
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
    order_date = dt.datetime(start=2023, end=2023)
    return {
        "_id":next(order_id),
        "person":person_data[person_number]['_id'],
        "product":product_data[product_number]['_id'],
        "product_name":product_data[product_number]['name'],
        "currency":product_data[product_number]['currency'],
        "discount":product_data[product_number]['discount'],
        "price":product_data[product_number]['price'],
        "quantity": rnd.randint(1, 3),
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
    review_date = dt.datetime(start=2023, end=2023)
    return {
        "_id":next(review_id),
        "person":person_data[f('choice', items=person_id_count)]['_id'],
        "product":product_data[f('choice', items=product_id_count)]['_id'],
        "artist":artist_data[f('choice', items=artist_id_count)]['_id'],
        "rating":f('choice', items=[1,2,3,4,5]),
        "review_text":' '.join(f('words', quantity=rnd.randint(8, 50))),
        "review_date": review_date
    }

review_schema = Schema(
    schema=review_generator,
    iterations=table_definition['review_amount']
)

review_data = review_schema.create()

print("review data created")

## Load the data

from pymongo import MongoClient
from bson.binary import UuidRepresentation
from datetime import datetime,timezone

MONGODB_URI = "mongodb://localhost:27017/"

client = MongoClient(MONGODB_URI, uuidRepresentation='standard')

# in case it exists drop the database
client.drop_database('surreal_deal')

db = client["surreal_deal"]

# Create collections
person = db["person"]
product = db["product"]
order = db["order"]
artist = db["artist"]
review = db["review"]

person.insert_many(person_data)

product.insert_many(product_data)

order.insert_many(order_data)

artist.insert_many(artist_data)

review.insert_many(review_data)

## Run the queries

## Q1-Q3 - comparing relationships, returning 3 fields from 3 tables (2x relationships)

### Q1: lookup vs record links

list(review.aggregate([
	{
		"$lookup": {
			"from": "person",
			"localField": "person",
			"foreignField": "_id",
			"as": "person",
		}
	},
	{
		"$lookup": {
			"from": "product",
			"localField": "product",
			"foreignField": "_id",
			"as": "product",
		}
	},
    { 
		"$project": { 
		"_id": 0, 
		"rating": 1, 
		"review_text": 1, 
		"review_date": 1, 
		"person.name": 1,
		"person.email": 1, 
		"person.phone": 1, 
		"product.name": 1,
		"product.category": 1,
		"product.image_url": 1 
		} 
	}
]))


### Q2: lookup vs graph - one connection

list(order.aggregate([
	{
		"$lookup": {
			"from": "person",
			"localField": "person",
			"foreignField": "_id",
			"as": "person",
		}
	},
	{
		"$lookup": {
			"from": "product",
			"localField": "product",
			"foreignField": "_id",
			"as": "product",
		}
	},
    { 
        "$project": { 
            "_id": 0, 
            "price": 1, 
            "order_date": 1, 
            "product_name": 1,
            "person.name": 1, 
            "person.email": 1, 
            "person.phone": 1,
            "product.category": 1,
            "product.description": 1,
            "product.image_url": 1 
        } 
    }
]))

### Q2 variant: graphlookup vs graph - one connection
# note - not possible with index?
# documentation doesn't say and have tried everything I can think of
# ids are indexed by default anyway, but doesn't seem to matter here

list(order.aggregate([
    {
        '$graphLookup': {
            'from': 'person', 
            'startWith': '$person', 
            'connectFromField': 'person', 
            'connectToField': '_id', 
            'maxDepth': 0, 
            'as': 'person'
        }
    }, 
    {
        '$graphLookup': {
            'from': 'product', 
            'startWith': '$product', 
            'connectFromField': 'product', 
            'connectToField': '_id', 
            'maxDepth': 0, 
            'as': 'product'
        }
    }, 
    { 
        "$project": { 
            "_id": 0, 
            "price": 1, 
            "order_date": 1, 
            "product_name": 1,
            "person.name": 1, 
            "person.email": 1, 
            "person.phone": 1,
            "product.category": 1,
            "product.description": 1,
            "product.image_url": 1 
        } 
    }
]))

### Q3: lookup vs graph - two connections
# TODO index is not triggered
# might work with $unwind?

list(order.aggregate([
	{
		"$lookup": {
			"from": "product",
			"localField": "product",
			"foreignField": "_id",
			"pipeline": [
				{
					"$lookup": {
						"from": "artist",
						"localField": "artist",
						"foreignField": "_id",
						"as": "artist",
					}
				},
			],
			"as": "product",
		}
	},
	{ 
        "$project": { 
            "_id": 0, 
            "price": 1, 
            "order_date": 1, 
            "product_name": 1,
            "product.category": 1,
            "product.description": 1,
            "product.image_url": 1,
			"product.artist.name": 1,
			"product.artist.email": 1,
			"product.artist.phone": 1,
        } 
    }
]))

### Q4: Name and email for all customers in England

person.create_index({"address.country": 1})

list(person.find(
	{ "address.country": "England" },
	{ "_id": 0, "name": 1, "email": 1 }
))

### Q5: standard count

# collection.count() is deprecated in PyMongo
# collection.count_documents() is the replacement but is slow.
# collection.estimated_document_count() is the fast version but might be wrong.
# Sometimes that doesn't matter, but I think here it does since SurrelDB count() is exact.
# also collection.estimated_document_count() gave wrong result in my tests

order.create_index({"order_status": 1, "order_date": 1})

order.count_documents({
	"order_status": { "$in": ['delivered', 'processing', 'shipped'] },
	"$expr": { "$lt": ["$order_date", datetime(2023, 4, 1, 0, 0, 0, 0)]}
})


### Q6: count with a relationship (agg framework) - Count the number of confirmed orders in Q1 by artists in England

# TODO Should I reduce to single $lookup? - people do nested though https://www.mongodb.com/community/forums/t/nested-lookup-aggregation/224456

list(order.aggregate([
	{
		"$lookup": {
			"from": "product",
			"localField": "product",
			"foreignField": "_id",
			"pipeline": [
				{
					"$lookup": {
						"from": "artist",
						"localField": "artist",
						"foreignField": "_id",
						"as": "artist",
					}
				},
			],
			"as": "product",
		}
	},
	{
		"$match": {
            "order_status": { "$in": ['delivered', 'processing', 'shipped'] },
            "$expr": { "$lt": ["$order_date", datetime(2023, 4, 1, 0, 0, 0, 0)]},
			"product.artist.address.country": "England",
		}
	},
	{ "$count": "count" }
]))



### Q7: Delete a specific review

review_ids_for_deletion = get_gen_uuid4_unique_list(total_num=table_definition['review_amount'], list_num=10, seed=50)

review.delete_one({ "_id": review_ids_for_deletion.pop() })

### Q8: Delete reviews from a particular category

product.create_index({"category": 1})

review.delete_many({ "product": { "$in": product.distinct("_id", { "category": "charcoal" }) } })

### Q9: Update a customer address

person_ids_for_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=10, seed=10)

person.update_one(
	{ "_id": person_ids_for_update.pop() },
	{
		"$set": {
			"address": {
				'address_line_1': '497 Ballycander',
				'address_line_2': "null",
				'city': 'Bromyard',
				'country': 'Wales',
				'post_code': 'ZX8N 4VJ',
				'coordinates': [68.772592, -35.491877]
			}
		}
	}
)

### Q10: Update discounts for products

product.create_index({"price": 1})

product.update_many(
	{ "price": { "$lt": 1000 } },
	{ "$set": { "discount": 0.2 } }
)


### Q11: "Transaction"* order from a new customer

# Transaction - order from a new customer
# TODO make with_transactions version for distributed test

new_person_id = uuid4()

product_ids_for_insert_and_update = get_gen_uuid4_unique_list(total_num=table_definition['person_amount'], list_num=10, seed=10)

product_id = product_ids_for_insert_and_update.pop()

# insert into the person table
person.insert_one({
		'_id': new_person_id,
		'first_name': 'Karyl',
		'last_name': 'Langley',
		'name': 'Karyl Langley',
		'company_name': "null",
		'email': 'dee1961@gmail.com',
		'phone': '+44 47 3516 5895',
		'address': {
			'address_line_1': '510 Henalta',
			'address_line_2': "null",
			'city': 'Lyme Regis',
			'country': 'Northern Ireland',
			'post_code': 'TO6Q 8CM',
			'coordinates': [-34.345071, 118.564172]
		}
	})

# insert into the order table
order.insert_one({
	"_id": uuid4(),
	"person": new_person_id,
	"product": product_id,
	'currency': '£',
	'discount': db.product.distinct("discount", { "_id" : product_id }), 
	"order_date": datetime.now(tz=timezone.utc),
	"order_status": "pending",
	"payment_method": "PayPal",
	"price": db.product.distinct("price", { "_id" : product_id }),
	"product_name": db.product.distinct("name", { "_id" : product_id }),
	"quantity": 1,
	"shipping_address": db.person.distinct("address", { "_id" : new_person_id })
})

# update the product table to reduce the quantity
product.update_one(
	{ "_id": product_id },
	{ "$inc": { "quantity": -1 } }
)

### Q12: "Transaction"* - New Artist creates their first product

# Transaction - New Artist creates their first product
# TODO make with_transactions version for distributed test

new_artist_id = uuid4()
new_product_id = uuid4()

# insert into the artist table
artist.insert_one({
    '_id': new_artist_id,
    'first_name': 'Anderson',
    'last_name': 'West',
    'name': 'Anderson West',
    'company_name': 'Atkins(ws) (ATK)',
    'email': 'six1933@gmail.com',
    'phone': '056 5881 1126',
    'address': {
        'address_line_1': '639 Connaugh',
        'address_line_2': "null",
        'city': 'Ripon',
        'country': 'Scotland',
        'post_code': 'CG3U 4TH',
        'coordinates': [4.273648, -112.907273]
	}
})

# insert into the product table
product.insert_one({
    '_id': new_product_id,
    'name': 'managed edt allocated pda',
    'description': 'counseling dildo greek pan works interest xhtml wrong dennis available cl specific next tower webcam peace magic',
    'category': 'watercolor',
    'price': 15735.96,
    'currency': '£',
    'discount': "null",
    'quantity': 1,
    'image_url': 'https://source.unsplash.com/1920x1080?',
    "artist": new_artist_id,
    "creation_history": {
        "quantity": 1,
        "created_at": datetime.now(tz=timezone.utc)
	}
})

client.close()


