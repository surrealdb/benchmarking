
## Create the data

# import required modules
from mimesis.locales import Locale
from mimesis.keys import maybe
from mimesis.schema import Field, Schema
from mimesis import Datetime

dt = Datetime(seed=42)
f = Field(locale=Locale.EN_GB, seed=42)

from random import Random
from uuid import UUID, uuid4

rnd = Random()
rnd.seed(10)

table_definition = {
    "person":{
        "amount":1000 *10
    },
    "product":{
        "amount":1000 *10
    },
    "order":{
        "amount":10000 *10
    },
    "artist":{
        "amount":500 *10
    },
    "review":{
        "amount":2000 *10
    }
}

# create data

## person

def generate_uuid4(amount):
    return [UUID(int=rnd.getrandbits(128), version=4) for _ in range(amount)]

person_id = generate_uuid4(table_definition['person']['amount'])

def person_generator() -> dict:
    first_name = f('first_name')
    last_name = f('last_name')
    return {
        "_id":person_id.pop(),
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

person_id_count = range(table_definition['person']['amount']-1)

print("person data created")

## artist

artist_id = generate_uuid4(table_definition['artist']['amount'])

def artist_generator() -> dict:
    first_name = f("first_name")
    last_name = f("last_name")
    return {
        "_id":artist_id.pop(), 
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

artist_id_count = range(table_definition['artist']['amount']-1)

print("artist data created")

## product

product_id = generate_uuid4(table_definition['product']['amount'])

def product_generator() -> dict:
    created_at = dt.datetime(start=2023, end=2023)
    quantity = rnd.randint(1, 20)
    return {
        "_id":product_id.pop(),
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
    iterations=table_definition['product']['amount']
)

product_data = product_schema.create()

product_id_count = range(table_definition['product']['amount']-1)

print("product data created")

## order

order_id = generate_uuid4(table_definition['order']['amount'])

def order_generator() -> dict:
    person_number = f('choice', items=person_id_count)
    product_number = f('choice', items=product_id_count)
    shipping_address = person_data[person_number]['address']
    order_date = dt.datetime(start=2023, end=2023)
    return {
        "_id":order_id.pop(),
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
    iterations=table_definition['order']['amount']
)

order_data = order_schema.create()

print("order data created")

review_id = generate_uuid4(table_definition['review']['amount'])

## review
def review_generator() -> dict:
    return {
        "_id":review_id.pop(),
        "person":person_data[f('choice', items=person_id_count)]['_id'],
        "product":product_data[f('choice', items=product_id_count)]['_id'],
        "artist":artist_data[f('choice', items=artist_id_count)]['_id'],
        "rating":f('choice', items=[1,2,3,4,5]),
        "review_text":' '.join(f('words', quantity=rnd.randint(8, 50)))
    }

review_schema = Schema(
    schema=review_generator,
    iterations=table_definition['review']['amount']
)

review_data = review_schema.create()

print("review data created")

## Load the data

from pymongo import MongoClient
from bson.binary import UuidRepresentation
from datetime import datetime,timezone

MONGODB_URI = "mongodb://localhost:27017/"

client = MongoClient(MONGODB_URI, uuidRepresentation='standard')
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

### Q1: lookup vs record links

list(review.aggregate([
	{
		"$lookup": {
			"from": "artist",
			"localField": "artist",
			"foreignField": "_id",
			"pipeline": [
				{ "$project": { "_id": 0, "name": 1, "email": 1, "phone": 1 } }
			],
			"as": "artist",
		}

	},
	{
		"$lookup": {
			"from": "person",
			"localField": "person",
			"foreignField": "_id",
			"pipeline": [
				{ "$project": { "_id": 0, "name": 1, "email": 1, "phone": 1 } }
			],
			"as": "person",
		}

	},
	{
		"$lookup": {
			"from": "product",
			"localField": "product",
			"foreignField": "_id",
			"pipeline": [
				{ "$project": { "_id": 0, "name": 1, "category": 1, "price": 1 } }
			],
			"as": "product",
		}
	}
]))


### Q2 A: lookup vs graph - one connection

list(order.aggregate([
	{
		"$lookup": {
			"from": "person",
			"localField": "person",
			"foreignField": "_id",
			"pipeline": [
				{ "$project": { "_id": 0, "name": 1, "email": 1, "phone": 1 } }
			],
			"as": "person",
		}
	},
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
						"pipeline": [
							{ "$project": { "_id": 0, "name": 1, "email": 1, "phone": 1 } }
						],
						"as": "artist",
					}
				},
				{ "$project": { "_id": 0, "category": 1, "description": 1, "image_url": 1, "artist": 1 } }
			],
			"as": "product",
		}
	},
	{ "$project": { "_id": 0, "price": 1, "order_date": 1, "product_name": 1, "artist": 1, "person": 1, "product": 1 } }
]))

### Q2 B: lookup vs graph - two connections

list(order.aggregate([
	{
		"$lookup": {
			"from": "person",
			"localField": "person",
			"foreignField": "_id",
			"pipeline": [
				{ "$project": { "_id": 0, "name": 1, "email": 1, "phone": 1 } }
			],
			"as": "person",
		}
	},
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
						"pipeline": [
							{ "$project": { "_id": 0, "name": 1, "email": 1, "phone": 1 } }
						],
						"as": "artist",
					}
				},
				{ "$project": { "_id": 0, "category": 1, "description": 1, "image_url": 1, "artist": 1 } }
			],
			"as": "product",
		}
	},
	{ "$project": { "_id": 0, "price": 1, "order_date": 1, "product_name": 1, "artist": 1, "person": 1, "product": 1 } }
]))

### Q3: Name and email for all customers in England

person.create_index({"address.country": 1})

list(person.find(
	{ "address.country": "England" },
	{ "_id": 0, "name": 1, "email": 1 }
))

### Q4: standard count

# collection.count() is deprecated in PyMongo
# collection.count_documents() is the replacement but is slow.
# collection.estimated_document_count() is the fast version but might be wrong .
# Sometimes that doesn't matter, but I think here it does since SurrelDB count() is exact.

order.create_index({"order_date": 1, "order_status": 1})

order.count_documents({
	"order_status": { "$in": ['delivered', 'processing', 'shipped'] },
	"$expr": { "$lt": ["$order_date", datetime(2023, 4, 1, 0, 0, 0, 0)]}
})


### Q5: count with a relationship (agg framework) - Count the number of confirmed orders in Q1 by artists in England

# TODO make sure the number of orders is consistent with SurrealDB one
# TODO see if I can have the index also cover "product.artist.address.country"
# Should I reduce to single $lookup? - people do nested though https://www.mongodb.com/community/forums/t/nested-lookup-aggregation/224456

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
						"pipeline": [
							{ "$project": { "_id": 0, "address.country": 1 } }
						],
						"as": "artist",
					}
				},
				{ "$project": { "_id": 0, "artist": 1 } }
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

### Q6: Delete a specific review
# TODO add static UUID

review.delete_one({ "_id": "UUID" })


### Q7: Delete reviews from a particular category

product.create_index({"category": 1})

review.delete_many({ "product": { "$in": product.distinct("_id", { "category": "charcoal" }) } })

### Q8: Update a customer address
# TODO add static UUID

person.update_one(
	{ "_id": "UUID" },
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

### Q9: Update discounts for products

product.create_index({"price": 1})

product.update_many(
	{ "price": { "$lt": 1000 } },
	{ "$set": { "discount": 0.2 } }
)


### Q10: "Transaction"* order from a new customer

# Transaction - order from a new customer
# TODO add static UUID
# TODO make with_transactions version for distributed test

new_person_id = uuid4()
product_id = "UUID"

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

### Q11: "Transaction"* - New Artist creates their first product

# Transaction - New Artist creates their first product
# TODO make with_transactions version for distributed test

new_artist_id = uuid4()

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
    '_id': uuid4(),
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


