# Selecting multiple fields from multiple tables using record links
SELECT
    id,
    rating,
    review_text,
	artist.name,
	artist.email,
	artist.phone,
	person.name,
	person.email,
	person.phone,
	product.name,
	product.category,
	product.price
FROM review;


# Selecting multiple fields from multiple tables using graph
SELECT
	price,
	order_date,
	product_name,
	->product.category,
	->product.description,
	->product.image_url,
	<-person.name,
	<-person.email,
	<-person.phone,
	->product<-create<-artist.name, # should just be two? product->artist.name
	->product<-create<-artist.email,
	->product<-create<-artist.phone
FROM order;

# Name and email for all customers in England
SELECT name, email 
FROM person 
WHERE address.country = "England";	

# Count the number of confirmed orders in Q1 by artists in England
SELECT count() FROM order
WHERE (order_status != "pending"
OR order_status = null)
AND time::month(<datetime>order_date) <=3
AND ->product<-create<-artist.address.country ?= "England"
GROUP ALL;

# Delete a specific review
DELETE review:⟨000af635-01c8-4794-bd2f-e8b313c57ae9⟩;

# Delete reviews from a particular category
DELETE review
WHERE product.category = "charcoal";

# Update customer address
UPDATE person:⟨0f53c8bf-d3c0-4947-bdf0-76bda74c3f52⟩
SET address = {
	'address_line_1': '497 Ballycander',
	'address_line_2': None,
	'city': 'Bromyard',
	'country': 'Wales',
	'post_code': 'ZX8N 4VJ',
	'coordinates': [68.772592, -35.491877]
	};

# Update discount for products
UPDATE product
SET discount = 0.2
WHERE price < 1000;

# Transaction - order from a new customer
BEGIN TRANSACTION;
-- insert into the person table # use create 
INSERT INTO person {
	'id': 'person:⟨644ef4d0-423f-4446-95df-bc8b5b634874⟩',
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
	};

-- relate into the order table
RELATE person:⟨644ef4d0-423f-4446-95df-bc8b5b634874⟩ -> order -> product:⟨00068999-b5e2-4a90-a1a2-e2469229c847⟩ CONTENT {
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

-- update the product table to reduce the quantity
UPDATE product:⟨00068999-b5e2-4a90-a1a2-e2469229c847⟩ SET quantity -= 1;
COMMIT TRANSACTION;

# Transaction - New Artist creates their first product
BEGIN TRANSACTION;
-- insert into the artist table
INSERT INTO artist {
        'id': 'artist:⟨b8683656-6fe3-47ae-830c-41451c3ce953⟩',
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
        };

-- insert into the product table
INSERT INTO product {
        'id': 'product:⟨d8f59414-4484-4634-a866-8b607fd2311e⟩',
        'name': 'managed edt allocated pda',
        'description': 'counseling dildo greek pan works interest xhtml wrong dennis available cl specific next tower webcam peace magic',
        'category': 'watercolor',
        'price': 15735.96,
        'currency': '£',
        'discount': None,
        'quantity': 1,
        'image_url': 'https://source.unsplash.com/1920x1080?'
        };

# TODO do variation without create
-- relate into the create table
RELATE artist:⟨b8683656-6fe3-47ae-830c-41451c3ce953⟩ -> create -> product:⟨d8f59414-4484-4634-a866-8b607fd2311e⟩ CONTENT {
        created_at: time::now(),
        quantity: 1
};
COMMIT TRANSACTION;