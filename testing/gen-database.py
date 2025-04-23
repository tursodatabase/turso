#!/usr/bin/env python3

import sqlite3
from faker import Faker
import random
import sys

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        age INTEGER
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        order_date TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        rating INTEGER,
        comment TEXT,
        review_date TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
''')

# Insert users and products
fake = Faker()
user_ids = []
for _ in range(10000):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zipcode = fake.zipcode()
    age = fake.random_int(min=1, max=100)

    cursor.execute('''
        INSERT INTO users (first_name, last_name, email, phone_number, address, city, state, zipcode, age)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (first_name, last_name, email, phone_number, address, city, state, zipcode, age))

user_ids = [row[0] for row in cursor.execute('SELECT id FROM users').fetchall()]

product_list = ["hat", "cap", "shirt", "sweater", "sweatshirt",
                "shorts", "jeans", "sneakers", "boots", "coat", "accessories"]
product_ids = []

for product in product_list:
    price = fake.random_int(min=10, max=100)
    cursor.execute('''
        INSERT INTO products (name, price)
        VALUES (?, ?)
    ''', (product, price))

product_ids = [row[0] for row in cursor.execute('SELECT id FROM products').fetchall()]

# Insert orders
for _ in range(10000):
    user_id = random.choice(user_ids)
    product_id = random.choice(product_ids)
    quantity = random.randint(1, 5)
    order_date = fake.date_between(start_date='-2y', end_date='today')

    cursor.execute('''
        INSERT INTO orders (user_id, product_id, quantity, order_date)
        VALUES (?, ?, ?, ?)
    ''', (user_id, product_id, quantity, order_date))

# Insert reviews
for _ in range(10000):
    user_id = random.choice(user_ids)
    product_id = random.choice(product_ids)
    rating = random.randint(1, 5)
    comment = fake.sentence(nb_words=10)
    review_date = fake.date_between(start_date='-2y', end_date='today')

    cursor.execute('''
        INSERT INTO reviews (user_id, product_id, rating, comment, review_date)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, product_id, rating, comment, review_date))

conn.commit()
conn.close()
