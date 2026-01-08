#!/usr/bin/env python3
import random
import sqlite3

from faker import Faker

conn = sqlite3.connect("testing/testing-bigass.db")
cursor = conn.cursor()

fake = Faker()

cursor.execute("""
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
        age INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    )
""")

cursor.execute(
    """
CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL
    );
""",
    [],
)

# specific products we already test for
cursor.execute("INSERT INTO products VALUES(1,'hat',79.0);")
cursor.execute("INSERT INTO products VALUES(2,'cap',82.0);")
cursor.execute("INSERT INTO products VALUES(3,'shirt',18.0);")
cursor.execute("INSERT INTO products VALUES(4,'sweater',25.0);")
cursor.execute("INSERT INTO products VALUES(5,'sweatshirt',74.0);")
cursor.execute("INSERT INTO products VALUES(6,'shorts',70.0);")
cursor.execute("INSERT INTO products VALUES(7,'jeans',78.0);")
cursor.execute("INSERT INTO products VALUES(8,'sneakers',82.0);")
cursor.execute("INSERT INTO products VALUES(9,'boots',1.0);")
cursor.execute("INSERT INTO products VALUES(10,'coat',33.0);")
cursor.execute("INSERT INTO products VALUES(11,'accessories',81.0);")

for i in range(12, 12001):
    name = fake.word().title()
    price = round(random.uniform(5.0, 999.99), 2)
    cursor.execute("INSERT INTO products (id, name, price) VALUES (?, ?, ?)", [i, name, price])

cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        order_date TIMESTAMP,
        total_amount REAL,
        status TEXT,
        shipping_address TEXT,
        shipping_city TEXT,
        shipping_state TEXT,
        shipping_zip TEXT,
        payment_method TEXT,
        tracking_number TEXT,
        notes TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price REAL,
        discount REAL,
        tax REAL,
        total_price REAL,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        product_id INTEGER,
        user_id INTEGER,
        rating INTEGER,
        title TEXT,
        comment TEXT,
        helpful_count INTEGER,
        verified_purchase BOOLEAN,
        review_date TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS inventory_transactions (
        id INTEGER PRIMARY KEY,
        product_id INTEGER,
        transaction_type TEXT,
        quantity INTEGER,
        previous_quantity INTEGER,
        new_quantity INTEGER,
        transaction_date TIMESTAMP,
        reference_type TEXT,
        reference_id INTEGER,
        notes TEXT,
        performed_by TEXT,
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer_support_tickets (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        order_id INTEGER,
        ticket_number TEXT,
        category TEXT,
        priority TEXT,
        status TEXT,
        subject TEXT,
        description TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        resolved_at TIMESTAMP,
        assigned_to TEXT,
        resolution_notes TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (order_id) REFERENCES orders(id)
    )
""")

print("Generating users...")
users_data = []
for i in range(15000):
    if i % 1000 == 0:
        print(f"  Generated {i} users...")

    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zipcode = fake.zipcode()
    age = fake.random_int(min=18, max=85)
    created_at = fake.date_time_between(start_date="-3y", end_date="now")
    updated_at = fake.date_time_between(start_date=created_at, end_date="now")

    users_data.append(
        (first_name, last_name, email, phone_number, address, city, state, zipcode, age, created_at, updated_at)
    )

cursor.executemany(
    """
    INSERT INTO users (first_name, last_name, email, phone_number, address,
                      city, state, zipcode, age, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    users_data,
)

print("Generating orders...")
order_statuses = ["pending", "processing", "shipped", "delivered", "cancelled", "refunded"]
payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]

orders_data = []
for i in range(20000):
    if i % 2000 == 0:
        print(f"  Generated {i} orders...")

    user_id = random.randint(1, 15000)
    order_date = fake.date_time_between(start_date="-1y", end_date="now")
    total_amount = round(random.uniform(10.0, 5000.0), 2)
    status = random.choice(order_statuses)
    shipping_address = fake.street_address()
    shipping_city = fake.city()
    shipping_state = fake.state_abbr()
    shipping_zip = fake.zipcode()
    payment_method = random.choice(payment_methods)
    tracking_number = fake.ean13() if status in ["shipped", "delivered"] else None
    notes = fake.text(max_nb_chars=100) if random.random() < 0.3 else None

    orders_data.append(
        (
            user_id,
            order_date,
            total_amount,
            status,
            shipping_address,
            shipping_city,
            shipping_state,
            shipping_zip,
            payment_method,
            tracking_number,
            notes,
        )
    )

cursor.executemany(
    """
    INSERT INTO orders (user_id, order_date, total_amount, status, shipping_address,
                       shipping_city, shipping_state, shipping_zip, payment_method,
                       tracking_number, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    orders_data,
)

print("Generating order items...")
order_items_data = []
for order_id in range(1, 20001):
    if order_id % 2000 == 0:
        print(f"  Generated items for {order_id} orders...")

    num_items = random.randint(1, 8)
    for _ in range(num_items):
        product_id = random.randint(1, 12000)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(0.99, 999.99), 2)
        discount = round(random.uniform(0, 0.3) * unit_price, 2) if random.random() < 0.2 else 0
        tax = round((unit_price - discount) * quantity * 0.08, 2)
        total_price = round((unit_price - discount) * quantity + tax, 2)

        order_items_data.append((order_id, product_id, quantity, unit_price, discount, tax, total_price))

cursor.executemany(
    """
    INSERT INTO order_items (order_id, product_id, quantity, unit_price,
                            discount, tax, total_price)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""",
    order_items_data,
)

print("Generating reviews...")
reviews_data = []
for i in range(25000):
    if i % 2500 == 0:
        print(f"  Generated {i} reviews...")

    product_id = random.randint(1, 12000)
    user_id = random.randint(1, 15000)
    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 30, 40])[0]
    title = fake.catch_phrase()
    comment = fake.text(max_nb_chars=500)
    helpful_count = random.randint(0, 100)
    verified_purchase = random.choice([0, 1])
    review_date = fake.date_time_between(start_date="-1y", end_date="now")

    reviews_data.append((product_id, user_id, rating, title, comment, helpful_count, verified_purchase, review_date))

cursor.executemany(
    """
    INSERT INTO reviews (product_id, user_id, rating, title, comment,
                        helpful_count, verified_purchase, review_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""",
    reviews_data,
)

print("Generating inventory transactions...")
transaction_types = ["purchase", "sale", "return", "adjustment", "transfer", "damage"]
reference_types = ["order", "return", "adjustment", "transfer", "manual"]

inventory_data = []
for i in range(18000):
    if i % 2000 == 0:
        print(f"  Generated {i} inventory transactions...")

    product_id = random.randint(1, 12000)
    transaction_type = random.choice(transaction_types)
    quantity = random.randint(1, 100)
    previous_quantity = random.randint(0, 1000)
    new_quantity = (
        previous_quantity + quantity if transaction_type in ["purchase", "return"] else previous_quantity - quantity
    )
    new_quantity = max(0, new_quantity)
    transaction_date = fake.date_time_between(start_date="-6m", end_date="now")
    reference_type = random.choice(reference_types)
    reference_id = random.randint(1, 20000) if reference_type == "order" else random.randint(1, 1000)
    notes = fake.text(max_nb_chars=100) if random.random() < 0.3 else None
    performed_by = fake.name()

    inventory_data.append(
        (
            product_id,
            transaction_type,
            quantity,
            previous_quantity,
            new_quantity,
            transaction_date,
            reference_type,
            reference_id,
            notes,
            performed_by,
        )
    )

cursor.executemany(
    """
    INSERT INTO inventory_transactions (product_id, transaction_type, quantity, previous_quantity,
                                       new_quantity, transaction_date, reference_type, reference_id,
                                       notes, performed_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    inventory_data,
)

print("Generating customer support tickets...")
ticket_categories = ["shipping", "product", "payment", "account", "return", "technical", "other"]
priorities = ["low", "medium", "high", "urgent"]
ticket_statuses = ["open", "in_progress", "waiting_customer", "resolved", "closed"]

tickets_data = []
for i in range(10000):
    if i % 1000 == 0:
        print(f"  Generated {i} support tickets...")

    user_id = random.randint(1, 15000)
    order_id = random.randint(1, 20000) if random.random() < 0.7 else None
    ticket_number = f"TICKET-{fake.random_int(min=100000, max=999999)}"
    category = random.choice(ticket_categories)
    priority = random.choice(priorities)
    status = random.choice(ticket_statuses)
    subject = fake.catch_phrase()
    description = fake.text(max_nb_chars=1000)
    created_at = fake.date_time_between(start_date="-6m", end_date="now")
    updated_at = fake.date_time_between(start_date=created_at, end_date="now")
    resolved_at = (
        fake.date_time_between(start_date=updated_at, end_date="now") if status in ["resolved", "closed"] else None
    )
    assigned_to = fake.name() if status != "open" else None
    resolution_notes = fake.text(max_nb_chars=500) if status in ["resolved", "closed"] else None

    tickets_data.append(
        (
            user_id,
            order_id,
            ticket_number,
            category,
            priority,
            status,
            subject,
            description,
            created_at,
            updated_at,
            resolved_at,
            assigned_to,
            resolution_notes,
        )
    )

cursor.executemany(
    """
    INSERT INTO customer_support_tickets (user_id, order_id, ticket_number, category, priority,
                                         status, subject, description, created_at, updated_at,
                                         resolved_at, assigned_to, resolution_notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    tickets_data,
)

cursor.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY,
        title TEXT,
        subtitle TEXT,
        author TEXT,
        category TEXT,
        tags TEXT,
        summary TEXT,
        body TEXT,
        published_at TIMESTAMP,
        updated_at TIMESTAMP
    )
""")

print("Generating articles for FTS testing...")
categories = ["technology", "science", "business", "health", "politics", "entertainment", "sports", "lifestyle"]

articles_data = []
for i in range(10000):
    if i % 1000 == 0:
        print(f"  Generated {i} articles...")

    title = fake.sentence(nb_words=random.randint(5, 12)).rstrip('.')
    subtitle = fake.sentence(nb_words=random.randint(8, 15))
    author = fake.name()
    category = random.choice(categories)
    # 3-6 comma-separated tags
    tags = ", ".join(fake.words(nb=random.randint(3, 6)))
    # 2-4 sentence summary
    summary = " ".join(fake.sentences(nb=random.randint(2, 4)))
    # 20-50 paragraphs body
    body = "\n\n".join(fake.paragraphs(nb=random.randint(20, 50)))
    published_at = fake.date_time_between(start_date="-2y", end_date="now")
    updated_at = fake.date_time_between(start_date=published_at, end_date="now")

    articles_data.append((title, subtitle, author, category, tags, summary, body, published_at, updated_at))

cursor.executemany(
    """
    INSERT INTO articles (title, subtitle, author, category, tags, summary, body, published_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    articles_data,
)


print("Creating indexes...")
cursor.execute("CREATE INDEX age_idx on users (age)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON inventory_transactions(product_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_tickets_user_id ON customer_support_tickets(user_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_tickets_status ON customer_support_tickets(status)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_author ON articles(author)")
conn.commit()

# Print summary statistics
print("\n=== Database Generation Complete ===")
print(f"Users: {cursor.execute('SELECT COUNT(*) FROM users').fetchone()[0]:,}")
print(f"Products: {cursor.execute('SELECT COUNT(*) FROM products').fetchone()[0]:,}")
print(f"Orders: {cursor.execute('SELECT COUNT(*) FROM orders').fetchone()[0]:,}")
print(f"Order Items: {cursor.execute('SELECT COUNT(*) FROM order_items').fetchone()[0]:,}")
print(f"Reviews: {cursor.execute('SELECT COUNT(*) FROM reviews').fetchone()[0]:,}")
print(f"Inventory Transactions: {cursor.execute('SELECT COUNT(*) FROM inventory_transactions').fetchone()[0]:,}")
print(f"Support Tickets: {cursor.execute('SELECT COUNT(*) FROM customer_support_tickets').fetchone()[0]:,}")
print(f"Articles: {cursor.execute('SELECT COUNT(*) FROM articles').fetchone()[0]:,}")
# Calculate approximate database size
cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
size_bytes = cursor.fetchone()[0]
print(f"\nApproximate database size: {size_bytes / (1024 * 1024):.2f} MB")
conn.close()
print("\nDatabase created successfully!")
