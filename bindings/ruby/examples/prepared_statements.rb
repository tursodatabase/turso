#!/usr/bin/env ruby
# frozen_string_literal: true

# Prepared statement example for Turso Ruby bindings

require "turso"

Turso.open(":memory:") do |conn|
  conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")

  # Use prepared statement for batch inserts
  products = [
    ["Widget", 9.99],
    ["Gadget", 19.99],
    ["Gizmo", 29.99],
    ["Doodad", 4.99]
  ]

  stmt = conn.prepare("INSERT INTO products (name, price) VALUES (?, ?)")

  products.each do |name, price|
    stmt.execute(name, price)
    stmt.reset!
  end

  stmt.finalize!

  # Query with prepared statement
  select_stmt = conn.prepare("SELECT * FROM products WHERE price > ?")
  results = select_stmt.execute(10.0)

  puts "Products over $10:"
  results.each do |row|
    puts "  #{row[:name]}: $#{row[:price]}"
  end

  select_stmt.finalize!
end
