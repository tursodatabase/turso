#!/usr/bin/env ruby
# frozen_string_literal: true


require "turso"

Turso.open(":memory:") do |conn|
  conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

  conn.execute("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")
  conn.execute("INSERT INTO users (name, email) VALUES (?, ?)", "Bob", "bob@example.com")

  puts "Last insert row ID: #{conn.last_insert_row_id}"

  results = conn.execute("SELECT * FROM users")
  results.each do |row|
    puts "User: #{row[:name]} <#{row[:email]}>"
  end
end
