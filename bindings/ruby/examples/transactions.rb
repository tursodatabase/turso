#!/usr/bin/env ruby
# frozen_string_literal: true

# Transaction example for Turso Ruby bindings

require "turso"

Turso.open(":memory:") do |conn|
  conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER)")
  conn.execute("INSERT INTO accounts (name, balance) VALUES (?, ?)", "Alice", 1000)
  conn.execute("INSERT INTO accounts (name, balance) VALUES (?, ?)", "Bob", 500)

  # Transfer money using a transaction
  conn.transaction do
    conn.execute("UPDATE accounts SET balance = balance - 100 WHERE name = ?", "Alice")
    conn.execute("UPDATE accounts SET balance = balance + 100 WHERE name = ?", "Bob")
    puts "Transfer complete!"
  end

  # Check balances
  conn.execute("SELECT name, balance FROM accounts").each do |row|
    puts "#{row[:name]}: $#{row[:balance]}"
  end

  # Transaction with rollback on error
  begin
    conn.transaction do
      conn.execute("UPDATE accounts SET balance = balance - 2000 WHERE name = ?", "Alice")
      raise "Insufficient funds!" # This causes rollback
    end
  rescue => e
    puts "Transaction rolled back: #{e.message}"
  end
end
