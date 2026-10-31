---
display_name: "a built-in MCP server"
---

# MCP Server - Model Context Protocol

## Overview

Turso includes a built-in MCP (Model Context Protocol) server that allows AI assistants and other tools to interact with your databases programmatically.

## Starting the MCP Server

To start Turso in MCP server mode, use the `--mcp` flag:

```bash
/path/to/tursodb --mcp
```

This will start an MCP server that listens on stdio for JSON-RPC requests. Without a database path the server uses an in-memory database. Pass a path to open a file instead (`/path/to/tursodb app.db --mcp`), or switch databases later with the `open_database` tool.

## Available Tools

The MCP server exposes the following tools. Clients call a tool with a JSON-RPC `tools/call` request that gives the tool name and its arguments:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_query",
    "arguments": {
      "query": "SELECT * FROM users WHERE age > 21"
    }
  }
}
```

The examples below show the `arguments` object for each tool.

### `open_database`
Open or create a database file. Creates parent directories if needed.

**Parameters:**
- `path` (string, required): Path to the database file (absolute or relative). Use `:memory:` for an in-memory database.

**Example:**
```json
{ "path": "./data/app.db" }
```

### `current_database`
Get the path of the currently open database.

**Example:**
```json
{}
```

### `list_tables`
List all tables in the database.

**Example:**
```json
{}
```

### `describe_table`
Describe the columns of a table.

**Parameters:**
- `table_name` (string, required): Name of the table to describe

**Example:**
```json
{ "table_name": "users" }
```

### `execute_query`
Execute a single read-only SELECT query.

**Parameters:**
- `query` (string, required): The SELECT query to execute

**Example:**
```json
{ "query": "SELECT * FROM users WHERE age > 21" }
```

### `insert_data`
Execute a single INSERT statement.

**Parameters:**
- `query` (string, required): The INSERT statement to execute

**Example:**
```json
{ "query": "INSERT INTO users (name, age) VALUES ('Alice', 30)" }
```

### `update_data`
Execute a single UPDATE statement.

**Parameters:**
- `query` (string, required): The UPDATE statement to execute

**Example:**
```json
{ "query": "UPDATE users SET age = 31 WHERE name = 'Alice'" }
```

### `delete_data`
Execute a single DELETE statement.

**Parameters:**
- `query` (string, required): The DELETE statement to execute

**Example:**
```json
{ "query": "DELETE FROM users WHERE age < 18" }
```

### `schema_change`
Execute a single schema modification statement (CREATE TABLE, ALTER TABLE, DROP TABLE).

**Parameters:**
- `query` (string, required): The schema modification statement to execute

**Example:**
```json
{ "query": "CREATE TABLE users (name TEXT, age INTEGER)" }
```

## Integration with AI Assistants

### Claude Desktop

To use with Claude Desktop, add the following to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "turso": {
      "command": "/path/to/tursodb",
      "args": ["--mcp"]
    }
  }
}
```

Note: You must use the full path to the tursodb executable as Claude Desktop may not recognize items in your PATH.

### Other MCP Clients

The Turso MCP server follows the standard MCP protocol and can be used with any MCP-compatible client.

## Example Session

Here's an example of using the MCP server:

1. **Start the server:**
   ```bash
   /path/to/tursodb --mcp
   ```

2. **Query data:**
   ```
   > What tables are in the database?
   [Uses list_tables tool]

   > Show me all users older than 25
   [Uses execute_query tool with "SELECT * FROM users WHERE age > 25"]
   ```

3. **Modify data:**
   ```
   > Add a new user named Bob who is 28 years old
   [Uses insert_data tool with an INSERT statement]
   ```

## Troubleshooting

### Server doesn't start
- Ensure the tursodb executable path is correct
- Check that you're using the full path to the executable

### Commands fail
- Verify SQL syntax is correct
- Check that tables and columns exist
- Ensure you have write permissions if modifying data

## See Also

- MCP Protocol Documentation: https://modelcontextprotocol.io
- Turso Documentation: https://turso.tech/docs