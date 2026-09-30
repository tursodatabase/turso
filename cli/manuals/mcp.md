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

This will start an MCP server that listens on stdio for commands. The server starts without a database connection, allowing you to select or create databases using MCP commands.

## Available Tools

The MCP server exposes the following tools:

### `query`
Execute a SQL query and get results.

**Parameters:**
- `sql` (string, required): The SQL query to execute

**Example:**
```json
{
  "tool": "query",
  "arguments": {
    "sql": "SELECT * FROM users WHERE age > 21"
  }
}
```

### `execute`
Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).

**Parameters:**
- `sql` (string, required): The SQL statement to execute

**Example:**
```json
{
  "tool": "execute",
  "arguments": {
    "sql": "INSERT INTO users (name, age) VALUES ('Alice', 30)"
  }
}
```

### `list_tables`
List all tables in the database.

**Example:**
```json
{
  "tool": "list_tables",
  "arguments": {}
}
```

### `describe_table`
Get the schema of a specific table.

**Parameters:**
- `table` (string, required): The name of the table to describe

**Example:**
```json
{
  "tool": "describe_table",
  "arguments": {
    "table": "users"
  }
}
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
   [Uses query tool with "SELECT * FROM users WHERE age > 25"]
   ```

3. **Modify data:**
   ```
   > Add a new user named Bob who is 28 years old
   [Uses execute tool with INSERT statement]
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