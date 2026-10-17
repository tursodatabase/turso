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

The server exposes these tools through `tools/list`:

| Tool | Purpose | Required argument |
| --- | --- | --- |
| `open_database` | Open or create a database file | `path` |
| `current_database` | Show the active database path | None |
| `list_tables` | List tables in the active database | None |
| `describe_table` | Show a table's columns | `table_name` |
| `execute_query` | Run a read-only `SELECT` query | `query` |
| `insert_data` | Run one `INSERT` statement | `query` |
| `update_data` | Run one `UPDATE` statement | `query` |
| `delete_data` | Run one `DELETE` statement | `query` |
| `schema_change` | Run one schema modification statement | `query` |

Each `tools/call` request passes the tool name and its arguments in `params`:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "execute_query",
    "arguments": { "query": "SELECT * FROM users WHERE age > 21" }
  }
}
```

Use `execute_query` only for `SELECT`. Data changes must use the matching `insert_data`, `update_data`, or `delete_data` tool. Schema changes such as `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` use `schema_change`. Each SQL tool accepts one statement.

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

2. If you started without a database path, call `open_database` with a `path` first. Then use `list_tables` to inspect the database and `describe_table` with a `table_name` argument to inspect a table.

3. Use `execute_query` with a `query` argument for reads, and the corresponding write or schema tool for changes.

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
