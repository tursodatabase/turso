# Turso Manual Pages

Welcome to the Turso manual pages. These pages provide detailed documentation for various features and capabilities.

## Available Manuals

### custom-types - Custom Types for STRICT Tables
Define user-defined types with custom encode/decode logic, operator overloads, and defaults for STRICT tables.

```
.manual custom-types
```

### cdc - Change Data Capture
Track and capture all data changes made to your database tables for replication, syncing, and reactive applications.

```
.manual cdc
```

### encryption - At-Rest Database Encryption
Protect your database files with transparent encryption using AES-GCM or high-performance AEGIS ciphers.

```
.manual encryption
```

### mcp - Model Context Protocol
Learn about Turso's built-in MCP server that enables AI assistants and other tools to interact with your databases.

```
.manual mcp
```

### vector - Vector Search
Build similarity search and semantic search applications using vector embeddings and distance functions.

```
.manual vector
```

### materialized-views - Live Materialized Views
Create automatically updating views that use Incremental View Maintenance to stay current with minimal overhead.

```
.manual materialized-views
```

## Usage

To view a manual page, use the `.manual` or `.man` command:

```
.manual <page>    # Full command
.man <page>       # Short alias
```

### Examples

```
.manual mcp       # View the MCP server documentation
.man mcp          # Same as above, using the alias
```

## Adding More Manuals

Additional manual pages will be added for other features as they become available.