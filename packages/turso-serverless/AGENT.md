# Agent Development Guide

This document provides guidance for LLMs working on the `@tursodatabase/serverless` TypeScript driver.

## Project Overview

This is a **fetch() API-compatible serverless database driver** for Turso Cloud that implements the SQL over HTTP protocol (internally called "hrana"). It's designed for serverless and edge compute environments like Cloudflare Workers and Vercel Edge Functions.

### Key Features
- **HTTP-based SQL execution** using the v3 cursor endpoint for streaming
- **Native streaming API** with Connection/Statement pattern
- **LibSQL compatibility layer** for drop-in replacement
- **TypeScript-first** with full type safety
- **Edge-optimized** using only `fetch()` API

## Architecture

### Core Files Structure
```
src/
├── connection.ts    # Connection class and connect() function
├── statement.ts     # Statement class with get()/all()/iterate() methods
├── protocol.ts      # Low-level SQL over HTTP protocol implementation
├── compat.ts        # LibSQL API compatibility layer
├── compat/index.ts  # Compatibility layer exports
└── index.ts         # Main package exports
```

### Package Exports
- **Main API**: `@tursodatabase/serverless` - Native streaming API
- **Compatibility**: `@tursodatabase/serverless/compat` - LibSQL-compatible API

## Native API Design

### Connection/Statement Pattern
```typescript
import { connect } from "@tursodatabase/serverless";

const client = connect({ url, authToken });
const stmt = client.prepare("SELECT * FROM users WHERE id = ?", [123]);

// Three execution modes:
const row = await stmt.get();        // First row or null
const rows = await stmt.all();       // All rows as array  
for await (const row of stmt.iterate()) { ... } // Streaming iterator
```

### Key Classes

#### Connection
- **Purpose**: Database connection and session management
- **Methods**: `prepare()`, `execute()`, `batch()`, `executeRaw()`
- **Internal**: Manages baton tokens, base URL updates, cursor streaming

#### Statement  
- **Purpose**: Prepared statement execution with multiple access patterns
- **Methods**: `get()`, `all()`, `iterate()`
- **Streaming**: `iterate()` provides row-by-row streaming via AsyncGenerator

#### Protocol Layer
- **Purpose**: HTTP cursor endpoint communication
- **Key Function**: `executeCursor()` returns streaming cursor entries
- **Protocol**: Uses v3 cursor endpoint (`/v3/cursor`) with newline-delimited JSON

## LibSQL Compatibility Layer

### Purpose
Provides drop-in compatibility with the standard libSQL client API for existing applications.

### Key Differences
- **Entry Point**: `createClient()` instead of `connect()`
- **Import Path**: `@tursodatabase/serverless/compat`
- **API Surface**: Matches libSQL client interface exactly
- **Config Validation**: Only supports `url` and `authToken`, validates against unsupported options

### Supported vs Unsupported
```typescript
// ✅ Supported
const client = createClient({ url, authToken });
await client.execute(sql, args);
await client.batch(statements);

// ✅ Supported: remote encryption key for encrypted Turso Cloud databases
createClient({ url, authToken, remoteEncryptionKey: "base64-encoded-key" });

// ❌ Unsupported (throws LibsqlError)
createClient({ url, authToken, encryptionKey: "..." }); // Validation error
await client.transaction(); // Not implemented
await client.sync(); // Not supported for remote
```

## Protocol Implementation

### SQL over HTTP (v3 Cursor)
- **Endpoint**: `POST /v3/cursor`
- **Request**: JSON with baton, batch steps
- **Response**: Streaming newline-delimited JSON entries
- **Entry Types**: `step_begin`, `row`, `step_end`, `step_error`, `error`

### Session Management
- **Baton Tokens**: Maintain session continuity across requests
- **Base URL Updates**: Handle server-side redirects/load balancing
- **URL Normalization**: Convert `libsql://` to `https://` automatically

## Testing Strategy

### Integration Tests
```
integration-tests/
├── serverless.test.mjs  # Native API tests
└── compat.test.mjs      # Compatibility layer tests
```

### Test Requirements
- **Environment Variables**: `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`
- **Serial Execution**: All tests use `test.serial()` to avoid conflicts
- **Real Database**: Tests run against actual Turso instance

### Running Tests
```bash
npm test  # Runs all integration tests
npm run build  # TypeScript compilation
```

## Development Guidelines

### Code Organization
- **Single Responsibility**: Each file has a clear, focused purpose
- **Type Safety**: Full TypeScript coverage with proper imports
- **Error Handling**: Use proper error classes (`LibsqlError` for compat)
- **Streaming First**: Leverage AsyncGenerator for memory efficiency

### Key Patterns
- **Protocol Abstraction**: Keep protocol details in `protocol.ts`
- **Compatibility Isolation**: LibSQL compatibility in separate module
- **Row Objects**: Arrays with column name properties (non-enumerable)
- **Config Validation**: Explicit validation with helpful error messages

### Performance Considerations
- **Streaming**: Use `iterate()` for large result sets
- **Memory**: Cursor endpoint provides constant memory usage
- **Latency**: First results available immediately with streaming

## Common Tasks

### Adding New Features
1. **Protocol**: Add to `protocol.ts` if it requires HTTP changes
2. **Connection**: Add to `connection.ts` for connection-level features  
3. **Statement**: Add to `statement.ts` for statement-level features
4. **Compatibility**: Update `compat.ts` if LibSQL compatibility needed
5. **Tests**: Add integration tests for new functionality

### Debugging Issues
1. **Check Protocol**: Use `executeRaw()` to inspect cursor entries
2. **Validate Config**: Ensure URL/auth token are correct
3. **Test Streaming**: Compare `all()` vs `iterate()` behavior
4. **Review Errors**: Check for `LibsqlError` vs generic errors

### Extending Compatibility
1. **Research LibSQL**: Check `resources/libsql-client-ts` for API patterns
2. **Validate Config**: Add validation for unsupported options
3. **Map Interfaces**: Convert between LibSQL and native formats
4. **Test Coverage**: Ensure compatibility tests cover new features

## Important Notes

### Security
- **No Secret Logging**: Never log auth tokens or sensitive data
- **Validation**: Always validate inputs, especially in compatibility layer
- **Error Messages**: Don't expose internal implementation details

### Compatibility
- **Breaking Changes**: Avoid breaking the native API
- **LibSQL Parity**: Match LibSQL behavior exactly in compatibility layer
- **Version Support**: Document which libSQL features are supported

### Edge Cases
- **Large Results**: Test with large datasets to verify streaming
- **Network Issues**: Handle connection failures gracefully
- **Protocol Evolution**: Be prepared for protocol version updates

## Future Considerations

### Potential Enhancements
- **Transaction Support**: Interactive transactions in compatibility layer
- **Prepared Statement Caching**: Cache prepared statements
- **Connection Pooling**: Multiple concurrent connections
- **Protocol Negotiation**: Support multiple protocol versions

### Monitoring
- **Performance Metrics**: Track query latency and throughput
- **Error Rates**: Monitor protocol and application errors
- **Resource Usage**: Memory and CPU usage in serverless environments

This guide should help future contributors understand the architecture and maintain consistency across the codebase.