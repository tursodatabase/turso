# Python Bindings Examples

## Prerequisites

Build and install the Python bindings:

```bash
cd bindings/python
pip install maturin
maturin develop
```

## basic.py

Basic example demonstrating local embedded database usage.

```bash
python examples/basic.py
```

## sync_example.py

Demonstrates syncing with Turso Cloud, including optional remote encryption support.

```bash
export TURSO_REMOTE_URL="http://your-db.localhost:8080"
export TURSO_AUTH_TOKEN="your-auth-token"  # optional
export TURSO_REMOTE_ENCRYPTION_KEY="base64-encoded-key"  # optional
python examples/sync_example.py
```