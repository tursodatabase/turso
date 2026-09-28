# Turso Multiprocess WAL Coordination Specification

TLA+ specification for how Turso coordinates readers, the writer, and
checkpoints when several processes open the same database in WAL mode.
The specification itself (`MultiprocessWal.tla`) is the primary
documentation — generate the PDF for a typeset version.

## Quick Start

```bash
# Model check (about 32 million distinct states, a few minutes)
make check

# Generate PDF
make MultiprocessWal.pdf
```
