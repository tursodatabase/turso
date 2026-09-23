# Lean proofs of MVCC garbage collection in Truncate checkpoint mode

check:
	lake build

search:
	lake exe search 3 3 2 30000000 --no-backfill-behind --check-invariant

counterexample:
	lake exe search 3 3 2 30000000 || true

clean:
	rm -rf .lake

.PHONY: check search counterexample clean
