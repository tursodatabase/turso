# zero to one

* revisit line codec
* add columns from schema on merge
* Plugging JSON
* replug examples
* move datetime to quickwit common
* switch to nanos
* reintroduce the gcd map.

# Perf and Size
* remove alloc in `ord_to_term`
+ multivaued range queries restart from the beginning all of the time.
* re-add ZSTD compression for dictionaries
no systematic monotonic mapping
consider removing multilinear
f32?
adhoc solution for bool?
add metrics helper for aggregate. sum(row_id)
review inline absence/presence
improv perf of select using PDEP
compare with roaring bitmap/elias fano etc etc.
SIMD range? (see blog post)
Add alignment?
Consider another codec to bridge the gap between few and 5k elements

# Cleanup and rationalization
in benchmark, unify percent vs ratio, f32 vs f64.
investigate if should have better errors? io::Error is overused at the moment.
rename rank/select in unit tests
Review the public API via cargo doc
go through TODOs
remove all  doc_id occurrences -> row_id
use the rank & select naming in unit tests branch.
multi-linear -> blockwise
linear codec -> simply a multiplication for the index column
rename columnar to something more explicit, like column_dictionary or columnar_table
rename fastfield -> column
document changes
rationalization FastFieldValue, HasColumnType
isolate u128_based and uniform naming

# Other
fix enhance column-cli

# Santa Claus
autodetect datetime ipaddr, plug customizable tokenizer.
