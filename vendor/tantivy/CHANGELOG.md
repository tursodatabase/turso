Tantivy 0.26.2
================================

## Bugfixes
- Fix missing local buffer flushes in nested aggregations that caused incorrect metric results for low-cardinality terms buckets [#2992](https://github.com/quickwit-oss/tantivy/issues/2992)(@PSeitz)
- Disable the buffered union `seek_danger` override to avoid consuming unaligned scorers [#3086](https://github.com/quickwit-oss/tantivy/issues/3086)(@PSeitz)

Tantivy 0.26.1
================================

## Bugfixes
- Fix memory consumption accounting in nested term aggregation to only scan the active parent bucket (@PSeitz)
- Fix memory consumption accounting in composite aggregation to only scan the active parent bucket (@PSeitz)

Tantivy 0.26 (Unreleased)
================================

## Bugfixes
- Align float query coercion during search with the columnar coercion rules [#2692](https://github.com/quickwit-oss/tantivy/pull/2692)(@fulmicoton)
- Fix lenient elastic range queries with trailing closing parentheses [#2816](https://github.com/quickwit-oss/tantivy/pull/2816)(@evance-br)
- Fix intersection `seek()` advancing below current doc id [#2812](https://github.com/quickwit-oss/tantivy/pull/2812)(@fulmicoton)
- Fix phrase query prefixed with `*` [#2751](https://github.com/quickwit-oss/tantivy/pull/2751)(@Darkheir)
- Fix `vint` buffer overflow during index creation [#2778](https://github.com/quickwit-oss/tantivy/pull/2778)(@rebasedming)
- Fix integer overflow in `ExpUnrolledLinkedList` for large datasets [#2735](https://github.com/quickwit-oss/tantivy/pull/2735)(@mdashti)
- Fix integer overflow in segment sorting and merge policy truncation [#2846](https://github.com/quickwit-oss/tantivy/pull/2846)(@anaslimem)
- Fix merging of intermediate aggregation results [#2719](https://github.com/quickwit-oss/tantivy/pull/2719)(@PSeitz)
- Fix deduplicate doc counts in term aggregation for multi-valued fields [#2854](https://github.com/quickwit-oss/tantivy/pull/2854)(@nuri-yoo)

## Features/Improvements
- **Aggregation**
    - Add filter aggregation [#2711](https://github.com/quickwit-oss/tantivy/pull/2711)(@mdashti)
    - Add include/exclude filtering for term aggregations [#2717](https://github.com/quickwit-oss/tantivy/pull/2717)(@PSeitz)
    - Add public accessors for intermediate aggregation results [#2829](https://github.com/quickwit-oss/tantivy/pull/2829)(@congx4)
    - Replace HyperLogLog++ with Apache DataSketches HLL for cardinality aggregation [#2837](https://github.com/quickwit-oss/tantivy/pull/2837) [#2842](https://github.com/quickwit-oss/tantivy/pull/2842)(@congx4)
    - Add composite aggregation [#2856](https://github.com/quickwit-oss/tantivy/pull/2856)(@fulmicoton)
- **Fast Fields**
    - Add fast field fallback for `TermQuery` when the field is not indexed [#2693](https://github.com/quickwit-oss/tantivy/pull/2693)(@PSeitz-dd)
    - Add fast field support for `Bytes` values [#2830](https://github.com/quickwit-oss/tantivy/pull/2830)(@mdashti)
- **Query Parser**
    - Add support for regexes in the query grammar [#2677](https://github.com/quickwit-oss/tantivy/pull/2677) [#2818](https://github.com/quickwit-oss/tantivy/pull/2818)(@Darkheir)
    - Deduplicate queries in query parser [#2698](https://github.com/quickwit-oss/tantivy/pull/2698)(@PSeitz-dd)
- Add erased `SortKeyComputer` for sorting on column types unknown until runtime [#2770](https://github.com/quickwit-oss/tantivy/pull/2770) [#2790](https://github.com/quickwit-oss/tantivy/pull/2790)(@stuhood @PSeitz)
- Add natural-order-with-none-highest support in `TopDocs::order_by` [#2780](https://github.com/quickwit-oss/tantivy/pull/2780)(@stuhood)
- Move stemming behing `stemmer` feature flag [#2791](https://github.com/quickwit-oss/tantivy/pull/2791)(@fulmicoton)
- Make `DeleteMeta`, `AddOperation`, `advance_deletes`, `with_max_doc`, `serializer` module, and `delete_queue` public [#2762](https://github.com/quickwit-oss/tantivy/pull/2762) [#2765](https://github.com/quickwit-oss/tantivy/pull/2765) [#2766](https://github.com/quickwit-oss/tantivy/pull/2766) [#2835](https://github.com/quickwit-oss/tantivy/pull/2835)(@philippemnoel @PSeitz)
- Make `Language` hashable [#2763](https://github.com/quickwit-oss/tantivy/pull/2763)(@philippemnoel)
- Improve `space_usage` reporting for JSON fields and columnar data [#2761](https://github.com/quickwit-oss/tantivy/pull/2761)(@PSeitz-dd)
- Split `Term` into `Term` and `IndexingTerm` [#2744](https://github.com/quickwit-oss/tantivy/pull/2744) [#2750](https://github.com/quickwit-oss/tantivy/pull/2750)(@PSeitz-dd @PSeitz)

## Performance
- **Aggregation**
    - Large speed up and memory reduction for nested high cardinality aggregations by using one collector per request instead of one per bucket, and adding `PagedTermMap` for faster medium cardinality term aggregations [#2715](https://github.com/quickwit-oss/tantivy/pull/2715) [#2759](https://github.com/quickwit-oss/tantivy/pull/2759)(@PSeitz @PSeitz-dd)
    - Optimize low-cardinality term aggregations by using a `Vec` instead of a `HashMap` [#2740](https://github.com/quickwit-oss/tantivy/pull/2740)(@fulmicoton-dd)
- Optimize `ExistsQuery` for a high number of dynamic columns [#2694](https://github.com/quickwit-oss/tantivy/pull/2694)(@PSeitz-dd)
- Add lazy scorers to stop score evaluation early when a doc won't reach the top-K threshold [#2726](https://github.com/quickwit-oss/tantivy/pull/2726) [#2777](https://github.com/quickwit-oss/tantivy/pull/2777)(@fulmicoton @stuhood)
- Add `DocSet::cost()` and use it to order scorers in intersections [#2707](https://github.com/quickwit-oss/tantivy/pull/2707)(@PSeitz)
- Add `collect_block` support for collector wrappers [#2727](https://github.com/quickwit-oss/tantivy/pull/2727)(@stuhood)
- Optimize saturated posting lists by replacing them with `AllScorer` in boolean queries [#2745](https://github.com/quickwit-oss/tantivy/pull/2745) [#2760](https://github.com/quickwit-oss/tantivy/pull/2760) [#2774](https://github.com/quickwit-oss/tantivy/pull/2774)(@fulmicoton @mdashti @trinity-1686a)
- Add `seek_danger` on `DocSet` for more efficient intersections [#2538](https://github.com/quickwit-oss/tantivy/pull/2538) [#2810](https://github.com/quickwit-oss/tantivy/pull/2810)(@PSeitz @stuhood @fulmicoton)
- Skip column traversal in `RangeDocSet` when query range does not overlap with column bounds [#2783](https://github.com/quickwit-oss/tantivy/pull/2783)(@ChangRui-Ryan)
- Speed up exclude queries by supporting multiple excluded `DocSet`s without intermediate union [#2825](https://github.com/quickwit-oss/tantivy/pull/2825)(@PSeitz)
- Improve union performance for non-score unions with `fill_buffer` and optimized `TinySet` [#2863](https://github.com/quickwit-oss/tantivy/pull/2863)(@PSeitz)

Tantivy 0.25
================================

## Bugfixes
- fix union performance regression in tantivy 0.24 [#2663](https://github.com/quickwit-oss/tantivy/pull/2663)(@PSeitz)
- make zstd optional in sstable [#2633](https://github.com/quickwit-oss/tantivy/pull/2633)(@Parth)
- Fix TopDocs::order_by_string_fast_field for asc order [#2672](https://github.com/quickwit-oss/tantivy/pull/2672)(@stuhood @PSeitz)

## Features/Improvements
- add docs/example and Vec<u32> values to sstable [#2660](https://github.com/quickwit-oss/tantivy/pull/2660)(@PSeitz)
- Add string fast field support to `TopDocs`. [#2642](https://github.com/quickwit-oss/tantivy/pull/2642)(@stuhood)
- update edition to 2024 [#2620](https://github.com/quickwit-oss/tantivy/pull/2620)(@PSeitz)
- Allow optional spaces between the field name and the value in the query parser [#2678](https://github.com/quickwit-oss/tantivy/pull/2678)(@Darkheir)
- Support mixed field types in query parser [#2676](https://github.com/quickwit-oss/tantivy/pull/2676)(@trinity-1686a)
- Add per-field size details [#2679](https://github.com/quickwit-oss/tantivy/pull/2679)(@fulmicoton)

Tantivy 0.24.2
================================
- Fix TopNComputer for reverse order. [#2672](https://github.com/quickwit-oss/tantivy/pull/2672)(@stuhood @PSeitz) 

Affected queries are [order_by_fast_field](https://docs.rs/tantivy/latest/tantivy/collector/struct.TopDocs.html#method.order_by_fast_field) and
[order_by_u64_field](https://docs.rs/tantivy/latest/tantivy/collector/struct.TopDocs.html#method.order_by_u64_field)
for `Order::Asc`

Tantivy 0.24.1
================================
- Fix: bump required rust version to 1.81
  
Tantivy 0.24
================================
Tantivy 0.24 will be backwards compatible with indices created with v0.22 and v0.21. The new minimum rust version will be 1.75. Tantivy 0.23 will be skipped.

#### Bugfixes
- fix potential endless loop in merge [#2457](https://github.com/quickwit-oss/tantivy/pull/2457)(@PSeitz)
- fix bug that causes out-of-order sstable key. [#2445](https://github.com/quickwit-oss/tantivy/pull/2445)(@fulmicoton)
- fix ReferenceValue API flaw [#2372](https://github.com/quickwit-oss/tantivy/pull/2372)(@PSeitz)
- fix `OwnedBytes` debug panic [#2512](https://github.com/quickwit-oss/tantivy/pull/2512)(@b41sh)
- catch panics during merges [#2582](https://github.com/quickwit-oss/tantivy/pull/2582)(@rdettai)
- switch from u32 to usize in bitpacker. This enables multivalued columns larger than 4GB, which crashed during merge before. [#2581](https://github.com/quickwit-oss/tantivy/pull/2581) [#2586](https://github.com/quickwit-oss/tantivy/pull/2586)(@fulmicoton-dd @PSeitz)

#### Breaking API Changes
- remove index sorting [#2434](https://github.com/quickwit-oss/tantivy/pull/2434)(@PSeitz)

#### Features/Improvements
- **Aggregation**
    - Support for cardinality aggregation [#2337](https://github.com/quickwit-oss/tantivy/pull/2337) [#2446](https://github.com/quickwit-oss/tantivy/pull/2446) (@raphaelcoeffic @PSeitz)
    - Support for extended stats aggregation [#2247](https://github.com/quickwit-oss/tantivy/pull/2247)(@giovannicuccu)
    - Add Key::I64 and Key::U64 variants in aggregation to avoid f64 precision issues [#2468](https://github.com/quickwit-oss/tantivy/pull/2468)(@PSeitz)
    - Faster term aggregation fetch terms [#2447](https://github.com/quickwit-oss/tantivy/pull/2447)(@PSeitz)
    - Improve custom order deserialization [#2451](https://github.com/quickwit-oss/tantivy/pull/2451)(@PSeitz)
    - Change AggregationLimits behavior [#2495](https://github.com/quickwit-oss/tantivy/pull/2495)(@PSeitz)
    - lower contention on AggregationLimits [#2394](https://github.com/quickwit-oss/tantivy/pull/2394)(@PSeitz)
    - fix postcard compatibility for top_hits, add postcard test [#2346](https://github.com/quickwit-oss/tantivy/pull/2346)(@PSeitz)
    - reduce top hits memory consumption [#2426](https://github.com/quickwit-oss/tantivy/pull/2426)(@PSeitz)
    - check unsupported parameters top_hits [#2351](https://github.com/quickwit-oss/tantivy/pull/2351)(@PSeitz)
    - Change AggregationLimits to AggregationLimitsGuard [#2495](https://github.com/quickwit-oss/tantivy/pull/2495)(@PSeitz)
    - add support for counting non integer in aggregation [#2547](https://github.com/quickwit-oss/tantivy/pull/2547)(@trinity-1686a)
- **Range Queries**
    - Support fast field range queries on json fields [#2456](https://github.com/quickwit-oss/tantivy/pull/2456)(@PSeitz)
    - Add support for str fast field range query [#2460](https://github.com/quickwit-oss/tantivy/pull/2460) [#2452](https://github.com/quickwit-oss/tantivy/pull/2452) [#2453](https://github.com/quickwit-oss/tantivy/pull/2453)(@PSeitz)
    - modify fastfield range query heuristic [#2375](https://github.com/quickwit-oss/tantivy/pull/2375)(@trinity-1686a)
    - add FastFieldRangeQuery for explicit range queries on fast field (for `RangeQuery` it is autodetected) [#2477](https://github.com/quickwit-oss/tantivy/pull/2477)(@PSeitz)

- add format backwards-compatibility tests [#2485](https://github.com/quickwit-oss/tantivy/pull/2485)(@PSeitz)
- add columnar format compatibility tests [#2433](https://github.com/quickwit-oss/tantivy/pull/2433)(@PSeitz)
- Improved snippet ranges algorithm [#2474](https://github.com/quickwit-oss/tantivy/pull/2474)(@gezihuzi)
- make find_field_with_default return json fields without path [#2476](https://github.com/quickwit-oss/tantivy/pull/2476)(@trinity-1686a)
- Make `BooleanQuery` support `minimum_number_should_match` [#2405](https://github.com/quickwit-oss/tantivy/pull/2405)(@LebranceBW)
- Make `NUM_MERGE_THREADS` configurable [#2535](https://github.com/quickwit-oss/tantivy/pull/2535)(@Barre)

- **RegexPhraseQuery** 
`RegexPhraseQuery` supports phrase queries with regex. E.g. query "b.* b.* wolf" matches "big bad wolf". Slop is supported as well: "b.* wolf"~2 matches "big bad wolf" [#2516](https://github.com/quickwit-oss/tantivy/pull/2516)(@PSeitz)

- **Optional Index in Multivalue Columnar Index** 
For mostly empty multivalued indices there was a large overhead during creation when iterating all docids (merge case). 
This is alleviated by placing an optional index in the multivalued index to mark documents that have values. 
This will slightly increase space and access time. [#2439](https://github.com/quickwit-oss/tantivy/pull/2439)(@PSeitz)

- **Store DateTime as nanoseconds in doc store** DateTime in the doc store was truncated to microseconds previously. This removes this truncation, while still keeping backwards compatibility. [#2486](https://github.com/quickwit-oss/tantivy/pull/2486)(@PSeitz)

- **Performance/Memory**
    - lift clauses in LogicalAst for optimized ast during execution [#2449](https://github.com/quickwit-oss/tantivy/pull/2449)(@PSeitz)
    - Use Vec instead of BTreeMap to back OwnedValue object [#2364](https://github.com/quickwit-oss/tantivy/pull/2364)(@fulmicoton)
    - Replace TantivyDocument with CompactDoc. CompactDoc is much smaller and provides similar performance. [#2402](https://github.com/quickwit-oss/tantivy/pull/2402)(@PSeitz)
    - Recycling buffer in PrefixPhraseScorer [#2443](https://github.com/quickwit-oss/tantivy/pull/2443)(@fulmicoton)

- **Json Type**
    - JSON supports now all values on the root level. Previously an object was required. This enables support for flat mixed types. allow more JSON values, fix i64 special case [#2383](https://github.com/quickwit-oss/tantivy/pull/2383)(@PSeitz)
    - add json path constructor to term [#2367](https://github.com/quickwit-oss/tantivy/pull/2367)(@PSeitz)

- **QueryParser**
    - fix de-escaping too much in query parser [#2427](https://github.com/quickwit-oss/tantivy/pull/2427)(@trinity-1686a)
    - improve query parser [#2416](https://github.com/quickwit-oss/tantivy/pull/2416)(@trinity-1686a)
    - Support field grouping `title:(return AND "pink panther")` [#2333](https://github.com/quickwit-oss/tantivy/pull/2333)(@trinity-1686a)
    - allow term starting with wildcard [#2568](https://github.com/quickwit-oss/tantivy/pull/2568)(@trinity-1686a)

- Exist queries match subpath fields [#2558](https://github.com/quickwit-oss/tantivy/pull/2558)(@rdettai)
- add access benchmark for columnar [#2432](https://github.com/quickwit-oss/tantivy/pull/2432)(@PSeitz)
- extend indexwriter proptests [#2342](https://github.com/quickwit-oss/tantivy/pull/2342)(@PSeitz)
- add bench & test for columnar merging [#2428](https://github.com/quickwit-oss/tantivy/pull/2428)(@PSeitz)
- Change in Executor API [#2391](https://github.com/quickwit-oss/tantivy/pull/2391)(@fulmicoton)
- Removed usage of num_cpus [#2387](https://github.com/quickwit-oss/tantivy/pull/2387)(@fulmicoton)
- use bingang for agg and stacker benchmark [#2378](https://github.com/quickwit-oss/tantivy/pull/2378)[#2492](https://github.com/quickwit-oss/tantivy/pull/2492)(@PSeitz) 
- cleanup top level exports [#2382](https://github.com/quickwit-oss/tantivy/pull/2382)(@PSeitz)
- make convert_to_fast_value_and_append_to_json_term pub [#2370](https://github.com/quickwit-oss/tantivy/pull/2370)(@PSeitz)
- remove JsonTermWriter [#2238](https://github.com/quickwit-oss/tantivy/pull/2238)(@PSeitz)
- validate sort by field type [#2336](https://github.com/quickwit-oss/tantivy/pull/2336)(@PSeitz)
- Fix trait bound of StoreReader::iter [#2360](https://github.com/quickwit-oss/tantivy/pull/2360)(@adamreichold)
- remove read_postings_no_deletes [#2526](https://github.com/quickwit-oss/tantivy/pull/2526)(@PSeitz)

Tantivy 0.22.1
================================
- Fix TopNComputer for reverse order. [#2672](https://github.com/quickwit-oss/tantivy/pull/2672)(@stuhood @PSeitz) 

Affected queries are [order_by_fast_field](https://docs.rs/tantivy/latest/tantivy/collector/struct.TopDocs.html#method.order_by_fast_field) and
[order_by_u64_field](https://docs.rs/tantivy/latest/tantivy/collector/struct.TopDocs.html#method.order_by_u64_field)
for `Order::Asc`

Tantivy 0.22
================================

Tantivy 0.22 will be able to read indices created with Tantivy 0.21.

#### Bugfixes
- Fix null byte handling in JSON paths (null bytes in json keys caused panic during indexing) [#2345](https://github.com/quickwit-oss/tantivy/pull/2345)(@PSeitz)
- Fix bug that can cause `get_docids_for_value_range` to panic. [#2295](https://github.com/quickwit-oss/tantivy/pull/2295)(@fulmicoton)
- Avoid 1 document indices by increase min memory to 15MB for indexing [#2176](https://github.com/quickwit-oss/tantivy/pull/2176)(@PSeitz)
- Fix merge panic for JSON fields [#2284](https://github.com/quickwit-oss/tantivy/pull/2284)(@PSeitz)
- Fix bug occurring when merging JSON object indexed with positions. [#2253](https://github.com/quickwit-oss/tantivy/pull/2253)(@fulmicoton)
- Fix empty DateHistogram gap bug [#2183](https://github.com/quickwit-oss/tantivy/pull/2183)(@PSeitz)
- Fix range query end check (fields with less than 1 value per doc are affected) [#2226](https://github.com/quickwit-oss/tantivy/pull/2226)(@PSeitz)
- Handle exclusive out of bounds ranges on fastfield range queries [#2174](https://github.com/quickwit-oss/tantivy/pull/2174)(@PSeitz)

#### Breaking API Changes
- rename ReloadPolicy onCommit to onCommitWithDelay [#2235](https://github.com/quickwit-oss/tantivy/pull/2235)(@giovannicuccu)
- Move exports from the root into modules [#2220](https://github.com/quickwit-oss/tantivy/pull/2220)(@PSeitz)
- Accept field name instead of `Field` in FilterCollector [#2196](https://github.com/quickwit-oss/tantivy/pull/2196)(@PSeitz)
- remove deprecated IntOptions and DateTime [#2353](https://github.com/quickwit-oss/tantivy/pull/2353)(@PSeitz)

#### Features/Improvements
- Tantivy documents as a trait: Index data directly without converting to tantivy types first [#2071](https://github.com/quickwit-oss/tantivy/pull/2071)(@ChillFish8)
- encode some part of posting list as -1 instead of direct values (smaller inverted indices) [#2185](https://github.com/quickwit-oss/tantivy/pull/2185)(@trinity-1686a)
- **Aggregation**
  - Support to deserialize f64 from string [#2311](https://github.com/quickwit-oss/tantivy/pull/2311)(@PSeitz)
  - Add a top_hits aggregator [#2198](https://github.com/quickwit-oss/tantivy/pull/2198)(@ditsuke)
  - Support bool type in term aggregation [#2318](https://github.com/quickwit-oss/tantivy/pull/2318)(@PSeitz)
  - Support ip addresses in term aggregation [#2319](https://github.com/quickwit-oss/tantivy/pull/2319)(@PSeitz)
  - Support date type in term aggregation [#2172](https://github.com/quickwit-oss/tantivy/pull/2172)(@PSeitz)
  - Support escaped dot when addressing field [#2250](https://github.com/quickwit-oss/tantivy/pull/2250)(@PSeitz)

- Add ExistsQuery to check documents that have a value [#2160](https://github.com/quickwit-oss/tantivy/pull/2160)(@imotov)
- Expose TopDocs::order_by_u64_field again [#2282](https://github.com/quickwit-oss/tantivy/pull/2282)(@ditsuke)

- **Memory/Performance**
  - Faster TopN: replace BinaryHeap with TopNComputer [#2186](https://github.com/quickwit-oss/tantivy/pull/2186)(@PSeitz)
  - reduce number of allocations during indexing [#2257](https://github.com/quickwit-oss/tantivy/pull/2257)(@PSeitz)
  - Less Memory while indexing: docid deltas while indexing [#2249](https://github.com/quickwit-oss/tantivy/pull/2249)(@PSeitz)
  - Faster indexing: use term hashmap in fastfield [#2243](https://github.com/quickwit-oss/tantivy/pull/2243)(@PSeitz)
  - term hashmap remove copy in is_empty, unused unordered_id [#2229](https://github.com/quickwit-oss/tantivy/pull/2229)(@PSeitz)
  - add method to fetch block of first values in columnar [#2330](https://github.com/quickwit-oss/tantivy/pull/2330)(@PSeitz)
  - Faster aggregations: add fast path for full columns in fetch_block [#2328](https://github.com/quickwit-oss/tantivy/pull/2328)(@PSeitz)
  - Faster sstable loading: use fst for sstable index [#2268](https://github.com/quickwit-oss/tantivy/pull/2268)(@trinity-1686a)

- **QueryParser**
  - allow newline where we allow space in query parser [#2302](https://github.com/quickwit-oss/tantivy/pull/2302)(@trinity-1686a)
  - allow some mixing of occur and bool in strict query parser [#2323](https://github.com/quickwit-oss/tantivy/pull/2323)(@trinity-1686a)
  - handle * inside term in lenient query parser [#2228](https://github.com/quickwit-oss/tantivy/pull/2228)(@trinity-1686a)
  - add support for exists query syntax in query parser [#2170](https://github.com/quickwit-oss/tantivy/pull/2170)(@trinity-1686a)
- Add shared search executor [#2312](https://github.com/quickwit-oss/tantivy/pull/2312)(@MochiXu)
- Truncate keys to u16::MAX in term hashmap [#2299](https://github.com/quickwit-oss/tantivy/pull/2299)(@PSeitz)
- report if a term matched when warming up posting list [#2309](https://github.com/quickwit-oss/tantivy/pull/2309)(@trinity-1686a)
- Support json fields in FuzzyTermQuery [#2173](https://github.com/quickwit-oss/tantivy/pull/2173)(@PingXia-at)
- Read list of fields encoded in term dictionary for JSON fields [#2184](https://github.com/quickwit-oss/tantivy/pull/2184)(@PSeitz)
- add collect_block to BoxableSegmentCollector [#2331](https://github.com/quickwit-oss/tantivy/pull/2331)(@PSeitz)
- expose collect_block buffer size [#2326](https://github.com/quickwit-oss/tantivy/pull/2326)(@PSeitz)
- Forward regex parser errors [#2288](https://github.com/quickwit-oss/tantivy/pull/2288)(@adamreichold)
- Make FacetCounts defaultable and cloneable. [#2322](https://github.com/quickwit-oss/tantivy/pull/2322)(@adamreichold)
- Derive Debug for SchemaBuilder [#2254](https://github.com/quickwit-oss/tantivy/pull/2254)(@GodTamIt)
- add missing inlines to tantivy options [#2245](https://github.com/quickwit-oss/tantivy/pull/2245)(@PSeitz)

Tantivy 0.21.1
================================
#### Bugfixes
- Range queries on fast fields with less values on that field than documents had an invalid end condition, leading to missing results. [#2226](https://github.com/quickwit-oss/tantivy/issues/2226)(@appaquet @PSeitz)
- Increase the minimum memory budget from 3MB to 15MB to avoid single doc segments (API fix). [#2176](https://github.com/quickwit-oss/tantivy/issues/2176)(@PSeitz)

Tantivy 0.21
================================
#### Bugfixes
- Fix track fast field memory consumption, which led to higher memory consumption than the budget allowed during indexing [#2148](https://github.com/quickwit-oss/tantivy/issues/2148)[#2147](https://github.com/quickwit-oss/tantivy/issues/2147)(@PSeitz)
- Fix a regression from 0.20 where sort index by date wasn't working anymore [#2124](https://github.com/quickwit-oss/tantivy/issues/2124)(@PSeitz)
- Fix getting the root facet on the `FacetCollector`. [#2086](https://github.com/quickwit-oss/tantivy/issues/2086)(@adamreichold)
- Align numerical type priority order of columnar and query. [#2088](https://github.com/quickwit-oss/tantivy/issues/2088)(@fmassot)
#### Breaking Changes
- Remove support for Brotli and Snappy compression [#2123](https://github.com/quickwit-oss/tantivy/issues/2123)(@adamreichold)
#### Features/Improvements
- Implement lenient query parser [#2129](https://github.com/quickwit-oss/tantivy/pull/2129)(@trinity-1686a)
- order_by_u64_field and order_by_fast_field allow sorting in ascending and descending order [#2111](https://github.com/quickwit-oss/tantivy/issues/2111)(@naveenann)
- Allow dynamic filters in text analyzer builder [#2110](https://github.com/quickwit-oss/tantivy/issues/2110)(@fulmicoton @fmassot)
- **Aggregation**
  - Add missing parameter for term aggregation [#2149](https://github.com/quickwit-oss/tantivy/issues/2149)[#2103](https://github.com/quickwit-oss/tantivy/issues/2103)(@PSeitz)
  - Add missing parameter for percentiles [#2157](https://github.com/quickwit-oss/tantivy/issues/2157)(@PSeitz)
  - Add missing parameter for stats,min,max,count,sum,avg [#2151](https://github.com/quickwit-oss/tantivy/issues/2151)(@PSeitz)
  - Improve aggregation deserialization error message [#2150](https://github.com/quickwit-oss/tantivy/issues/2150)(@PSeitz)
  - Add validation for type Bytes to term_agg [#2077](https://github.com/quickwit-oss/tantivy/issues/2077)(@PSeitz)
  - Alternative mixed field collection [#2135](https://github.com/quickwit-oss/tantivy/issues/2135)(@PSeitz)
- Add missing query_terms impl for TermSetQuery. [#2120](https://github.com/quickwit-oss/tantivy/issues/2120)(@adamreichold)
- Minor improvements to OwnedBytes [#2134](https://github.com/quickwit-oss/tantivy/issues/2134)(@adamreichold)
- Remove allocations in split compound words [#2080](https://github.com/quickwit-oss/tantivy/issues/2080)(@PSeitz)
- Ngram tokenizer now returns an error with invalid arguments [#2102](https://github.com/quickwit-oss/tantivy/issues/2102)(@fmassot)
- Make TextAnalyzerBuilder public [#2097](https://github.com/quickwit-oss/tantivy/issues/2097)(@adamreichold)
- Return an error when tokenizer is not found while indexing [#2093](https://github.com/quickwit-oss/tantivy/issues/2093)(@naveenann)
- Delayed column opening during merge [#2132](https://github.com/quickwit-oss/tantivy/issues/2132)(@PSeitz)

Tantivy 0.20.2
================================
- Align numerical type priority order on the search side.  [#2088](https://github.com/quickwit-oss/tantivy/issues/2088) (@fmassot)
- Fix is_child_of function not considering the root facet. [#2086](https://github.com/quickwit-oss/tantivy/issues/2086) (@adamreichhold)

Tantivy 0.20.1
================================
- Fix building on windows with mmap [#2070](https://github.com/quickwit-oss/tantivy/issues/2070) (@ChillFish8)

Tantivy 0.20
================================
#### Bugfixes
- Fix phrase queries with slop (slop supports now transpositions, algorithm that carries slop so far for num terms > 2) [#2031](https://github.com/quickwit-oss/tantivy/issues/2031)[#2020](https://github.com/quickwit-oss/tantivy/issues/2020)(@PSeitz)
- Handle error for exists on MMapDirectory [#1988](https://github.com/quickwit-oss/tantivy/issues/1988) (@PSeitz)
- Aggregation
  - Fix min doc_count empty merge bug [#2057](https://github.com/quickwit-oss/tantivy/issues/2057) (@PSeitz)
  - Fix: Sort order for term aggregations (sort order on key was inverted) [#1858](https://github.com/quickwit-oss/tantivy/issues/1858) (@PSeitz)

#### Features/Improvements
- Add PhrasePrefixQuery [#1842](https://github.com/quickwit-oss/tantivy/issues/1842) (@trinity-1686a)
- Add `coerce` option for text and numbers types (convert the value instead of returning an error during indexing) [#1904](https://github.com/quickwit-oss/tantivy/issues/1904) (@PSeitz)
- Add regex tokenizer [#1759](https://github.com/quickwit-oss/tantivy/issues/1759)(@mkleen)
- Move tokenizer API to separate crate. Having a separate crate with a stable API will allow us to use tokenizers with different tantivy versions. [#1767](https://github.com/quickwit-oss/tantivy/issues/1767) (@PSeitz)
- **Columnar crate**: New fast field handling (@fulmicoton @PSeitz) [#1806](https://github.com/quickwit-oss/tantivy/issues/1806)[#1809](https://github.com/quickwit-oss/tantivy/issues/1809)
  - Support for fast fields with optional values. Previously tantivy supported only single-valued and multi-value fast fields. The encoding of optional fast fields is now very compact.
  - Fast field Support for JSON (schemaless fast fields). Support multiple types on the same column. [#1876](https://github.com/quickwit-oss/tantivy/issues/1876) (@fulmicoton)
  - Unified access for fast fields over different cardinalities.
  - Unified storage for typed and untyped fields.
  - Move fastfield codecs into columnar. [#1782](https://github.com/quickwit-oss/tantivy/issues/1782) (@fulmicoton)
  - Sparse dense index for optional values [#1716](https://github.com/quickwit-oss/tantivy/issues/1716) (@PSeitz)
  - Switch to nanosecond precision in DateTime fastfield [#2016](https://github.com/quickwit-oss/tantivy/issues/2016) (@PSeitz)
- **Aggregation**
  - Add `date_histogram` aggregation (only `fixed_interval` for now) [#1900](https://github.com/quickwit-oss/tantivy/issues/1900) (@PSeitz)
  - Add `percentiles` aggregations [#1984](https://github.com/quickwit-oss/tantivy/issues/1984) (@PSeitz)
  - [**breaking**] Drop JSON support on intermediate agg result (we use postcard as format in `quickwit` to send intermediate results) [#1992](https://github.com/quickwit-oss/tantivy/issues/1992) (@PSeitz)
  - Set memory limit in bytes for aggregations after which they abort (Previously there was only the bucket limit) [#1942](https://github.com/quickwit-oss/tantivy/issues/1942)[#1957](https://github.com/quickwit-oss/tantivy/issues/1957)(@PSeitz)
  - Add support for u64,i64,f64 fields in term aggregation [#1883](https://github.com/quickwit-oss/tantivy/issues/1883) (@PSeitz)
  - Allow histogram bounds to be passed as Rfc3339 [#2076](https://github.com/quickwit-oss/tantivy/issues/2076) (@PSeitz)
  - Add count, min, max, and sum aggregations [#1794](https://github.com/quickwit-oss/tantivy/issues/1794) (@guilload)
  - Switch to Aggregation without serde_untagged => better deserialization errors. [#2003](https://github.com/quickwit-oss/tantivy/issues/2003) (@PSeitz)
  - Switch to ms in histogram for date type (ES compatibility) [#2045](https://github.com/quickwit-oss/tantivy/issues/2045) (@PSeitz)
  - Reduce term aggregation memory consumption [#2013](https://github.com/quickwit-oss/tantivy/issues/2013) (@PSeitz)
  - Reduce agg memory consumption: Replace generic aggregation collector (which has a high memory requirement per instance) in aggregation tree with optimized versions behind a trait.
  - Split term collection count and sub_agg (Faster term agg with less memory consumption for cases without sub-aggs) [#1921](https://github.com/quickwit-oss/tantivy/issues/1921) (@PSeitz)
  - Schemaless aggregations: In combination with stacker tantivy supports now schemaless aggregations via the JSON type.
    - Add aggregation support for JSON type [#1888](https://github.com/quickwit-oss/tantivy/issues/1888) (@PSeitz)
    - Mixed types support on JSON fields in aggs [#1971](https://github.com/quickwit-oss/tantivy/issues/1971) (@PSeitz)
  - Perf: Fetch blocks of vals in aggregation for all cardinality [#1950](https://github.com/quickwit-oss/tantivy/issues/1950) (@PSeitz)
  - Allow histogram bounds to be passed as Rfc3339 [#2076](https://github.com/quickwit-oss/tantivy/issues/2076) (@PSeitz)
- `Searcher` with disabled scoring via `EnableScoring::Disabled` [#1780](https://github.com/quickwit-oss/tantivy/issues/1780) (@shikhar)
- Enable tokenizer on json fields [#2053](https://github.com/quickwit-oss/tantivy/issues/2053) (@PSeitz)
- Enforcing "NOT" and "-" queries consistency in UserInputAst [#1609](https://github.com/quickwit-oss/tantivy/issues/1609) (@bazhenov)
- Faster indexing
  - Refactor tokenization pipeline to use GATs [#1924](https://github.com/quickwit-oss/tantivy/issues/1924) (@trinity-1686a)
  - Faster term hash map [#2058](https://github.com/quickwit-oss/tantivy/issues/2058)[#1940](https://github.com/quickwit-oss/tantivy/issues/1940) (@PSeitz)
  - tokenizer-api: reduce Tokenizer allocation overhead [#2062](https://github.com/quickwit-oss/tantivy/issues/2062) (@PSeitz)
  - Refactor vint [#2010](https://github.com/quickwit-oss/tantivy/issues/2010) (@PSeitz)
- Faster search
  - Work in batches of docs on the SegmentCollector (Only for cases without score for now) [#1937](https://github.com/quickwit-oss/tantivy/issues/1937) (@PSeitz)
  - Faster fast field range queries using SIMD [#1954](https://github.com/quickwit-oss/tantivy/issues/1954) (@fulmicoton)
  - Improve fast field range query performance [#1864](https://github.com/quickwit-oss/tantivy/issues/1864) (@PSeitz)
- Make BM25 scoring more flexible [#1855](https://github.com/quickwit-oss/tantivy/issues/1855) (@alexcole)
- Switch fs2 to fs4 as it is now unmaintained and does not support illumos [#1944](https://github.com/quickwit-oss/tantivy/issues/1944) (@Toasterson)
- Made BooleanWeight and BoostWeight public [#1991](https://github.com/quickwit-oss/tantivy/issues/1991) (@fulmicoton)
- Make index compatible with virtual drives on Windows [#1843](https://github.com/quickwit-oss/tantivy/issues/1843) (@gyk)
- Add stop words for Hungarian language [#2069](https://github.com/quickwit-oss/tantivy/issues/2069) (@tnxbutno)
- Auto downgrade index record option, instead of vint error [#1857](https://github.com/quickwit-oss/tantivy/issues/1857) (@PSeitz)
- Enable range query on fast field for u64 compatible types [#1762](https://github.com/quickwit-oss/tantivy/issues/1762) (@PSeitz) [#1876]
- sstable
  - Isolating sstable and stacker in independent crates. [#1718](https://github.com/quickwit-oss/tantivy/issues/1718) (@fulmicoton)
  - New sstable format [#1943](https://github.com/quickwit-oss/tantivy/issues/1943)[#1953](https://github.com/quickwit-oss/tantivy/issues/1953) (@trinity-1686a)
  - Use DeltaReader directly to implement Dictionary::ord_to_term [#1928](https://github.com/quickwit-oss/tantivy/issues/1928) (@trinity-1686a)
  - Use DeltaReader directly to implement Dictionary::term_ord [#1925](https://github.com/quickwit-oss/tantivy/issues/1925) (@trinity-1686a)
- Add separate tokenizer manager for fast fields [#2019](https://github.com/quickwit-oss/tantivy/issues/2019) (@PSeitz)
- Make construction of LevenshteinAutomatonBuilder for FuzzyTermQuery instances lazy. [#1756](https://github.com/quickwit-oss/tantivy/issues/1756) (@adamreichold)
- Added support for madvise when opening an mmapped Index [#2036](https://github.com/quickwit-oss/tantivy/issues/2036) (@fulmicoton)
- Rename `DatePrecision` to `DateTimePrecision` [#2051](https://github.com/quickwit-oss/tantivy/issues/2051) (@guilload)
- Query Parser
  - Quotation mark can now be used for phrase queries. [#2050](https://github.com/quickwit-oss/tantivy/issues/2050) (@fulmicoton)
  - PhrasePrefixQuery is supported in the query parser via: `field:"phrase ter"*` [#2044](https://github.com/quickwit-oss/tantivy/issues/2044) (@adamreichold)
- Docs
  - Update examples for literate docs [#1880](https://github.com/quickwit-oss/tantivy/issues/1880) (@PSeitz)
  - Add ip field example [#1775](https://github.com/quickwit-oss/tantivy/issues/1775) (@PSeitz)
  - Fix doc store cache documentation [#1821](https://github.com/quickwit-oss/tantivy/issues/1821) (@PSeitz)
  - Fix BooleanQuery document [#1999](https://github.com/quickwit-oss/tantivy/issues/1999) (@RT_Enzyme)
  - Update comments in the faceted search example [#1737](https://github.com/quickwit-oss/tantivy/issues/1737) (@DawChihLiou)


Tantivy 0.19
================================
#### Bugfixes
- Fix missing fieldnorms for u64, i64, f64, bool, bytes and date [#1620](https://github.com/quickwit-oss/tantivy/pull/1620) (@PSeitz)
- Fix interpolation overflow in linear interpolation fastfield codec [#1480](https://github.com/quickwit-oss/tantivy/pull/1480) (@PSeitz @fulmicoton)

#### Features/Improvements
- Add support for `IN` in queryparser , e.g. `field: IN [val1 val2 val3]` [#1683](https://github.com/quickwit-oss/tantivy/pull/1683) (@trinity-1686a)
- Skip score calculation, when no scoring is required [#1646](https://github.com/quickwit-oss/tantivy/pull/1646) (@PSeitz)
- Limit fast fields to u32 (`get_val(u32)`) [#1644](https://github.com/quickwit-oss/tantivy/pull/1644) (@PSeitz)
- The `DateTime` type has been updated to hold timestamps with microseconds precision.
  `DateOptions` and `DatePrecision` have been added to configure Date fields. The precision is used to hint on fast values compression. Otherwise, seconds precision is used everywhere else (i.e terms, indexing) [#1396](https://github.com/quickwit-oss/tantivy/pull/1396) (@evanxg852000)
- Add IP address field type [#1553](https://github.com/quickwit-oss/tantivy/pull/1553) (@PSeitz)
- Add boolean field type [#1382](https://github.com/quickwit-oss/tantivy/pull/1382) (@boraarslan)
- Remove Searcher pool and make `Searcher` cloneable. (@PSeitz)
- Validate settings on create [#1570](https://github.com/quickwit-oss/tantivy/pull/1570) (@PSeitz)
- Detect and apply gcd on fastfield codecs [#1418](https://github.com/quickwit-oss/tantivy/pull/1418) (@PSeitz)
- Doc store
  - use separate thread to compress block store [#1389](https://github.com/quickwit-oss/tantivy/pull/1389) [#1510](https://github.com/quickwit-oss/tantivy/pull/1510) (@PSeitz @fulmicoton)
  - Expose doc store cache size [#1403](https://github.com/quickwit-oss/tantivy/pull/1403) (@PSeitz)
  - Enable compression levels for doc store [#1378](https://github.com/quickwit-oss/tantivy/pull/1378) (@PSeitz)
  - Make block size configurable [#1374](https://github.com/quickwit-oss/tantivy/pull/1374) (@kryesh)
- Make `tantivy::TantivyError` cloneable [#1402](https://github.com/quickwit-oss/tantivy/pull/1402) (@PSeitz)
- Add support for phrase slop in query language [#1393](https://github.com/quickwit-oss/tantivy/pull/1393) (@saroh)
- Aggregation
  - Add aggregation support for date type [#1693](https://github.com/quickwit-oss/tantivy/pull/1693)(@PSeitz)
  - Add support for keyed parameter in range and histogram aggregations [#1424](https://github.com/quickwit-oss/tantivy/pull/1424) (@k-yomo)
  - Add aggregation bucket limit [#1363](https://github.com/quickwit-oss/tantivy/pull/1363) (@PSeitz)
- Faster indexing
  - [#1610](https://github.com/quickwit-oss/tantivy/pull/1610) (@PSeitz)
  - [#1594](https://github.com/quickwit-oss/tantivy/pull/1594) (@PSeitz)
  - [#1582](https://github.com/quickwit-oss/tantivy/pull/1582) (@PSeitz)
  - [#1611](https://github.com/quickwit-oss/tantivy/pull/1611) (@PSeitz)
  - Added a pre-configured stop word filter for various language [#1666](https://github.com/quickwit-oss/tantivy/pull/1666) (@adamreichold)

Tantivy 0.18
================================

- For date values `chrono` has been replaced with `time` (@uklotzde) #1304 :
  - The `time` crate is re-exported as `tantivy::time` instead of `tantivy::chrono`.
  - The type alias `tantivy::DateTime` has been removed.
  - `Value::Date` wraps `time::PrimitiveDateTime` without time zone information.
  - Internally date/time values are stored as seconds since UNIX epoch in UTC.
  - Converting a `time::OffsetDateTime` to `Value::Date` implicitly converts the value into UTC.
    If this is not desired do the time zone conversion yourself and use `time::PrimitiveDateTime`
    directly instead.
- Add [histogram](https://github.com/quickwit-oss/tantivy/pull/1306) aggregation (@PSeitz)
- Add support for fastfield on text fields (@PSeitz)
- Add terms aggregation (@PSeitz)
- Add support for zstd compression (@kryesh)

Tantivy 0.18.1
================================
- Hotfix: positions computation.  #1629 (@fmassot, @fulmicoton, @PSeitz)

Tantivy 0.17
================================

- LogMergePolicy now triggers merges if the ratio of deleted documents reaches a threshold (@shikhar @fulmicoton) [#115](https://github.com/quickwit-oss/tantivy/issues/115)
- Adds a searcher Warmer API (@shikhar @fulmicoton)
- Change to non-strict schema. Ignore fields in data which are not defined in schema. Previously this returned an error. #1211
- Facets are necessarily indexed. Existing index with indexed facets should work out of the box. Index without facets that are marked with index: false should be broken (but they were already broken in a sense). (@fulmicoton) #1195 .
- Bugfix that could in theory impact durability in theory on some filesystems [#1224](https://github.com/quickwit-oss/tantivy/issues/1224)
- Schema now offers not indexing fieldnorms (@lpouget) [#922](https://github.com/quickwit-oss/tantivy/issues/922)
- Reduce the number of fsync calls [#1225](https://github.com/quickwit-oss/tantivy/issues/1225)
- Fix opening bytes index with dynamic codec (@PSeitz) [#1278](https://github.com/quickwit-oss/tantivy/issues/1278)
- Added an aggregation collector for range, average and stats compatible with Elasticsearch. (@PSeitz)
- Added a JSON schema type @fulmicoton [#1251](https://github.com/quickwit-oss/tantivy/issues/1251)
- Added support for slop in phrase queries @halvorboe [#1068](https://github.com/quickwit-oss/tantivy/issues/1068)

Tantivy 0.16.2
================================

- Bugfix in FuzzyTermQuery. (transposition_cost_one was not doing anything)

Tantivy 0.16.1
========================

- Major Bugfix on multivalued fastfield.  #1151
- Demux operation (@PSeitz)

Tantivy 0.16.0
=========================

- Bugfix in the filesum check. (@evanxg852000) #1127
- Bugfix in positions when the index is sorted by a field. (@appaquet) #1125

Tantivy 0.15.3
=========================

- Major bugfix. Deleting documents was broken when the index was sorted by a field. (@appaquet, @fulmicoton) #1101

Tantivy 0.15.2
========================

- Major bugfix. DocStore still panics when a deleted doc is at the beginning of a block. (@appaquet) #1088

Tantivy 0.15.1
=========================

- Major bugfix. DocStore panics when first block is deleted. (@appaquet) #1077

Tantivy 0.15.0
=========================

- API Changes. Using Range instead of (start, end) in the API and internals (`FileSlice`, `OwnedBytes`, `Snippets`, ...)
  This change is breaking but migration is trivial.
- Added an Histogram collector. (@fulmicoton) #994
- Added support for Option<TCollector>.  (@fulmicoton)
- DocAddress is now a struct (@scampi) #987
- Bugfix consistent tie break handling in facet's topk (@hardikpnsp) #357
- Date field support for range queries (@rihardsk) #516
- Added lz4-flex as the default compression scheme in tantivy (@PSeitz) #1009
- Renamed a lot of symbols to avoid all uppercasing on acronyms, as per new clippy recommendation. For instance, RAMDirectory -> RamDirectory. (@fulmicoton)
- Simplified positions index format (@fulmicoton) #1022
- Moved bitpacking to bitpacker subcrate and add BlockedBitpacker, which bitpacks blocks of 128 elements (@PSeitz) #1030
- Added support for more-like-this query in tantivy (@evanxg852000) #1011
- Added support for sorting an index, e.g presorting documents in an index by a timestamp field. This can heavily improve performance for certain scenarios, by utilizing the sorted data (Top-n optimizations)(@PSeitz). #1026
- Add iterator over documents in doc store (@PSeitz). #1044
- Fix log merge policy (@PSeitz). #1043
- Add detection to avoid small doc store blocks on merge (@PSeitz). #1054
- Make doc store compression dynamic (@PSeitz). #1060
- Switch to json for footer version handling (@PSeitz). #1060
- Updated TermMerger implementation to rely on the union feature of the FST (@scampi) #469
- Add boolean marking whether position is required in the query_terms API call (@fulmicoton). #1070

Tantivy 0.14.0
=========================

- Remove dependency to atomicwrites #833 .Implemented by @fulmicoton upon suggestion and research from @asafigan).
- Migrated tantivy error from the now deprecated `failure` crate to `thiserror` #760. (@hirevo)
- API Change. Accessing the typed value off a `Schema::Value` now returns an Option instead of panicking if the type does not match.
- Large API Change in the Directory API. Tantivy used to assume that all files could be somehow memory mapped. After this change, Directory return a `FileSlice` that can be reduced and eventually read into an `OwnedBytes` object. Long and blocking io operation are still required by they do not span over the entire file.
- Added support for Brotli compression in the DocStore. (@ppodolsky)
- Added helper for building intersections and unions in BooleanQuery (@guilload)
- Bugfix in `Query::explain`
- Removed dependency on `notify` #924. Replaced with `FileWatcher` struct that polls meta file every 500ms in background thread. (@halvorboe @guilload)
- Added `FilterCollector`, which wraps another collector and filters docs using a predicate over a fast field (@barrotsteindev)
- Simplified the encoding of the skip reader struct. BlockWAND max tf is now encoded over a single byte. (@fulmicoton)
- `FilterCollector` now supports all Fast Field value types (@barrotsteindev)
- FastField are not all loaded when opening the segment reader. (@fulmicoton)
- Added an API to merge segments, see `tantivy::merge_segments` #1005. (@evanxg852000)

This version breaks compatibility and requires users to reindex everything.

Tantivy 0.13.2
===================

Bugfix. Acquiring a facet reader on a segment that does not contain any
doc with this facet returns `None`. (#896)

Tantivy 0.13.1
===================

Made `Query` and `Collector` `Send + Sync`.
Updated misc dependency versions.

Tantivy 0.13.0
======================

Tantivy 0.13 introduce a change in the index format that will require
you to reindex your index (BlockWAND information are added in the skiplist).
The index size increase is minor as this information is only added for
full blocks.
If you have a massive index for which reindexing is not an option, please contact me
so that we can discuss possible solutions.

- Bugfix in `FuzzyTermQuery` not matching terms by prefix when it should (@Peachball)
- Relaxed constraints on the custom/tweak score functions. At the segment level, they can be mut, and they are not required to be Sync + Send.
- `MMapDirectory::open` does not return a `Result` anymore.
- Change in the DocSet and Scorer API. (@fulmicoton).
A freshly created DocSet point directly to their first doc. A sentinel value called TERMINATED marks the end of a DocSet.
`.advance()` returns the new DocId. `Scorer::skip(target)` has been replaced by `Scorer::seek(target)` and returns the resulting DocId.
As a result, iterating through DocSet now looks as follows

```rust
let mut doc = docset.doc();
while doc != TERMINATED {
   // ...
   doc = docset.advance();
}
```

The change made it possible to greatly simplify a lot of the docset's code.

- Misc internal optimization and introduction of the `Scorer::for_each_pruning` function. (@fulmicoton)
- Added an offset option to the Top(.*)Collectors. (@robyoung)
- Added Block WAND. Performance on TOP-K on term-unions should be greatly increased. (@fulmicoton, and special thanks
to the PISA team for answering all my questions!)

Tantivy 0.12.0
======================

- Removing static dispatch in tokenizers for simplicity. (#762)
- Added backward iteration for `TermDictionary` stream. (@halvorboe)
- Fixed a performance issue when searching for the posting lists of a missing term (@audunhalland)
- Added a configurable maximum number of docs (10M by default) for a segment to be considered for merge (@hntd187, landed by @halvorboe #713)
- Important Bugfix #777, causing tantivy to retain memory mapping. (diagnosed by @poljar)
- Added support for field boosting. (#547, @fulmicoton)

## How to update?

Crates relying on custom tokenizer, or registering tokenizer in the manager will require some
minor changes. Check <https://github.com/quickwit-oss/tantivy/blob/main/examples/custom_tokenizer.rs>
to check for some code sample.

Tantivy 0.11.3
=======================

- Fixed DateTime as a fast field (#735)

Tantivy 0.11.2
=======================

- The future returned by `IndexWriter::merge` does not borrow `self` mutably anymore (#732)
- Exposing a constructor for `WatchHandle` (#731)

Tantivy 0.11.1
=====================

- Bug fix #729

Tantivy 0.11.0
=====================

- Added f64 field. Internally reuse u64 code the same way i64 does (@fdb-hiroshima)
- Various bugfixes in the query parser.
  - Better handling of hyphens in query parser. (#609)
  - Better handling of whitespaces.
- Closes #498 - add support for Elastic-style unbounded range queries for alphanumeric types eg. "title:>hello", "weight:>=70.5", "height:<200" (@petr-tik)
- API change around `Box<BoxableTokenizer>`. See detail in #629
- Avoid rebuilding Regex automaton whenever a regex query is reused. #639 (@brainlock)
- Add footer with some metadata to index files. #605 (@fdb-hiroshima)
- Add a method to check the compatibility of the footer in the index with the running version of tantivy (@petr-tik)
- TopDocs collector: ensure stable sorting on equal score. #671 (@brainlock)
- Added handling of pre-tokenized text fields (#642), which will enable users to
  load tokens created outside tantivy. See usage in examples/pre_tokenized_text. (@kkoziara)
- Fix crash when committing multiple times with deleted documents. #681 (@brainlock)

## How to update?

- The index format is changed. You are required to reindex your data to use tantivy 0.11.
- `Box<dyn BoxableTokenizer>` has been replaced by a `BoxedTokenizer` struct.
- Regex are now compiled when the `RegexQuery` instance is built. As a result, it can now return
an error and handling the `Result` is required.
- `tantivy::version()` now returns a `Version` object. This object implements `ToString()`

Tantivy 0.10.2
=====================

- Closes #656. Solving memory leak.

Tantivy 0.10.1
=====================

- Closes #544.  A few users experienced problems with the directory watching system.
Avoid watching the mmap directory until someone effectively creates a reader that uses
this functionality.

Tantivy 0.10.0
=====================

*Tantivy 0.10.0 index format is compatible with the index format in 0.9.0.*

- Added an API to easily tweak or entirely replace the
 default score. See `TopDocs::tweak_score`and `TopScore::custom_score` (@fulmicoton)
- Added an ASCII folding filter (@drusellers)
- Bugfix in `query.count` in presence of deletes (@fulmicoton)
- Added `.explain(...)` in `Query` and `Weight` to (@fulmicoton)
- Added an efficient way to `delete_all_documents` in `IndexWriter` (@petr-tik).
  All segments are simply removed.

Minor
---------

- Switched to Rust 2018 (@uvd)
- Small simplification of the code.
Calling .freq() or .doc() when .advance() has never been called
on segment postings should panic from now on.
- Tokens exceeding `u16::max_value() - 4` chars are discarded silently instead of panicking.
- Fast fields are now preloaded when the `SegmentReader` is created.
- `IndexMeta` is now public.  (@hntd187)
- `IndexWriter` `add_document`, `delete_term`. `IndexWriter` is `Sync`, making it possible to use it with a `Arc<RwLock<IndexWriter>>`. `add_document` and `delete_term` can
only require a read lock. (@fulmicoton)
- Introducing `Opstamp` as an expressive type alias for `u64`. (@petr-tik)
- Stamper now relies on `AtomicU64` on all platforms (@petr-tik)
- Bugfix - Files get deleted slightly earlier
- Compilation resources improved (@fdb-hiroshima)

## How to update?

Your program should be usable as is.

### Fast fields

Fast fields used to be accessed directly from the `SegmentReader`.
The API changed, you are now required to acquire your fast field reader via the
`segment_reader.fast_fields()`, and use one of the typed method:

- `.u64()`, `.i64()` if your field is single-valued ;
- `.u64s()`, `.i64s()` if your field is multi-valued ;
- `.bytes()` if your field is bytes fast field.

Tantivy 0.9.0
=====================

*0.9.0 index format is not compatible with the
previous index format.*

- MAJOR BUGFIX :
  Some `Mmap` objects were being leaked, and would never get released. (@fulmicoton)
- Removed most unsafe (@fulmicoton)
- Indexer memory footprint improved. (VInt comp, inlining the first block. (@fulmicoton)
- Stemming in other language possible (@pentlander)
- Segments with no docs are deleted earlier (@barrotsteindev)
- Added grouped add and delete operations.
  They are guaranteed to happen together (i.e. they cannot be split by a commit).
  In addition, adds are guaranteed to happen on the same segment. (@elbow-jason)
- Removed `INT_STORED` and `INT_INDEXED`. It is now possible to use `STORED` and `INDEXED`
  for int fields. (@fulmicoton)
- Added DateTime field (@barrotsteindev)
- Added IndexReader. By default, index is reloaded automatically upon new commits (@fulmicoton)
- SIMD linear search within blocks (@fulmicoton)

## How to update ?

tantivy 0.9 brought some API breaking change.
To update from tantivy 0.8, you will need to go through the following steps.

- `schema::INT_INDEXED` and `schema::INT_STORED`  should be replaced by `schema::INDEXED` and `schema::INT_STORED`.
- The index now does not hold the pool of searcher anymore. You are required to create an intermediary object called
`IndexReader` for this.

    ```rust
    // create the reader. You typically need to create 1 reader for the entire
    // lifetime of you program.
    let reader = index.reader()?;

    // Acquire a searcher (previously `index.searcher()`) is now written:
    let searcher = reader.searcher();

    // With the default setting of the reader, you are not required to
    // call `index.load_searchers()` anymore.
    //
    // The IndexReader will pick up that change automatically, regardless
    // of whether the update was done in a different process or not.
    // If this behavior is not wanted, you can create your reader with
    // the `ReloadPolicy::Manual`, and manually decide when to reload the index
    // by calling `reader.reload()?`.

    ```

Tantivy 0.8.2
=====================

Fixing build for x86_64 platforms. (#496)
No need to update from 0.8.1 if tantivy
is building on your platform.

Tantivy 0.8.1
=====================

Hotfix of #476.

Merge was reflecting deletes before commit was passed.
Thanks @barrotsteindev  for reporting the bug.

Tantivy 0.8.0
=====================

*No change in the index format*

- API Breaking change in the collector API. (@jwolfe, @fulmicoton)
- Multithreaded search (@jwolfe, @fulmicoton)

Tantivy 0.7.1
=====================

*No change in the index format*

- Bugfix: NGramTokenizer panics on non ascii chars
- Added a space usage API

Tantivy 0.7
=====================

- Skip data for doc ids and positions (@fulmicoton),
  greatly improving performance
- Tantivy error now rely on the failure crate (@drusellers)
- Added support for `AND`, `OR`, `NOT` syntax in addition to the `+`,`-` syntax
- Added a snippet generator with highlight (@vigneshsarma, @fulmicoton)
- Added a `TopFieldCollector` (@pentlander)

Tantivy 0.6.1
=========================

- Bugfix #324. GC removing was removing file that were still in useful
- Added support for parsing AllQuery and RangeQuery via QueryParser
  - AllQuery: `*`
  - RangeQuery:
    - Inclusive `field:[startIncl to endIncl]`
    - Exclusive `field:{startExcl to endExcl}`
    - Mixed `field:[startIncl to endExcl}` and vice versa
    - Unbounded `field:[start to *]`, `field:[* to end]`

Tantivy 0.6
==========================

Special thanks to @drusellers and @jason-wolfe for their contributions
to this release!

- Removed C code. Tantivy is now pure Rust. (@fulmicoton)
- BM25 (@fulmicoton)
- Approximate field norms encoded over 1 byte. (@fulmicoton)
- Compiles on stable rust (@fulmicoton)
- Add &[u8] fastfield for associating arbitrary bytes to each document (@jason-wolfe) (#270)
  - Completely uncompressed
  - Internally: One u64 fast field for indexes, one fast field for the bytes themselves.
- Add NGram token support (@drusellers)
- Add Stopword Filter support (@drusellers)
- Add a FuzzyTermQuery (@drusellers)
- Add a RegexQuery (@drusellers)
- Various performance improvements (@fulmicoton)_

Tantivy 0.5.2
===========================

- bugfix #274
- bugfix #280
- bugfix #289

Tantivy 0.5.1
==========================

- bugfix #254 : tantivy failed if no documents in a segment contained a specific field.

Tantivy 0.5
==========================

- Faceting
- RangeQuery
- Configurable tokenization pipeline
- Bugfix in PhraseQuery
- Various query optimisation
- Allowing very large indexes
  - 64 bits file address
  - Smarter encoding of the `TermInfo` objects

Tantivy 0.4.3
==========================

- Bugfix race condition when deleting files. (#198)

Tantivy 0.4.2
==========================

- Prevent usage of AVX2 instructions (#201)

Tantivy 0.4.1
==========================

- Bugfix for non-indexed fields. (#199)

Tantivy 0.4.0
==========================

- Raise the limit of number of fields (previously 256 fields) (@fulmicoton)
- Removed u32 fields. They are replaced by u64 and i64 fields (#65) (@fulmicoton)
- Optimized skip in SegmentPostings (#130) (@lnicola)
- Replacing rustc_serialize by serde. Kudos to  benchmark@KodrAus and @lnicola
- Using error-chain (@KodrAus)
- QueryParser: (@fulmicoton)
  - Explicit error returned when searched for a term that is not indexed
  - Searching for a int term via the query parser was broken `(age:1)`
  - Searching for a non-indexed field returns an explicit Error
  - Phrase query for non-tokenized field are not tokenized by the query parser.
- Faster/Better indexing (@fulmicoton)
  - using murmurhash2
  - faster merging
  - more memory efficient fast field writer (@lnicola )
  - better handling of collisions
  - lesser memory usage
- Added API, most notably to iterate over ranges of terms (@fulmicoton)
- Bugfix that was preventing to unmap segment files, on index drop (@fulmicoton)
- Made the doc! macro public (@fulmicoton)
- Added an alternative implementation of the streaming dictionary (@fulmicoton)

Tantivy 0.3.1
==========================

- Expose a method to trigger files garbage collection

Tantivy 0.3
==========================

Special thanks to @Kodraus @lnicola @Ameobea @manuel-woelker @celaus
for their contribution to this release.

Thanks also to everyone in tantivy gitter chat
for their advise and company :)

<https://gitter.im/tantivy-search/tantivy>

Warning:

Tantivy 0.3 is NOT backward compatible with tantivy 0.2
code and index format.
You should not expect backward compatibility before
tantivy 1.0.

New Features
------------

- Delete. You can now delete documents from an index.
- Support for windows (Thanks to @lnicola)

Various Bugfixes & small improvements
----------------------------------------

- Added CI for Windows (<https://ci.appveyor.com/project/fulmicoton/tantivy>)
Thanks to @KodrAus ! (#108)
- Various dependy version update (Thanks to @Ameobea) #76
- Fixed several race conditions in `Index.wait_merge_threads`
- Fixed #72. Mmap were never released.
- Fixed #80. Fast field used to take an amplitude of 32 bits after a merge. (Ouch!)
- Fixed #92. u32 are now encoded using big endian in the fst
  in order to make there enumeration consistent with
  the natural ordering.
- Building binary targets for tantivy-cli (Thanks to @KodrAus)
- Misc invisible bug fixes, and code cleanup.
- Use
