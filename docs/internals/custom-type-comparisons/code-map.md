# Custom-type handling before the redesign: code map

A survey of where custom-type values are read, written, compared and converted, taken at main 6a8ccc1d3c
(2026-11-03). Paths are relative to the repository root. Line numbers drift as the steps land; the code wins
over this map. "grep only" means the hit was seen but the surrounding code was not read.

## 0. Vocabulary used below

- Stored form: the value as it sits in the table B-tree = ENCODE(value) (then COERCE per chain type).
- Decoded form (user-facing): DECODE(stored).
- A column read in an expression emits `Insn::Column` and then DECODE right after it (translator.rs `translate_column_expr`, ~2451-2700). So registers in expressions normally hold decoded values.
- Raw stored reads: `emit_stored_column` (core/translate/expr/columns.rs:126) and `emit_table_column`. Used for DML, index keys, CDC, FK, hash-join payloads.
- `CollationSeq` = `Text(TextCollation)` or `Type(TypeOrdering)`. `TypeOrdering` = Numeric | Array | TextArray (core/type_ordering.rs:19-28).
- `DmlRowForm` = UserFacing | Stored | StoredExceptUpdated: says which form the registers of a row being written are in. `DmlColumnContext`, `SelfTableContext` (ForSelect / ForDML) are in core/vdbe/builder.rs.

Program flags (core/vdbe/builder.rs ~505-551):
- `suppress_custom_type_decode`: set only in order_by.rs:436/446 around ORDER BY sort-key translation. Honored in translator.rs:279 (`copy_cached_expr_reg`) and ~2683 (`translate_column_expr`).
- `keep_array_results_as_blobs`: set at core/translate/mod.rs:111. Skips the final `Insn::ArrayDecode` in `emit_array_decode_for_results`.
- `suppress_column_default`: columns.rs:151.
- `skip_expression_index_values`: expr/metadata.rs:309-320, translator.rs:326.

Resolver cache entry `CachedExprReg{expr, reg, needs_decode}`: `needs_decode = true` only for hash-join payload column registers (main_loop/hash.rs:861-869). Every other `cache_expr_reg` call passes false (window.rs:1245, 1263, 3111, 3131; emission.rs RETURNING; emitter/mod.rs CHECK; incremental/vdbe_maintenance binding.rs:117,151, join.rs:402,422). `translate_expr` checks the cache first (translator.rs:97); `copy_cached_expr_reg` (264) decodes when `needs_decode && !suppress_custom_type_decode`.

---

## 1. Where ENCODE / COERCE / DECODE bytecode is emitted

### 1.1 Core emit helpers (core/translate/expr/custom_types.rs)
- `emit_dml_expr_index_value` (59): expression-index value from a DML row image.
- `emit_user_facing_column_value` (93): decode a stored column register. Array columns return early with no decode.
- `emit_decode` (122).
- `emit_domain_cast_constraints` (150): domain CHECKs after a CAST.
- `emit_store_value` (239): ENCODE, then COERCE for each type in the chain.
- `emit_type_expr` (257) -> `emit_expr_with_bindings` (306): evaluates an ENCODE/DECODE/COERCE expression with `id_register_overrides["value"]` bound to the input register.
- `emit_coerce` (271).
- `emit_trigger_decode_registers` (352).
- `emit_constraint_values_decode` (388): decode stored values for error messages.
- core/translate/expr/arrays.rs: `emit_array_encode` (119), `emit_array_decode` (184), `emit_custom_type_encode_columns` (212), `emit_custom_type_decode_columns` (279).
- core/translate/expr/columns.rs `emit_stored_column` (126): for a column added by ALTER with a default, when the row has no such field, the default is computed and encoded.
- core/translate/expr/gencol.rs: `compute_virtual_columns` (10); `emit_virtual_column_store` (80) ENCODE/COERCE of generated values (strict tables only).

### 1.2 INSERT (core/translate/insert.rs)
- Order: pre-encode TypeCheck (773, `input_type_check_table_ref`) -> `emit_custom_type_encode` (785; defined 4403) -> post-encode TypeCheck (790, `type_check_table_ref`). Virtual columns computed in Stored form (806).
- NOT NULL: `emit_notnulls` (1877). If the column has a DECODE, it decodes to a temp register and checks the decoded value (1948-1968). Constraint-violation values are decoded via `emit_constraint_values_decode` when `row_is_encoded && table_has_custom_type && constraint_errors_show_values()` (2019-2028).
- Registers tagged with `RegisterColumn::table_column` (913-922) so partial-index WHEREs keep affinity/collation.
- Index keys: plain columns copied from encoded registers (3593-3645). Expression indexes: `emit_dml_expr_index_value` (3619, 1418).
- Defaults (`translate_column`, 2941-3010): `translate_expr_no_constant_opt(..., NoConstantOptReason::CustomTypeEncode)` when the chain has ENCODE or COERCE.
- CDC: `emit_cdc_patch_record` (1004) copies the key into the rowid-alias column and rebuilds the record from stored registers.
- Triggers: BEFORE triggers get `DmlRowForm::UserFacing` (645). AFTER triggers get NEW registers as `DmlRowForm::Stored` (1040-1060). FK child checks run before the write (948). RETURNING is seeded after the write (1101).

### 1.3 UPDATE (core/translate/emitter/update.rs)
- Columns not updated are read with `emit_table_column` (stored; 1004, 1455). Updated columns come from SET expressions; those with ENCODE/COERCE use `NoConstantOptReason::CustomTypeEncode` (901-921).
- `StoredExceptUpdated` used for virtual columns, triggers and BEFORE-trigger contexts (879, 1501, 1525, 1724). `emit_input_type_check` (1088) used at 1479 and 1664.
- Encode of updated columns plus post-encode TypeCheck only if `btree_table.is_strict` (1841-1865).
- CHECK constraints run afterwards (1867+), and only if they name a SET column.
- New partial-index WHERE: `emit_dml_expr_index_value` (1981). Old image: `emit_partial_index_where_old_image` (emitter/mod.rs:2121).
- Constraint-value decode for errors: 2179.

### 1.4 DELETE (emitter/delete.rs)
- OLD row for RETURNING/triggers read with `emit_table_column` (584, 967): stored form; decoded later by the trigger / RETURNING paths.

### 1.5 UPSERT (core/translate/upsert.rs)
- DO UPDATE on strict tables builds decoded copies of current, new and excluded rows via `emit_custom_type_decode_columns` (562-618), so WHERE and SET see decoded values. Column refs are rewritten to `Expr::Register{custom_type}` (1887-1890).
- After SET: virtual columns computed with `DmlRowForm::UserFacing` (787), ALL columns encoded (796), post-encode TypeCheck (807). Also the `input_type_check_table_ref` check (770).
- Expression-index values: `emit_dml_expr_index_value` (1820, 1859).

### 1.6 Column reads (SELECT and other expressions)
- translator.rs `translate_column_expr` (2451): BTree column -> `emit_stored_column`, then no affinity for custom types, then `emit_user_facing_column_value` unless `suppress_custom_type_decode`.
- `translate_self_table_column_expr` (2377) is the DML/self-table variant (SelfTableContext).
- FromClauseSubquery columns: copied from coroutine result registers or an ephemeral cursor, no decode. RecursiveCteInput: `Insn::Column`, no decode. `Expr::Variable` / `Expr::Register`: plain copies.

### 1.7 CAST (translator.rs `translate_cast_expr`, 731)
- Array CAST -> `emit_array_encode`.
- Custom-type CAST -> `emit_store_value` (ENCODE+COERCE), then `emit_decode`, then `emit_domain_cast_constraints` (741-745). Result is a decoded value. (The doc comment at expr_type.rs:332 says CAST gives the stored form; the code gives ENCODE-then-DECODE.)

### 1.8 Index keys
- Insert/update/upsert: see above (stored registers; expression indexes via `emit_dml_expr_index_value`).
- Old-image helpers, emitter/mod.rs `emit_index_column_value_old_image` (2060): expression index -> `translate_expr_no_constant_opt` under `SelfTableContext::ForSelect` (decoded); generated column -> `emit_table_column`; else `emit_stored_column`.
- New-image `emit_index_column_value_new_image` (2169): expression -> `emit_dml_expr_index_value`; plain -> stored registers.
- CREATE INDEX backfill (index.rs `emit_index_column_value_from_cursor`, 1320-1372): expression -> `translate_expr` under ForSelect (decoded); virtual generated column -> `emit_table_column`; else `emit_stored_column`.
- Seek keys (main_loop/seek.rs `encode_seek_keys_for_custom_types`, 23-92; called at 230, 357, in_seek.rs:101): applies the column's ENCODE (not COERCE) to each key unless the chain has no ENCODE, or the type "reads stored values unchanged" and `!converts_strings_compared_with_custom_types()`. Skips expression-index columns and NULL keys.

### 1.9 Expression indexes
- Insert/update/upsert use `emit_dml_expr_index_value`. Index value is computed from decoded column values (see 1.8). Index column collation from `expression_type_ordering` (index.rs:1090-1091, 1145-1155).

### 1.10 Defaults and generated columns
- Defaults: see 1.2 (ENCODE/COERCE with `NoConstantOptReason::CustomTypeEncode`). `suppress_column_default` (columns.rs:151). ALTER-added defaults: `emit_stored_column` computes+encodes.
- Generated (virtual) columns: gencol.rs (1.1). UPSERT computes them UserFacing (787); INSERT in Stored form (806); UPDATE uses `StoredExceptUpdated`.

### 1.11 Triggers (core/translate/trigger_exec.rs)
- `decode_trigger_registers` (1021-1076): non-strict tables skip. OLD registers always decoded. NEW registers decoded where `new_form.holds_stored_value(i)`. Result `new_form = UserFacing`.
- Trigger bodies get NEW/OLD as `Expr::Variable` carrying `custom_type` and collation (`variable_from_parameter_index`, 170-189; `column_custom_type_names` 555, 926). WHEN clauses use `Expr::Register{custom_type}` (1334). `populate_trigger_register_columns` (1078) records `RegisterColumn::trigger_row_column`.

### 1.12 RETURNING (emission.rs)
- `seed_returning_row_image_in_cache` (349): decodes each column into the cache with `needs_decode=false`.

### 1.13 CHECK (emitter/mod.rs `emit_check_constraints`, 2366-2485)
- Receives stored registers; custom-type columns with DECODE that are named by a CHECK are decoded into new registers (2385-2415) and cached by name (`Expr::Id`, `Expr::Qualified`, `Expr::Column`), all `needs_decode=false`.

### 1.14 Foreign keys (core/translate/fkeys.rs)
- Child key read stored (`emit_stored_column` / `emit_table_column_for_dml`, 564-575); compared with `Insn::Eq` using `fk_ref.parent_collations[i]` (583-589).
- For CASCADE / SET NULL / SET DEFAULT: key registers decoded in place by `decode_fk_key_registers` (1787-1804), since action subprograms read columns decoded (calls at 2408, 2541-2542, 2721).
- Missing-parent error info decoded via `emit_constraint_values_decode` (1522-1540).

### 1.15 CDC
- `emit_cdc_full_record` (emitter/mod.rs:1225) uses `emit_stored_column`: CDC images hold stored (encoded) values. INSERT: `emit_cdc_patch_record` (insert.rs:1004). Row-image builder at emitter/mod.rs:2000-2056 reads stored columns into a `DmlRowForm::Stored` context.

### 1.16 Views / IVM
- core/incremental/vdbe_maintenance/{plan.rs, binding.rs, join.rs, aggregate.rs, linear.rs}: no direct ENCODE/DECODE emission (grep only). They use `expr_collation` / `first_collation` (plan.rs:634, 650), `supported_key_collation` (672), `cache_expr_reg(..., false)` (binding.rs:117, 151; join.rs:402, 422), and pass `experimental_custom_types_enabled()` to the Resolver. Deeper reading skipped.
- Plain (non-materialized) views are expanded like subqueries (see section 6).

### 1.17 Sorters
- ORDER BY sorter (order_by.rs): bare custom-type column with a DECODE is translated with `suppress_custom_type_decode`, so the sorter holds STORED values (387-450). Result columns sharing a sort-key slot (`remapping.deduplicated`) are decoded after extraction with `emit_type_expr(decode_expr)` (321-343). Non-deduplicated result columns are translated separately (decoded). `result_column_custom_type_info` (30-51) recognises only a bare `Expr::Column`. Then `emit_array_decode_for_results` (347).
- GROUP BY sorter (group_by.rs:164-178, 701): no decode suppression; keys are decoded values.
- Compound ORDER BY (compound_select.rs:838+): sorter over already-decoded result columns.
- Window: partition/order keys decoded values (window.rs 649, 933, 2012).

### 1.18 Aggregates
- Aggregate args are translated with `translate_expr` (decoded values). min/max `AggStep` collation = `expr_collation(arg)` (aggregation.rs:618-647). Others use BINARY (500-757). DISTINCT aggregates: collation from `expr_collation(arg)` (main_loop/init.rs:90-92).

### 1.19 Window
- No encode/decode; keys and function args are decoded. RANGE with offset (window.rs:2560-2590) builds `order_expr +/- offset` and calls `custom_operator_call`; if the ORDER BY collation is `CollationSeq::Type`, there is no type arithmetic and offset is not NULL, it errors "RANGE with offset PRECEDING/FOLLOWING is not supported for column type X".

### 1.20 Compound selects / subqueries / CTEs / result rows
- core/translate/result_row.rs: result expressions translated with `translate_expr` (decoded; 79-113), then `emit_array_decode_for_results` (509-544) when `type_of(expr).is_array()`. For arrays only a bare `Column` result gets per-element `emit_array_decode(col)`; other array expressions get only `Insn::ArrayDecode` (skipped if `keep_array_results_as_blobs`).
- Destinations (160-460): ResultRows, EphemeralIndex (MakeRecord + IdxInsert with index collations; reorder + Sequence for seek indexes), EphemeralTable, RecursiveCteQueue, `CoroutineYield` (419), ExistsSubqueryResult, RowValueSubqueryResult (Copy), RowSet. None encodes or decodes; all carry already-decoded registers.
- DISTINCT dedup happens after `emit_array_decode_for_results` (result_row.rs ~117-135): on decoded results.

### 1.21 Hash-join caches (main_loop/hash.rs)
- Build side: payload columns read stored with `emit_stored_column` (385-403; affinity only if the type is not custom).
- Probe side: payload registers cached with `needs_decode=true` for columns (861-869); `copy_cached_expr_reg` applies `emit_user_facing_column_value`. This is the only `needs_decode=true` use.

### 1.22 Summary of who emits ENCODE/COERCE
Only INSERT, UPDATE (strict tables), UPSERT, generated columns, ALTER-added column defaults, and CAST. Seek keys get ENCODE only. Everything else only decodes (or reads stored).

---

## 2. Comparing, ordering, hashing, deduplicating; collation selection

### 2.1 Runtime consumers of `CollationSeq`
- Comparison jumps: vdbe/value.rs:135 `ComparisonOp::compare` -> `compare_operands`, called from vdbe/execute.rs:1094 / 1116. Numeric vs a non-number errors only here (`TypeOrdering::compare_operands`, type_ordering.rs:56).
- Index and sorter records: core/types.rs `compare_immutable` (2724, 3033, 3162, 4212) via `key_info.collation.compare`.
- Hash tables: vdbe/hash_table.rs:51 (hash), 97 and 110 (equality), 1050 (`can_hash` check).
- Aggregates: execute.rs:7598 (min/max); 8242, 8249, 8299 (ordered-set aggregates sort by collation).
- `CollationSeq::can_hash()` is false only for custom text collations. `Type` orderings are hashable.
- Equal values hash alike for each `TypeOrdering` (type_ordering.rs header comment, `hash` at 63).

### 2.2 Collation decision functions (core/translate/collate.rs)
- `comparison_collation` (491): tries `common_type_ordering` first, then text collation rules. Used for WHERE operators, hash join keys, constraints.
- `in_subquery_collation` (532), `in_list_collation` (558).
- `expr_collation` (575): used by sort, group, distinct, compound, window, index keys. Wrapped by `ExprContext::expr_collation` (expr_context.rs:48).
- `first_collation` (602); `compound_column_collation` (617).
- `key_collation` (704): table and index columns (explicit collation, else the column's type ordering).
- `Column::collation()` (schema.rs:7744): `Type(ordering)` if the column has one, else Text collation or BINARY.

### 2.3 Per-site table

| Site | Sees | Collation source | Pointer |
|---|---|---|---|
| WHERE `=,<,...` | decoded | `comparison_collation(lhs, rhs, ctx)` | collate.rs:491; optimizer/constraints.rs:845, 2238, 2315 |
| IN (list) | decoded | `in_list_collation` | constraints.rs:1108; collate.rs:558 |
| IN (subquery) | decoded | `in_subquery_collation` (ephemeral index collations too) | subquery.rs:1059-1081; constraints.rs:1400 |
| BETWEEN / CASE | decoded (expression compare) | comparison_collation via comparison jumps | execute.rs:1094/1116 |
| Index seek / range scan | key ENCODEd, index holds stored values | index column collation (`key_collation` / `column.collation()`); constraints gated by `index_column_can_serve` | index.rs:1070-1071; constraints.rs:320-355, 1303-1336; seek.rs:23-92 |
| ORDER BY sorter | STORED for a bare column with DECODE, otherwise decoded | `expr_collation` per ORDER BY expr | order_by.rs:128-141, 387-450, 613-633 |
| ORDER BY index satisfaction | stored (index order) | requires `target_col.collation == idx_col.collation`; breaks if `index_order_differs_from_value_order` | optimizer/order.rs:102, 1022, 1037, 1043 |
| GROUP BY | decoded | `expr_collation` | group_by.rs:164-178, 701; select.rs:1867 (`== BINARY` check) |
| DISTINCT (result) | decoded | `expr_collation` per result col | main_loop/init.rs:6-19; plan.rs:784-824 (`DistinctCtx`, `HashDistinct`) |
| DISTINCT aggregate | decoded | `expr_collation(arg)`; `DistinctCtx::new` rejects non-hashable | main_loop/init.rs:90-92 |
| Compound (UNION etc.) | decoded | `compound_column_collation(arms, col)` for every ephemeral dedup index | compound_select.rs:808-832 |
| Window PARTITION / ORDER | decoded | `expr_collation` | window.rs:649, 933, 2012 |
| min()/max() | decoded | `expr_collation(arg)` | aggregation.rs:618-647 |
| min/max index shortcut | stored (index) | `MinMaxDef.collation = expr_collation(arg)` | optimizer/mod.rs:933-940; optimizer/order.rs:120-140 |
| Ordered-set aggregates | decoded | `expr_collation`; custom text collations rejected | aggregation.rs:862-876; execute.rs:8242-8299 |
| Hash join keys | build: stored payload; keys compared by hash tables | `comparison_collation`; bloom filter only when all BINARY; hash join allowed only if collation `can_hash()` | main_loop/hash.rs:108-131; optimizer/access_method.rs:1286; hash_table.rs |
| Automatic index | per term | excludes non-`=` terms on columns with `index_order_differs_from_value_order` | constraints.rs:320-355 |
| UNIQUE / PK checks | stored | index column collations | index.rs:1070-1071 |
| FK checks | stored (child key read stored, parent compared stored) | `fk_ref.parent_collations[i]` from `column.collation()`; rowid alias -> BINARY; parent unique index matched on equal collation | fkeys.rs:564-589; schema.rs:4201-4227, 4257, 7270-7273 |
| Recursive CTE column | decoded | `first_collation` over initial and recursive exprs | recursive_cte.rs:263-298 |
| Subquery unnesting | decoded | compares inner and outer `expr_collation` | optimizer/unnest.rs:1149-1150 (grep only) |

### 2.4 Index order vs value order
- `Column::index_order_differs_from_value_order` (schema.rs:7758): true when the column has NO `type_ordering` AND its type is a custom type (`type_ordering.is_none() && get_type_def(..).is_some()`). So it is true for date, timestamp, uuid, boolean, etc., not only for types with a non-identity DECODE.
- Effect: such an index can serve `=` only, never range or ORDER BY (constraints.rs:320-355, 1303-1336; optimizer/order.rs:1037).
- ORDER BY sortable check (order_by.rs:91-103): rejects a bare custom-type column when `col.type_ordering().is_none() && !type_def.is_sortable()`.
- CREATE INDEX rejects struct/union types and unsortable types (index.rs:231-246).

---

## 3. Type-specific ordering machinery

- `TypeOrdering` (core/type_ordering.rs:19-28): `Numeric`, `Array` (non-text elements; text-form array read with numeric elements), `TextArray` (text elements). `ALL` at 31; `name()` 34 (EXPLAIN name); `compare` 42 (total, never fails); `compare_operands` 56 (numeric errors against a non-number; others never fail); `hash` 63.
- Numeric: `numeric_compare`, `numeric_compare_operands`, `numeric_hash` (core/numeric/decimal.rs). Arrays: `ArraySortKey` orders NULL < Elements < Other; elements compared element by element, shorter first; numbers hash as nearest real (type_ordering.rs:75-167).
- Becomes a `CollationSeq` as `CollationSeq::Type(ordering)`.
- Where attached:
  - Column: `Column::set_type_ordering` (schema.rs:7786); `Column::collation()` (7744); `Column::resolve_custom_type` (7765).
  - Derived (subquery/CTE/view) columns: plan.rs `query_output_columns` (3150-3228) sets `column.set_type_ordering(type_ordering(&result_type, schema))`.
  - Indexes: index.rs:1070-1071 (`key_collation(explicit, column.type_ordering())` or `column.collation()`); expression indexes via `expression_type_ordering` -> `type_of` over `TypeEnv::over_table` -> `type_ordering` (1090-1091, 1145-1155). Also alter.rs 1285, 2023, 2525, 2543 (grep only).
  - Expressions: expr_type.rs `type_ordering` (594).
- Which types have one: `builtin_ordering` (schema.rs:305) returns an ordering only for numeric. Arrays get Array/TextArray. No other built-in type (uuid, date, timestamp, ...) has a `TypeOrdering`; they compare by stored value. User types get none (schema.rs `from_create_type` 640).
- `TypeDef::is_sortable` (schema.rs:557): true if ordering is set, or no DECODE, or the type has `OPERATOR '<'`.
- `OperatorSignature` (schema.rs:261-300), `builtin_signatures` (329); user `OPERATOR '<'` clauses recorded at `from_create_type`. Operator calls found by `find_operator_call` (core/translate/expr/custom_type_operators.rs:42), used by `type_of` and codegen. `runs_custom_type_function` (used optimizer/mod.rs:4015, 4099) marks expressions that run custom-type functions (not nonnull-provable).
- `common_type_ordering` (collate.rs, called first by `comparison_collation`).
- `check_type_param_use` (schema.rs:807).

---

## 4. Expression typer: core/translate/expr/expr_type.rs

- `ExprType` (the value of the typer), `TypeEnv` (`TypeEnv::new(Some(table_refs), resolver)`, `TypeEnv::over_table`).
- `type_of` (207): per expression kind. Highlights:
  - Column: `column_reference_type` (476): FromClauseSubquery -> `subquery.column_types[column]`; RecursiveCteInput -> `input.column_types` (478); BTree -> `column_custom_type` (332), which reads the table column's type (resolve through the type registry; domain/alias chain).
  - Literal: plain builtin type.
  - CAST: type of the target. (Doc comment at 332 says stored form; translator.rs:741-745 gives the decoded value after ENCODE+DECODE.)
  - Function calls / operators: operator-signature lookup via `find_operator_call` (custom_type_operators.rs:42); otherwise function result types.
  - CASE / COALESCE: `common_type` (961).
  - Scalar / row-value subquery column: `SubqueryResult` RowValue with one register reads `row_value_subqueries.get(id).column(reg).expr_type` (RowValueSubqueryColumn recorded at subquery.rs:868-883 as `type_of(result_col.expr)`).
  - Compound: `compound_column_type` (508), `query_column_type` (486).
- `type_ordering` (594): maps an `ExprType` to `Option<TypeOrdering>`.
- Callers (non-exhaustive): collate.rs (`comparison_collation`, `expr_collation`), result_row.rs (array decode decision), plan.rs `query_output_columns` (3150-3228), index.rs (expression index type ordering), alter.rs (2525, 2543), result_column_types.rs (wire types; grep only), optimizer/unnest.rs:312-316, 1017, 1303, optimizer/mod.rs:912, 4462, subquery.rs:868-883, `ExprContext::type_env`.

---

## 5. Type definitions and dialect hooks

### 5.1 core/schema.rs
- `TypeDefKind` (241); `ResolvedType` (406); `TypeDef` (452) with `is_sortable` (557); `from_create_type` (640): builtins get ordering and signatures; user types get none.
- Registry helpers: `get_type_def` (1814), `column_has_decode` (1857), `resolve_type` (1874).
- `bootstrap_builtin_types` (1182-1244): built-in types as `CREATE TYPE` SQL, shared by both dialects:
  - uuid, boolean, json, jsonb, varchar (param), date, time, timestamp, timestamptz, smallint, bigint, inet, cidr, macaddr, macaddr8, bytea; aliases bool, int2, int8.
  - `numeric(value any, precision, scale)` BASE blob with ENCODE `numeric_encode`, COERCE `numeric_fit`, DECODE `numeric_decode`; the only builtin with a `TypeOrdering`.
  - `OPERATOR '<'` is on uuid, boolean, varchar, date, time, timestamp, timestamptz, smallint, bytea.
- Built-in PG types are NOT registered in `postgres/`: `PostgresDialect::register_catalog` just calls the sqlite `register_builtin_catalog` (postgres/frontend/catalog.rs).

### 5.2 PG frontend type mapping (postgres/parser/translator.rs)
- `map_pg_type` (~5855-5900): column types. BOOLEAN->boolean, SMALLINT->smallint, BIGINT->bigint, UUID->uuid, DATE/TIME/TIMETZ/TIMESTAMP/TIMESTAMPTZ->date/time/timestamp/timestamptz, BYTEA, INET, JSON, JSONB; VARCHAR(n)->varchar with params; bare VARCHAR/CHAR->TEXT; NUMERIC(p,s)->numeric with params; array types get dimensions.
- `pg_type_name_to_ast_type` (6061-6092, used for CAST): integers->INTEGER, REAL/NUMERIC/DECIMAL/MONEY->REAL, BOOLEAN->INTEGER, DATE/TIME/TIMESTAMP/TIMESTAMPTZ->custom names, UUID/JSON/JSONB/INET/CIDR/...->TEXT, BYTEA->BLOB, others kept as written.
- `translate_create_enum` (5941-6014): emits `CREATE TYPE <name>(value text) BASE text ENCODE CASE WHEN value IN (labels) THEN value ELSE RAISE(ABORT,'invalid input value for enum <name>') END DECODE value OPERATOR '<'`.
- postgres/frontend/catalog.rs:745, 1577, 1640: catalog queries use `get_type_def` / `get_type_def_unchecked` to report type OIDs and base types.

### 5.3 Dialect trait (core/dialect/mod.rs), all defaulting to SQLite behavior (false)
- Types/casts/comparisons: `requires_custom_types` (169), `converts_strings_compared_with_custom_types` (242), `casts_nonzero_integer_to_true` (268), `constraint_errors_show_values` (278).
- Related: `mod_of_integers_is_integer` (233), `errors_on_zero_divisor` (226) (arithmetic); `add_column_computes_default_for_existing_rows` (251) (defaults).
- `PostgresDialect` (postgres/frontend/catalog.rs:35-241) returns true for all of the above. It also sets `qualified_column_uses_nearest_table_only`, `binds_names_at_create`, `supports_namespaces`, `default_namespaces` ["public"], `upsert_rejects_row_written_by_same_statement`, `allows_qualified_columns_in_index_terms`.
- No Dialect method returns a type ordering, comparison collation, or type-specific operator.

---

## 6. How custom-type values leave subqueries, CTEs, views, compound selects

- Values leaving a subquery are DECODED. Result expressions are translated with `translate_expr` (decoded) and the destination (coroutine, ephemeral table/index, CTE queue, row-value register, exists) stores those decoded registers with no re-encode (result_row.rs 79-460).
- The outer query sees them as plain values. It learns the type from metadata, not from the bytes:
  - `query_output_columns` (plan.rs:3150-3228): for each derived column, affinity from `infer_type_from_expr` (or `compound_column_affinity`), `result_type = compound_column_type(arms)` for compounds else `type_of(result_expr, env)`, text collation, array dimensions, `column.set_type_ordering(type_ordering(&result_type, schema))`; returns `column_types: Vec<ExprType>` (3226). `subquery_output_columns` (3231) wraps it and makes names unique.
  - Callers: plan.rs:3296 (FROM subquery), 3341 (CTE/view with explicit column names), 3385 (grep only), planner.rs:1916 (rebuilt after join rewriting).
  - Outer reads `subquery.column_types[column]` via `column_reference_type` (expr_type.rs:476); RecursiveCteInput via `input.column_types` (478).
- Outer column read of a FromClauseSubquery column: `translate_column_expr` copies from the coroutine registers / ephemeral cursor, with no decode (so decoded value stays as is).
- Scalar/row-value subqueries: `RowValueSubqueryColumn{affinity, expr_type: type_of(result_col.expr)}` (subquery.rs:868-883).
- `IN (subquery)`: ephemeral index with collations from `in_subquery_collation` (subquery.rs:1059-1081).
- Compound selects: each arm decoded; column type from `compound_column_type` (expr_type.rs:508); collation `compound_column_collation` (collate.rs:617; compound_select.rs:819-832); dedup via ephemeral indexes with those collations; type ordering on the output column via `query_output_columns`.
- Recursive CTE: `recursive_cte_result_column_collation` (recursive_cte.rs:263-298); queue holds decoded values; reads via `Insn::Column` without decode.
- Views: expanded as FROM-subqueries (same path, plan.rs:3341). Materialized views / IVM: see 1.16.
- Array results: only a bare Column gets per-element `emit_array_decode`; other array expressions get `Insn::ArrayDecode` only (result_row.rs:509-544).

---

## 7. Neutral observations (facts that differ between sites)

- The ORDER BY sorter holds stored values for a bare custom-type column with a DECODE; GROUP BY, DISTINCT, compound and window keys hold decoded values.
- Sorter/index collation is chosen from `type_of`/`type_ordering`/`column.collation()`, not from whether the registers are stored or decoded.
- Index range scans and ORDER BY service are disabled for every custom type with no `TypeOrdering` (`index_order_differs_from_value_order`), including types with identity DECODE.
- Seek keys get ENCODE only (no COERCE), and are skipped when the type reads stored values unchanged and the dialect does not convert strings.
- The `column_custom_type` doc comment (expr_type.rs:332) disagrees with the CAST code (translator.rs:741-745).
- Array results that are not a bare column are not element-decoded.
- `needs_decode` on the resolver cache is used only for hash-join payloads.
- Not inspected (grep only): alter.rs, result_column_types.rs, optimizer/unnest.rs, translate/schema.rs:548 and 3173, vdbe/vacuum.rs, mvcc/*, incremental internals.
