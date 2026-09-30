-- JSON_TABLE

-- Should fail (JSON_TABLE can be used only in FROM clause)
SELECT JSON_TABLE('[]', '$');

-- Only allow EMPTY and ERROR for ON ERROR
SELECT * FROM JSON_TABLE('[]', 'strict $.a' COLUMNS (js2 int PATH '$') DEFAULT 1 ON ERROR);
SELECT * FROM JSON_TABLE('[]', 'strict $.a' COLUMNS (js2 int PATH '$') NULL ON ERROR);
SELECT * FROM JSON_TABLE('[]', 'strict $.a' COLUMNS (js2 int PATH '$') EMPTY ON ERROR);
SELECT * FROM JSON_TABLE('[]', 'strict $.a' COLUMNS (js2 int PATH '$') ERROR ON ERROR);

-- Column and path names must be distinct
SELECT * FROM JSON_TABLE(jsonb'"1.23"', '$.a' as js2 COLUMNS (js2 int path '$'));

-- Should fail (no columns)
SELECT * FROM JSON_TABLE(NULL, '$' COLUMNS ());

SELECT * FROM JSON_TABLE (NULL::jsonb, '$' COLUMNS (v1 timestamp)) AS f (v1, v2);

--duplicated column name
SELECT * FROM JSON_TABLE(jsonb'"1.23"', '$.a' COLUMNS (js2 int path '$', js2 int path '$'));

--return composite data type.
create type comp as (a int, b int);
SELECT * FROM JSON_TABLE(jsonb '{"rec": "(1,2)"}', '$' COLUMNS (id FOR ORDINALITY, comp comp path '$.rec' omit quotes)) jt;
drop type comp;

-- NULL => empty table
SELECT * FROM JSON_TABLE(NULL::jsonb, '$' COLUMNS (foo int)) bar;
SELECT * FROM JSON_TABLE(jsonb'"1.23"', 'strict $.a' COLUMNS (js2 int PATH '$'));

--
SELECT * FROM JSON_TABLE(jsonb '123', '$'
	COLUMNS (item int PATH '$', foo int)) bar;

-- JSON_TABLE: basic functionality
CREATE DOMAIN jsonb_test_domain AS text CHECK (value <> 'foo');
CREATE TEMP TABLE json_table_test (js) AS
	(VALUES
		('1'),
		('[]'),
		('{}'),
		('[1, 1.23, "2", "aaaaaaa", "foo", null, false, true, {"aaa": 123}, "[1,2]", "\"str\""]')
	);

-- Regular "unformatted" columns
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			"int" int PATH '$',
			"text" text PATH '$',
			"char(4)" char(4) PATH '$',
			"bool" bool PATH '$',
			"numeric" numeric PATH '$',
			"domain" jsonb_test_domain PATH '$',
			js json PATH '$',
			jb jsonb PATH '$'
		)
	) jt
	ON true;

-- "formatted" columns
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			jst text    FORMAT JSON  PATH '$',
			jsc char(4) FORMAT JSON  PATH '$',
			jsv varchar(4) FORMAT JSON  PATH '$',
			jsb jsonb FORMAT JSON PATH '$',
			jsbq jsonb FORMAT JSON PATH '$' OMIT QUOTES
		)
	) jt
	ON true;

-- EXISTS columns
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			exists1 bool EXISTS PATH '$.aaa',
			exists2 int EXISTS PATH '$.aaa',
			exists3 int EXISTS PATH 'strict $.aaa' UNKNOWN ON ERROR,
			exists4 text EXISTS PATH 'strict $.aaa' FALSE ON ERROR
		)
	) jt
	ON true;

-- Other miscellaneous checks
SELECT *
FROM json_table_test vals
	LEFT OUTER JOIN
	JSON_TABLE(
		vals.js::jsonb, 'lax $[*]'
		COLUMNS (
			id FOR ORDINALITY,
			aaa int, -- "aaa" has implicit path '$."aaa"'
			aaa1 int PATH '$.aaa',
			js2 json PATH '$',
			jsb2w jsonb PATH '$' WITH WRAPPER,
			jsb2q jsonb PATH '$' OMIT QUOTES,
			ia int[] PATH '$',
			ta text[] PATH '$',
			jba jsonb[] PATH '$'
		)
	) jt
	ON true;

-- Test using casts in DEFAULT .. ON ERROR expression
SELECT * FROM JSON_TABLE(jsonb '{"d1": "H"}', '$'
    COLUMNS (js1 jsonb_test_domain PATH '$.a2' DEFAULT '"foo1"'::jsonb::text ON EMPTY));

SELECT * FROM JSON_TABLE(jsonb '{"d1": "H"}', '$'
    COLUMNS (js1 jsonb_test_domain PATH '$.a2' DEFAULT 'foo'::jsonb_test_domain ON EMPTY));

SELECT * FROM JSON_TABLE(jsonb '{"d1": "H"}', '$'
    COLUMNS (js1 jsonb_test_domain PATH '$.a2' DEFAULT 'foo1'::jsonb_test_domain ON EMPTY));

SELECT * FROM JSON_TABLE(jsonb '{"d1": "foo"}', '$'
    COLUMNS (js1 jsonb_test_domain PATH '$.d1' DEFAULT 'foo2'::jsonb_test_domain ON ERROR));

SELECT * FROM JSON_TABLE(jsonb '{"d1": "foo"}', '$'
    COLUMNS (js1 oid[] PATH '$.d2' DEFAULT '{1}'::int[]::oid[] ON EMPTY));

-- JSON_TABLE: Test backward parsing

CREATE VIEW jsonb_table_view2 AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			"int" int PATH '$',
			"text" text PATH '$',
			"char(4)" char(4) PATH '$',
			"bool" bool PATH '$',
			"numeric" numeric PATH '$',
			"domain" jsonb_test_domain PATH '$'));

CREATE VIEW jsonb_table_view3 AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			js json PATH '$',
			jb jsonb PATH '$',
			jst text    FORMAT JSON  PATH '$',
			jsc char(4) FORMAT JSON  PATH '$',
			jsv varchar(4) FORMAT JSON  PATH '$'));

CREATE VIEW jsonb_table_view4 AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
            jsb jsonb   FORMAT JSON PATH '$',
            jsbq jsonb FORMAT JSON PATH '$' OMIT QUOTES,
            aaa int, -- implicit path '$."aaa"',
            aaa1 int PATH '$.aaa'));

CREATE VIEW jsonb_table_view5 AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			exists1 bool EXISTS PATH '$.aaa',
			exists2 int EXISTS PATH '$.aaa' TRUE ON ERROR,
			exists3 text EXISTS PATH 'strict $.aaa' UNKNOWN ON ERROR));

CREATE VIEW jsonb_table_view6 AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			js2 json PATH '$',
			jsb2w jsonb PATH '$' WITH WRAPPER,
			jsb2q jsonb PATH '$' OMIT QUOTES,
			ia int[] PATH '$',
			ta text[] PATH '$',
			jba jsonb[] PATH '$'));

\sv jsonb_table_view2
\sv jsonb_table_view3
\sv jsonb_table_view4
\sv jsonb_table_view5
\sv jsonb_table_view6

EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM jsonb_table_view2;
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM jsonb_table_view3;
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM jsonb_table_view4;
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM jsonb_table_view5;
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM jsonb_table_view6;

-- JSON_TABLE() with alias
EXPLAIN (COSTS OFF, VERBOSE)
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			id FOR ORDINALITY,
			"int" int PATH '$',
			"text" text PATH '$'
	)) json_table_func;

EXPLAIN (COSTS OFF, FORMAT JSON, VERBOSE)
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			id FOR ORDINALITY,
			"int" int PATH '$',
			"text" text PATH '$'
	)) json_table_func;

DROP VIEW jsonb_table_view2;
DROP VIEW jsonb_table_view3;
DROP VIEW jsonb_table_view4;
DROP VIEW jsonb_table_view5;
DROP VIEW jsonb_table_view6;
DROP DOMAIN jsonb_test_domain;

-- JSON_TABLE: only one FOR ORDINALITY columns allowed
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (id FOR ORDINALITY, id2 FOR ORDINALITY, a int PATH '$.a' ERROR ON EMPTY)) jt;
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (id FOR ORDINALITY, a int PATH '$' ERROR ON EMPTY)) jt;

-- JSON_TABLE: ON EMPTY/ON ERROR behavior
SELECT *
FROM
	(VALUES ('1'), ('"err"')) vals(js),
	JSON_TABLE(vals.js::jsonb, '$' COLUMNS (a int PATH '$')) jt;

SELECT *
FROM
	(VALUES ('1'), ('"err"')) vals(js)
		LEFT OUTER JOIN
	JSON_TABLE(vals.js::jsonb, '$' COLUMNS (a int PATH '$' ERROR ON ERROR)) jt
		ON true;

-- TABLE-level ERROR ON ERROR is not propagated to columns
SELECT *
FROM
	(VALUES ('1'), ('"err"')) vals(js)
		LEFT OUTER JOIN
	JSON_TABLE(vals.js::jsonb, '$' COLUMNS (a int PATH '$' ERROR ON ERROR)) jt
		ON true;

SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH '$.a' ERROR ON EMPTY)) jt;
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH 'strict $.a' ERROR ON ERROR) ERROR ON ERROR) jt;
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int PATH 'lax $.a' ERROR ON EMPTY) ERROR ON ERROR) jt;

SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int PATH '$'   DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int PATH 'strict $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int PATH 'lax $.a' DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR)) jt;

-- JSON_TABLE: EXISTS PATH types
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int4 EXISTS PATH '$.a' ERROR ON ERROR));	-- ok; can cast to int4
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int4 EXISTS PATH '$' ERROR ON ERROR));	-- ok; can cast to int4
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int2 EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a int8 EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a float4 EXISTS PATH '$.a'));
-- Default FALSE (ON ERROR) doesn't fit char(3)
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a char(3) EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a char(3) EXISTS PATH '$.a' ERROR ON ERROR));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a char(5) EXISTS PATH '$.a' ERROR ON ERROR));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a json EXISTS PATH '$.a'));
SELECT * FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a jsonb EXISTS PATH '$.a'));

-- EXISTS PATH domain over int
CREATE DOMAIN dint4 AS int;
CREATE DOMAIN dint4_0 AS int CHECK (VALUE <> 0 );
SELECT a, a::bool FROM JSON_TABLE(jsonb '"a"', '$' COLUMNS (a dint4 EXISTS PATH '$.a' ));
SELECT a, a::bool FROM JSON_TABLE(jsonb '{"a":1}', '$' COLUMNS (a dint4_0 EXISTS PATH '$.b'));
SELECT a, a::bool FROM JSON_TABLE(jsonb '{"a":1}', '$' COLUMNS (a dint4_0 EXISTS PATH '$.b' ERROR ON ERROR));
SELECT a, a::bool FROM JSON_TABLE(jsonb '{"a":1}', '$' COLUMNS (a dint4_0 EXISTS PATH '$.b' FALSE ON ERROR));
SELECT a, a::bool FROM JSON_TABLE(jsonb '{"a":1}', '$' COLUMNS (a dint4_0 EXISTS PATH '$.b' TRUE ON ERROR));
DROP DOMAIN dint4, dint4_0;

-- JSON_TABLE: WRAPPER/QUOTES clauses on scalar columns
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' KEEP QUOTES ON SCALAR STRING));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' OMIT QUOTES ON SCALAR STRING));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' KEEP QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' OMIT QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' WITHOUT WRAPPER KEEP QUOTES));
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' WITHOUT WRAPPER OMIT QUOTES));

SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' WITH WRAPPER));

-- Error: OMIT QUOTES should not be specified when WITH WRAPPER is present
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text PATH '$' WITH WRAPPER OMIT QUOTES));
-- But KEEP QUOTES (the default) is fine
SELECT * FROM JSON_TABLE(jsonb '"world"', '$' COLUMNS (item text FORMAT JSON PATH '$' WITH WRAPPER KEEP QUOTES));

-- Test PASSING args
SELECT *
FROM JSON_TABLE(
	jsonb '[1,2,3]',
	'$[*] ? (@ < $x)'
		PASSING 3 AS x
		COLUMNS (y text FORMAT JSON PATH '$')
	) jt;

-- PASSING arguments are also passed to column paths
SELECT *
FROM JSON_TABLE(
	jsonb '[1,2,3]',
	'$[*] ? (@ < $x)'
		PASSING 10 AS x, 3 AS y
		COLUMNS (a text FORMAT JSON PATH '$ ? (@ < $y)')
	) jt;

-- Should fail (not supported)
SELECT * FROM JSON_TABLE(jsonb '{"a": 123}', '$' || '.' || 'a' COLUMNS (foo int));

-- JsonPathQuery() error message mentioning column name
SELECT * FROM JSON_TABLE('{"a": [{"b": "1"}, {"b": "2"}]}', '$' COLUMNS (b json path '$.a[*].b' ERROR ON ERROR));

-- JSON_TABLE: nested paths

-- Duplicate path names
SELECT * FROM JSON_TABLE(
	jsonb '[]', '$' AS a
	COLUMNS (
		b int,
		NESTED PATH '$' AS a
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$' AS a
	COLUMNS (
		b int,
		NESTED PATH '$' AS n_a
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$'
	COLUMNS (
		b int,
		NESTED PATH '$' AS b
		COLUMNS (
			c int
		)
	)
) jt;

SELECT * FROM JSON_TABLE(
	jsonb '[]', '$'
	COLUMNS (
		NESTED PATH '$' AS a
		COLUMNS (
			b int
		),
		NESTED PATH '$'
		COLUMNS (
			NESTED PATH '$' AS a
			COLUMNS (
				c int
			)
		)
	)
) jt;


-- JSON_TABLE: plan execution

CREATE TEMP TABLE jsonb_table_test (js jsonb);

INSERT INTO jsonb_table_test
VALUES (
	'[
		{"a":  1,  "b": [], "c": []},
		{"a":  2,  "b": [1, 2, 3], "c": [10, null, 20]},
		{"a":  3,  "b": [1, 2], "c": []},
		{"x": "4", "b": [1, 2], "c": 123}
	 ]'
);

select
	jt.*
from
	jsonb_table_test jtt,
	json_table (
		jtt.js,'strict $[*]' as p
		columns (
			n for ordinality,
			a int path 'lax $.a' default -1 on empty,
			nested path 'strict $.b[*]' as pb columns (b_id for ordinality, b int path '$' ),
			nested path 'strict $.c[*]' as pc columns (c_id for ordinality, c int path '$' )
		)
	) jt;


-- PASSING arguments are passed to nested paths and their columns' paths
SELECT *
FROM
	generate_series(1, 3) x,
	generate_series(1, 3) y,
	JSON_TABLE(jsonb
		'[[1,2,3],[2,3,4,5],[3,4,5,6]]',
		'strict $[*] ? (@[*] <= $x)'
		PASSING x AS x, y AS y
		COLUMNS (
			y text FORMAT JSON PATH '$',
			NESTED PATH 'strict $[*] ? (@ == $y)'
			COLUMNS (
				z int PATH '$'
			)
		)
	) jt;

-- JSON_TABLE: Test backward parsing with nested paths

CREATE VIEW jsonb_table_view_nested AS
SELECT * FROM
	JSON_TABLE(
		jsonb 'null', 'lax $[*]' PASSING 1 + 2 AS a, json '"foo"' AS "b c"
		COLUMNS (
			id FOR ORDINALITY,
			NESTED PATH '$[1]' AS p1 COLUMNS (
				a1 int,
				NESTED PATH '$[*]' AS "p1 1" COLUMNS (
					a11 text
				),
				b1 text
			),
			NESTED PATH '$[2]' AS p2 COLUMNS (
				NESTED PATH '$[*]' AS "p2:1" COLUMNS (
					a21 text
				),
				NESTED PATH '$[*]' AS p22 COLUMNS (
					a22 text
				)
			)
		)
	);

\sv jsonb_table_view_nested
DROP VIEW jsonb_table_view_nested;

CREATE TABLE s (js jsonb);
INSERT INTO s VALUES
	('{"a":{"za":[{"z1": [11,2222]},{"z21": [22, 234,2345]},{"z22": [32, 204,145]}]},"c": 3}'),
	('{"a":{"za":[{"z1": [21,4222]},{"z21": [32, 134,1345]}]},"c": 10}');

-- error
SELECT sub.* FROM s,
	JSON_TABLE(js, '$' PASSING 32 AS x, 13 AS y COLUMNS (
		xx int path '$.c',
		NESTED PATH '$.a.za[1]' columns (NESTED PATH '$.z21[*]' COLUMNS (z21 int path '$?(@ >= $"x")' ERROR ON ERROR))
	)) sub;

-- Parent columns xx1, xx appear before NESTED ones
SELECT sub.* FROM s,
	(VALUES (23)) x(x), generate_series(13, 13) y,
	JSON_TABLE(js, '$' AS c1 PASSING x AS x, y AS y COLUMNS (
		NESTED PATH '$.a.za[2]' COLUMNS (
			NESTED PATH '$.z22[*]' as z22 COLUMNS (c int PATH '$')),
			NESTED PATH '$.a.za[1]' columns (d int[] PATH '$.z21'),
			NESTED PATH '$.a.za[0]' columns (NESTED PATH '$.z1[*]' as z1 COLUMNS (a int PATH  '$')),
			xx1 int PATH '$.c',
			NESTED PATH '$.a.za[1]'  columns (NESTED PATH '$.z21[*]' as z21 COLUMNS (b int PATH '$')),
			xx int PATH '$.c'
	)) sub;

-- Test applying PASSING variables at different nesting levels
SELECT sub.* FROM s,
	(VALUES (23)) x(x), generate_series(13, 13) y,
	JSON_TABLE(js, '$' AS c1 PASSING x AS x, y AS y COLUMNS (
		xx1 int PATH '$.c',
		NESTED PATH '$.a.za[0].z1[*]' COLUMNS (NESTED PATH '$ ?(@ >= ($"x" -2))' COLUMNS (a int PATH '$')),
		NESTED PATH '$.a.za[0]' COLUMNS (NESTED PATH '$.z1[*] ? (@ >= ($"x" -2))' COLUMNS (b int PATH '$'))
	)) sub;

-- Test applying PASSING variable to paths all the levels
SELECT sub.* FROM s,
	(VALUES (23)) x(x),
	generate_series(13, 13) y,
	JSON_TABLE(js, '$' AS c1 PASSING x AS x, y AS y
	COLUMNS (
		xx1 int PATH '$.c',
		NESTED PATH '$.a.za[1]'
			COLUMNS (NESTED PATH '$.z21[*]' COLUMNS (b int PATH '$')),
		NESTED PATH '$.a.za[1] ? (@.z21[*] >= ($"x"-1))' COLUMNS
			(NESTED PATH '$.z21[*] ? (@ >= ($"y" + 3))' as z22 COLUMNS (a int PATH '$ ? (@ >= ($"y" + 12))')),
		NESTED PATH '$.a.za[1]' COLUMNS
			(NESTED PATH '$.z21[*] ? (@ >= ($"y" +121))' as z21 COLUMNS (c int PATH '$ ? (@ > ($"x" +111))'))
	)) sub;

----- test on empty behavior
SELECT sub.* FROM s,
	(values(23)) x(x),
	generate_series(13, 13) y,
	JSON_TABLE(js, '$' AS c1 PASSING x AS x, y AS y
	COLUMNS (
		xx1 int PATH '$.c',
		NESTED PATH '$.a.za[2]' COLUMNS (NESTED PATH '$.z22[*]' as z22 COLUMNS (c int PATH '$')),
		NESTED PATH '$.a.za[1]' COLUMNS (d json PATH '$ ? (@.z21[*] == ($"x" -1))'),
		NESTED PATH '$.a.za[0]' COLUMNS (NESTED PATH '$.z1[*] ? (@ >= ($"x" -2))' as z1 COLUMNS (a int PATH '$')),
		NESTED PATH '$.a.za[1]' COLUMNS
			(NESTED PATH '$.z21[*] ? (@ >= ($"y" +121))' as z21 COLUMNS (b int PATH '$ ? (@ > ($"x" +111))' DEFAULT 0 ON EMPTY))
	)) sub;

CREATE OR REPLACE VIEW jsonb_table_view7 AS
SELECT sub.* FROM s,
	(values(23)) x(x),
	generate_series(13, 13) y,
	JSON_TABLE(js, '$' AS c1 PASSING x AS x, y AS y
	COLUMNS (
		xx1 int PATH '$.c',
		NESTED PATH '$.a.za[2]' COLUMNS (NESTED PATH '$.z22[*]' as z22 COLUMNS (c int PATH '$' WITHOUT WRAPPER OMIT QUOTES)),
		NESTED PATH '$.a.za[1]' COLUMNS (d json PATH '$ ? (@.z21[*] == ($"x" -1))' WITH WRAPPER),
		NESTED PATH '$.a.za[0]' COLUMNS (NESTED PATH '$.z1[*] ? (@ >= ($"x" -2))' as z1 COLUMNS (a int PATH '$' KEEP QUOTES)),
		NESTED PATH '$.a.za[1]' COLUMNS
			(NESTED PATH '$.z21[*] ? (@ >= ($"y" +121))' as z21 COLUMNS (b int PATH '$ ? (@ > ($"x" +111))' DEFAULT 0 ON EMPTY))
	)) sub;
\sv jsonb_table_view7
DROP VIEW jsonb_table_view7;
DROP TABLE s;

-- Prevent ON EMPTY specification on EXISTS columns
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int exists empty object on empty));

-- Test ON ERROR / EMPTY value validity for the function and column types;
-- all fail
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int) NULL ON ERROR);
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int true on empty));
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int omit quotes true on error));
SELECT * FROM JSON_TABLE(jsonb '1', '$' COLUMNS (a int exists empty object on error));

-- Test JSON_TABLE() column deparsing -- don't emit default ON ERROR / EMPTY
-- behavior
CREATE VIEW json_table_view8 AS SELECT * from JSON_TABLE('"a"', '$' COLUMNS (a text PATH '$'));
\sv json_table_view8;

CREATE VIEW json_table_view9 AS SELECT * from JSON_TABLE('"a"', '$' COLUMNS (a text PATH '$') ERROR ON ERROR);
\sv json_table_view9;

DROP VIEW json_table_view8, json_table_view9;

-- Test JSON_TABLE() deparsing -- don't emit default ON ERROR behavior
CREATE VIEW json_table_view8 AS SELECT * from JSON_TABLE('"a"', '$' COLUMNS (a text PATH '$') EMPTY ON ERROR);
\sv json_table_view8;

CREATE VIEW json_table_view9 AS SELECT * from JSON_TABLE('"a"', '$' COLUMNS (a text PATH '$') EMPTY ARRAY ON ERROR);
\sv json_table_view9;

DROP VIEW json_table_view8, json_table_view9;
