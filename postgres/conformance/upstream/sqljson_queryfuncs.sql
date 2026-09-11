-- JSON_EXISTS
SELECT JSON_EXISTS(NULL::jsonb, '$');
SELECT JSON_EXISTS(jsonb '[]', '$');
SELECT JSON_EXISTS(JSON_OBJECT(RETURNING jsonb), '$');

SELECT JSON_EXISTS(jsonb '1', '$');
SELECT JSON_EXISTS(jsonb 'null', '$');
SELECT JSON_EXISTS(jsonb '[]', '$');

SELECT JSON_EXISTS(jsonb '1', '$.a');
SELECT JSON_EXISTS(jsonb '1', 'strict $.a'); -- FALSE on error
SELECT JSON_EXISTS(jsonb '1', 'strict $.a' ERROR ON ERROR);
SELECT JSON_EXISTS(jsonb 'null', '$.a');
SELECT JSON_EXISTS(jsonb '[]', '$.a');
SELECT JSON_EXISTS(jsonb '[1, "aaa", {"a": 1}]', 'strict $.a'); -- FALSE on error
SELECT JSON_EXISTS(jsonb '[1, "aaa", {"a": 1}]', 'lax $.a');
SELECT JSON_EXISTS(jsonb '{}', '$.a');
SELECT JSON_EXISTS(jsonb '{"b": 1, "a": 2}', '$.a');

SELECT JSON_EXISTS(jsonb '1', '$.a.b');
SELECT JSON_EXISTS(jsonb '{"a": {"b": 1}}', '$.a.b');
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.a.b');

SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x)' PASSING 1 AS x);
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x)' PASSING '1' AS x);
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 2 AS y);
SELECT JSON_EXISTS(jsonb '{"a": 1, "b": 2}', '$.* ? (@ > $x && @ < $y)' PASSING 0 AS x, 1 AS y);

-- extension: boolean expressions
SELECT JSON_EXISTS(jsonb '1', '$ > 2');
SELECT JSON_EXISTS(jsonb '1', '$.a > 2' ERROR ON ERROR);

-- JSON_VALUE
SELECT JSON_VALUE(NULL::jsonb, '$');

SELECT JSON_VALUE(jsonb 'null', '$');
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING int);

SELECT JSON_VALUE(jsonb 'true', '$');
SELECT JSON_VALUE(jsonb 'true', '$' RETURNING bool);

SELECT JSON_VALUE(jsonb '123', '$');
SELECT JSON_VALUE(jsonb '123', '$' RETURNING int) + 234;
SELECT JSON_VALUE(jsonb '123', '$' RETURNING text);
/* jsonb bytea ??? */
SELECT JSON_VALUE(jsonb '123', '$' RETURNING bytea ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '1.23', '$');
SELECT JSON_VALUE(jsonb '1.23', '$' RETURNING int);
SELECT JSON_VALUE(jsonb '"1.23"', '$' RETURNING numeric);
SELECT JSON_VALUE(jsonb '"1.23"', '$' RETURNING int ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '"aaa"', '$');
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING text);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(5));
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(2) ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(2));
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING char(3) ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING json);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING jsonb);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING json ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING jsonb ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"\"aaa\""', '$' RETURNING json);
SELECT JSON_VALUE(jsonb '"\"aaa\""', '$' RETURNING jsonb);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING int);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING int ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '"aaa"', '$' RETURNING int DEFAULT 111 ON ERROR);
SELECT JSON_VALUE(jsonb '"123"', '$' RETURNING int) + 234;

SELECT JSON_VALUE(jsonb '"2017-02-20"', '$' RETURNING date) + 9;

-- Test NULL checks execution in domain types
CREATE DOMAIN sqljsonb_int_not_null AS int NOT NULL;
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING sqljsonb_int_not_null);
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING sqljsonb_int_not_null ERROR ON ERROR);
SELECT JSON_VALUE(jsonb 'null', '$' RETURNING sqljsonb_int_not_null DEFAULT 2 ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1',  '$.a' RETURNING sqljsonb_int_not_null DEFAULT 2 ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1',  '$.a' RETURNING sqljsonb_int_not_null DEFAULT NULL ON EMPTY ERROR ON ERROR);
CREATE TYPE rainbow AS ENUM ('red', 'orange', 'yellow', 'green', 'blue', 'purple');
CREATE DOMAIN rgb AS rainbow CHECK (VALUE IN ('red', 'green', 'blue'));
SELECT JSON_VALUE('"purple"'::jsonb, 'lax $[*]' RETURNING rgb);
SELECT JSON_VALUE('"purple"'::jsonb, 'lax $[*]' RETURNING rgb ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '[]', '$');
SELECT JSON_VALUE(jsonb '[]', '$' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '{}', '$');
SELECT JSON_VALUE(jsonb '{}', '$' ERROR ON ERROR);

SELECT JSON_VALUE(jsonb '1', '$.a');
SELECT JSON_VALUE(jsonb '1', 'strict $.a' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'strict $.a' DEFAULT 'error' ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' ERROR ON ERROR);	-- NULL ON EMPTY
SELECT JSON_VALUE(jsonb '1', 'lax $.a' ERROR ON EMPTY ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'strict $.*' DEFAULT 2 ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' DEFAULT 2 ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' DEFAULT '2' ON EMPTY);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' NULL ON EMPTY DEFAULT '2' ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' DEFAULT '2' ON EMPTY DEFAULT '3' ON ERROR);
SELECT JSON_VALUE(jsonb '1', 'lax $.a' ERROR ON EMPTY DEFAULT '3' ON ERROR);

SELECT JSON_VALUE(jsonb '[1,2]', '$[*]' ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '[1,2]', '$[*]' DEFAULT '0' ON ERROR);
SELECT JSON_VALUE(jsonb '[" "]', '$[*]' RETURNING int ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '[" "]', '$[*]' RETURNING int DEFAULT 2 + 3 ON ERROR);
SELECT JSON_VALUE(jsonb '["1"]', '$[*]' RETURNING int DEFAULT 2 + 3 ON ERROR);
SELECT JSON_VALUE(jsonb '["1"]', '$[*]' RETURNING int FORMAT JSON); -- RETURNING FORMAT not allowed

-- RETUGNING pseudo-types not allowed
SELECT JSON_VALUE(jsonb '["1"]', '$[*]' RETURNING record);

SELECT
	x,
	JSON_VALUE(
		jsonb '{"a": 1, "b": 2}',
		'$.* ? (@ > $x)' PASSING x AS x
		RETURNING int
		DEFAULT -1 ON EMPTY
		DEFAULT -2 ON ERROR
	) y
FROM
	generate_series(0, 2) x;

SELECT JSON_VALUE(jsonb 'null', '$a' PASSING point ' (1, 2 )' AS a);
SELECT JSON_VALUE(jsonb 'null', '$a' PASSING point ' (1, 2 )' AS a RETURNING point);
SELECT JSON_VALUE(jsonb 'null', '$a' PASSING point ' (1, 2 )' AS a RETURNING point ERROR ON ERROR);

-- Test PASSING and RETURNING date/time types
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING timestamptz);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING timestamp);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING date '2018-02-21 12:34:56 +10' AS ts RETURNING date);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING time '2018-02-21 12:34:56 +10' AS ts RETURNING time);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timetz '2018-02-21 12:34:56 +10' AS ts RETURNING timetz);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamp '2018-02-21 12:34:56 +10' AS ts RETURNING timestamp);

-- Also test RETURNING json[b]
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING json);
SELECT JSON_VALUE(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING jsonb);

-- Test that numeric JSON values are coerced uniformly
select json_value('{"a": 1.234}', '$.a' returning int error on error);
select json_value('{"a": "1.234"}', '$.a' returning int error on error);

-- JSON_QUERY

SELECT JSON_VALUE(NULL::jsonb, '$');

SELECT
	JSON_QUERY(js, '$') AS "unspec",
	JSON_QUERY(js, '$' WITHOUT WRAPPER) AS "without",
	JSON_QUERY(js, '$' WITH CONDITIONAL WRAPPER) AS "with cond",
	JSON_QUERY(js, '$' WITH UNCONDITIONAL ARRAY WRAPPER) AS "with uncond",
	JSON_QUERY(js, '$' WITH ARRAY WRAPPER) AS "with"
FROM
	(VALUES
		(jsonb 'null'),
		('12.3'),
		('true'),
		('"aaa"'),
		('[1, null, "2"]'),
		('{"a": 1, "b": [2]}')
	) foo(js);

SELECT
	JSON_QUERY(js, 'strict $[*]') AS "unspec",
	JSON_QUERY(js, 'strict $[*]' WITHOUT WRAPPER) AS "without",
	JSON_QUERY(js, 'strict $[*]' WITH CONDITIONAL WRAPPER) AS "with cond",
	JSON_QUERY(js, 'strict $[*]' WITH UNCONDITIONAL ARRAY WRAPPER) AS "with uncond",
	JSON_QUERY(js, 'strict $[*]' WITH ARRAY WRAPPER) AS "with"
FROM
	(VALUES
		(jsonb '1'),
		('[]'),
		('[null]'),
		('[12.3]'),
		('[true]'),
		('["aaa"]'),
		('[[1, 2, 3]]'),
		('[{"a": 1, "b": [2]}]'),
		('[1, "2", null, [3]]')
	) foo(js);

SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text KEEP QUOTES);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text KEEP QUOTES ON SCALAR STRING);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text OMIT QUOTES);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING text OMIT QUOTES ON SCALAR STRING);
SELECT JSON_QUERY(jsonb '"aaa"', '$' OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING json OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING bytea FORMAT JSON OMIT QUOTES ERROR ON ERROR);

-- Behavior when a RETURNING type has typmod != -1
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING char(3) ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING char(3));
SELECT JSON_QUERY(jsonb '"aaa"', '$' RETURNING char(3) OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"aaa"', '$.a' RETURNING char(2) OMIT QUOTES DEFAULT 'bb' ON EMPTY);
SELECT JSON_QUERY(jsonb '"aaa"', '$.a' RETURNING char(2) OMIT QUOTES DEFAULT '"bb"'::jsonb ON EMPTY);

-- OMIT QUOTES behavior should not be specified when WITH WRAPPER used:
-- Should fail
SELECT JSON_QUERY(jsonb '[1]', '$' WITH WRAPPER OMIT QUOTES);
SELECT JSON_QUERY(jsonb '[1]', '$' WITH CONDITIONAL WRAPPER OMIT QUOTES);
-- Should succeed
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITH CONDITIONAL WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITH UNCONDITIONAL WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITH WRAPPER KEEP QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITHOUT WRAPPER OMIT QUOTES);
SELECT JSON_QUERY(jsonb '["1"]', '$[*]' WITHOUT WRAPPER KEEP QUOTES);

-- test QUOTES behavior.
SELECT JSON_QUERY(jsonb'{"rec": "{1,2,3}"}', '$.rec' returning int[] omit quotes);
SELECT JSON_QUERY(jsonb'{"rec": "{1,2,3}"}', '$.rec' returning int[] keep quotes);
SELECT JSON_QUERY(jsonb'{"rec": "{1,2,3}"}', '$.rec' returning int[] keep quotes error on error);
SELECT JSON_QUERY(jsonb'{"rec": "[1,2]"}', '$.rec' returning int4range omit quotes);
SELECT JSON_QUERY(jsonb'{"rec": "[1,2]"}', '$.rec' returning int4range keep quotes);
SELECT JSON_QUERY(jsonb'{"rec": "[1,2]"}', '$.rec' 	returning int4range keep quotes error on error);
CREATE DOMAIN qf_char_domain AS char(1);
CREATE DOMAIN qf_jsonb_domain AS jsonb;
SELECT JSON_QUERY(jsonb '"1"', '$' RETURNING qf_char_domain OMIT QUOTES ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '"1"', '$' RETURNING qf_jsonb_domain OMIT QUOTES ERROR ON ERROR);
DROP DOMAIN qf_char_domain, qf_jsonb_domain;

SELECT JSON_QUERY(jsonb '[]', '$[*]');
SELECT JSON_QUERY(jsonb '[]', '$[*]' NULL ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' EMPTY ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' EMPTY ARRAY ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' EMPTY OBJECT ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY);
SELECT JSON_QUERY(jsonb '[]', '$[*]' DEFAULT '"empty"' ON EMPTY);

SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY NULL ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY EMPTY ARRAY ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON EMPTY ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[]', '$[*]' ERROR ON ERROR);	-- NULL ON EMPTY

SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' DEFAULT '"empty"' ON ERROR);

SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING json);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING json FORMAT JSON);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING jsonb);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING jsonb FORMAT JSON);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING text);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING char(10));
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING text FORMAT JSON);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING bytea);
SELECT JSON_QUERY(jsonb '[1,2]', '$' RETURNING bytea FORMAT JSON);

SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING bytea EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING bytea FORMAT JSON EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING json EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2]', '$[*]' RETURNING jsonb EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '[3,4]', '$[*]' RETURNING bigint[] EMPTY OBJECT ON ERROR);
SELECT JSON_QUERY(jsonb '"[3,4]"', '$[*]' RETURNING bigint[] EMPTY OBJECT ON ERROR);

-- Coercion fails with quotes on
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int2 error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int4 error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int8 error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING bool error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING numeric error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING real error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING float8 error on error);
-- Fine with OMIT QUOTES
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING int2 omit quotes error on error);
SELECT JSON_QUERY(jsonb '"123.1"', '$' RETURNING float8 omit quotes error on error);

-- RETUGNING pseudo-types not allowed
SELECT JSON_QUERY(jsonb '[3,4]', '$[*]' RETURNING anyarray EMPTY OBJECT ON ERROR);

SELECT
	x, y,
	JSON_QUERY(
		jsonb '[1,2,3,4,5,null]',
		'$[*] ? (@ >= $x && @ <= $y)'
		PASSING x AS x, y AS y
		WITH CONDITIONAL WRAPPER
		EMPTY ARRAY ON EMPTY
	) list
FROM
	generate_series(0, 4) x,
	generate_series(0, 4) y;

-- record type returning with quotes behavior.
CREATE TYPE comp_abc AS (a text, b int, c timestamp);
SELECT JSON_QUERY(jsonb'{"rec": "(abc,42,01.02.2003)"}', '$.rec' returning comp_abc omit quotes);
SELECT JSON_QUERY(jsonb'{"rec": "(abc,42,01.02.2003)"}', '$.rec' returning comp_abc keep quotes);
SELECT JSON_QUERY(jsonb'{"rec": "(abc,42,01.02.2003)"}', '$.rec' returning comp_abc keep quotes error on error);
DROP TYPE comp_abc;

-- Extension: record types returning
CREATE TYPE sqljsonb_rec AS (a int, t text, js json, jb jsonb, jsa json[]);
CREATE TYPE sqljsonb_reca AS (reca sqljsonb_rec[]);

SELECT JSON_QUERY(jsonb '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljsonb_rec);
SELECT JSON_QUERY(jsonb '[{"a": "a", "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljsonb_rec ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[{"a": "a", "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING sqljsonb_rec);
SELECT * FROM unnest((JSON_QUERY(jsonb '{"jsa":  [{"a": 1, "b": ["foo"]}, {"a": 2, "c": {}}, 123]}', '$' RETURNING sqljsonb_rec)).jsa);
SELECT * FROM unnest((JSON_QUERY(jsonb '{"reca": [{"a": 1, "t": ["foo", []]}, {"a": 2, "jb": [{}, true]}]}', '$' RETURNING sqljsonb_reca)).reca);

SELECT JSON_QUERY(jsonb '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING jsonpath);
SELECT JSON_QUERY(jsonb '[{"a": 1, "b": "foo", "t": "aaa", "js": [1, "2", {}], "jb": {"x": [1, "2", {}]}},  {"a": 2}]', '$[0]' RETURNING jsonpath ERROR ON ERROR);

-- Extension: array types returning
SELECT JSON_QUERY(jsonb '[1,2,null,"3"]', '$[*]' RETURNING int[] WITH WRAPPER);
SELECT JSON_QUERY(jsonb '[1,2,null,"a"]', '$[*]' RETURNING int[] WITH WRAPPER ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '[1,2,null,"a"]', '$[*]' RETURNING int[] WITH WRAPPER);
SELECT * FROM unnest(JSON_QUERY(jsonb '[{"a": 1, "t": ["foo", []]}, {"a": 2, "jb": [{}, true]}]', '$' RETURNING sqljsonb_rec[]));

-- Extension: domain types returning
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.a' RETURNING sqljsonb_int_not_null);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' RETURNING sqljsonb_int_not_null);
SELECT JSON_QUERY(jsonb '{"a": 1}', '$.b' RETURNING sqljsonb_int_not_null ERROR ON EMPTY ERROR ON ERROR);

-- Test timestamptz passing and output
SELECT JSON_QUERY(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts);
SELECT JSON_QUERY(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING json);
SELECT JSON_QUERY(jsonb 'null', '$ts' PASSING timestamptz '2018-02-21 12:34:56 +10' AS ts RETURNING jsonb);

-- Test constraints

CREATE TABLE test_jsonb_constraints (
	js text,
	i int,
	x jsonb DEFAULT JSON_QUERY(jsonb '[1,2]', '$[*]' WITH WRAPPER)
	CONSTRAINT test_jsonb_constraint1
		CHECK (js IS JSON)
	CONSTRAINT test_jsonb_constraint2
		CHECK (JSON_EXISTS(js::jsonb, '$.a' PASSING i + 5 AS int, i::text AS "TXT", array[1,2,3] as arr))
	CONSTRAINT test_jsonb_constraint3
		CHECK (JSON_VALUE(js::jsonb, '$.a' RETURNING int DEFAULT '12' ON EMPTY ERROR ON ERROR) > i)
	CONSTRAINT test_jsonb_constraint4
		CHECK (JSON_QUERY(js::jsonb, '$.a' WITH CONDITIONAL WRAPPER EMPTY OBJECT ON ERROR) = jsonb '[10]')
	CONSTRAINT test_jsonb_constraint5
		CHECK (JSON_QUERY(js::jsonb, '$.a' RETURNING char(5) OMIT QUOTES EMPTY ARRAY ON EMPTY) >  'a' COLLATE "C")
);

\d test_jsonb_constraints

SELECT check_clause
FROM information_schema.check_constraints
WHERE constraint_name LIKE 'test_jsonb_constraint%'
ORDER BY 1;

SELECT pg_get_expr(adbin, adrelid)
FROM pg_attrdef
WHERE adrelid = 'test_jsonb_constraints'::regclass
ORDER BY 1;

INSERT INTO test_jsonb_constraints VALUES ('', 1);
INSERT INTO test_jsonb_constraints VALUES ('1', 1);
INSERT INTO test_jsonb_constraints VALUES ('[]');
INSERT INTO test_jsonb_constraints VALUES ('{"b": 1}', 1);
INSERT INTO test_jsonb_constraints VALUES ('{"a": 1}', 1);
INSERT INTO test_jsonb_constraints VALUES ('{"a": 10}', 1);

DROP TABLE test_jsonb_constraints;

-- Test mutabilily of query functions
CREATE TABLE test_jsonb_mutability(js jsonb, b int);
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a[0]'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.time()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.date()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.time_tz()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.timestamp()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.timestamp_tz()'));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.date() < $.time_tz())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.date() < $.time())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.time() < $.time())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.time() < $.time_tz())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp() < $.timestamp_tz())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp_tz() < $.timestamp_tz())'));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.time() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.date() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp() < $.datetime("HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp_tz() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp_tz() < $.datetime("HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.date() < $x' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.date() < $x' PASSING '1234'::int AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.timestamp(2) < $.timestamp(3))'));

CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime()'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@ < $.datetime())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime() < $.datetime())'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime() < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime("HH:MI TZH") < $.datetime("HH:MI TZH"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime("HH:MI") < $.datetime("YY-MM-DD HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.a ? (@.datetime("HH:MI TZH") < $.datetime("YY-MM-DD HH:MI"))'));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime("HH:MI TZH") < $x' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime("HH:MI TZH") < $y' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime() < $x' PASSING '12:34'::timetz AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime() < $x' PASSING '1234'::int AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime() ? (@ == $x)' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$.datetime("YY-MM-DD") ? (@ == $x)' PASSING '2020-07-14'::date AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$[1, $.a ? (@.datetime() == $x)]' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$[1, 0 to $.a ? (@.datetime() == $x)]' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_QUERY(js, '$[1, $.a ? (@.datetime("HH:MI") == $x)]' PASSING '12:34'::time AS x));
CREATE INDEX ON test_jsonb_mutability (JSON_VALUE(js, '$' DEFAULT random()::int ON ERROR));

-- DEFAULT expression
CREATE OR REPLACE FUNCTION ret_setint() RETURNS SETOF integer AS
$$
BEGIN
    RETURN QUERY EXECUTE 'select 1 union all select 1';
END;
$$
LANGUAGE plpgsql IMMUTABLE;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT ret_setint() ON ERROR) FROM test_jsonb_mutability;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT b + 1 ON ERROR) FROM test_jsonb_mutability;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT sum(1) over() ON ERROR) FROM test_jsonb_mutability;
SELECT JSON_QUERY(js, '$'  RETURNING int DEFAULT (SELECT 1) ON ERROR) FROM test_jsonb_mutability;
DROP TABLE test_jsonb_mutability;
DROP FUNCTION ret_setint;

CREATE DOMAIN queryfuncs_test_domain AS text CHECK (value <> 'foo');
SELECT JSON_VALUE(jsonb '{"d1": "H"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT 'foo'::queryfuncs_test_domain ON EMPTY);
SELECT JSON_VALUE(jsonb '{"d1": "H"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT 'foo1'::queryfuncs_test_domain ON EMPTY);
SELECT JSON_VALUE(jsonb '{"d1": "H"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT '"foo1"'::jsonb::text ON EMPTY);
SELECT JSON_VALUE(jsonb '{"d1": "foo"}', '$.a2' RETURNING queryfuncs_test_domain DEFAULT 'foo1'::queryfuncs_test_domain ON EMPTY);

-- Check the cases where a coercion-related expression is masking an
-- unsupported expressions

-- CoerceViaIO
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int DEFAULT (SELECT '"1"')::jsonb ON ERROR);
-- CoerceToDomain
SELECT JSON_QUERY('"a"', '$.a' RETURNING queryfuncs_test_domain DEFAULT (select '"1"')::queryfuncs_test_domain ON ERROR);
-- RelabelType
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int DEFAULT (SELECT 1)::oid::int ON ERROR);
-- ArrayCoerceExpr
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int[] DEFAULT (SELECT '{1}')::oid[]::int[] ON ERROR);
-- CollateExpr
SELECT JSON_QUERY('"a"', '$.a'  RETURNING int[] DEFAULT (SELECT '{1}')::text COLLATE "C" ON ERROR);
-- ConvertRowtypeExpr
CREATE TABLE someparent (a int);
CREATE TABLE somechild () INHERITS (someparent);
SELECT JSON_QUERY('"a"', '$.a'  RETURNING someparent DEFAULT (SELECT '(1)')::somechild::someparent ON ERROR);

DROP DOMAIN queryfuncs_test_domain;
DROP TABLE someparent, somechild;

-- Extension: non-constant JSON path
SELECT JSON_EXISTS(jsonb '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_VALUE(jsonb '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_VALUE(jsonb '{"a": 123}', '$' || '.' || 'b' DEFAULT 'foo' ON EMPTY);
SELECT JSON_QUERY(jsonb '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_QUERY(jsonb '{"a": 123}', '$' || '.' || 'a' WITH WRAPPER);
SELECT JSON_QUERY(jsonb '{"a": 123}', ('$' || '.' || 'a' || NULL)::date WITH WRAPPER);
-- Should fail (invalid path)
SELECT JSON_QUERY(jsonb '{"a": 123}', 'error' || ' ' || 'error');

-- Non-jsonb inputs automatically coerced to jsonb
SELECT JSON_EXISTS(json '{"a": 123}', '$' || '.' || 'a');
SELECT JSON_QUERY(NULL FORMAT JSON, '$');

-- Test non-const jsonpath
CREATE TEMP TABLE jsonpaths (path) AS SELECT '$';
SELECT json_value('"aaa"', path RETURNING json) FROM jsonpaths;
SELECT json_value('"aaa"', jsonpaths RETURNING json) FROM jsonpaths;

-- Test PASSING argument parsing
SELECT JSON_QUERY(jsonb 'null', '$xyz' PASSING 1 AS xy);
SELECT JSON_QUERY(jsonb 'null', '$xy' PASSING 1 AS xyz);
SELECT JSON_QUERY(jsonb 'null', '$xyz' PASSING 1 AS xyz);
SELECT JSON_QUERY(jsonb 'null', '$Xyz' PASSING 1 AS Xyz);
SELECT JSON_QUERY(jsonb 'null', '$Xyz' PASSING 1 AS "Xyz");
SELECT JSON_QUERY(jsonb 'null', '$"Xyz"' PASSING 1 AS "Xyz");

-- Test ON ERROR / EMPTY value validity for the function; all fail.
SELECT JSON_EXISTS(jsonb '1', '$' DEFAULT 1 ON ERROR);
SELECT JSON_VALUE(jsonb '1', '$' EMPTY ON ERROR);
SELECT JSON_QUERY(jsonb '1', '$' TRUE ON ERROR);

-- Test implicit coercion to a domain over fixed-length type specified in
-- RETURNING
CREATE DOMAIN queryfuncs_char2 AS char(2);
CREATE DOMAIN queryfuncs_char2_chk AS char(2) CHECK (VALUE NOT IN ('12'));
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2 ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2 DEFAULT '1' ON ERROR);
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2_chk ERROR ON ERROR);
SELECT JSON_QUERY(jsonb '123', '$' RETURNING queryfuncs_char2_chk DEFAULT '1' ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2 ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2 DEFAULT 1 ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2_chk ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '123', '$' RETURNING queryfuncs_char2_chk DEFAULT 1 ON ERROR);
DROP DOMAIN queryfuncs_char2, queryfuncs_char2_chk;

-- Test coercion to domain over another fixed-length type of the ON ERROR /
-- EMPTY expressions.  Ask user to cast the DEFAULT expression explicitly if
-- automatic casting cannot be done, for example, from int to bit(2).
CREATE DOMAIN queryfuncs_d_varbit3 AS varbit(3) CHECK (VALUE <> '01');
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING queryfuncs_d_varbit3  DEFAULT '111111' ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING queryfuncs_d_varbit3  DEFAULT '010' ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING queryfuncs_d_varbit3  DEFAULT '01' ON ERROR);
SELECT JSON_VALUE(jsonb '"111"', '$'  RETURNING bit(2) ERROR ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING bit(3)  DEFAULT 1 ON ERROR);
SELECT JSON_VALUE(jsonb '1234', '$' RETURNING bit(3)  DEFAULT 1::bit(3) ON ERROR);
SELECT JSON_VALUE(jsonb '"111"', '$.a'  RETURNING bit(3) DEFAULT '1111' ON EMPTY);
DROP DOMAIN queryfuncs_d_varbit3;
