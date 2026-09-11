--
-- encoding-sensitive tests for json and jsonb
--

-- We provide expected-results files for UTF8 (json_encoding.out)
-- and for SQL_ASCII (json_encoding_1.out).  Skip otherwise.
SELECT getdatabaseencoding() NOT IN ('UTF8', 'SQL_ASCII')
       AS skip_test \gset
\if :skip_test
\quit
\endif

SELECT getdatabaseencoding();           -- just to label the results files

-- first json

-- basic unicode input
SELECT '"\u"'::json;			-- ERROR, incomplete escape
SELECT '"\u00"'::json;			-- ERROR, incomplete escape
SELECT '"\u000g"'::json;		-- ERROR, g is not a hex digit
SELECT '"\u0000"'::json;		-- OK, legal escape
SELECT '"\uaBcD"'::json;		-- OK, uppercase and lower case both OK

-- handling of unicode surrogate pairs

select json '{ "a":  "\ud83d\ude04\ud83d\udc36" }' -> 'a' as correct_in_utf8;
select json '{ "a":  "\ud83d\ud83d" }' -> 'a'; -- 2 high surrogates in a row
select json '{ "a":  "\ude04\ud83d" }' -> 'a'; -- surrogates in wrong order
select json '{ "a":  "\ud83dX" }' -> 'a'; -- orphan high surrogate
select json '{ "a":  "\ude04X" }' -> 'a'; -- orphan low surrogate

--handling of simple unicode escapes

select json '{ "a":  "the Copyright \u00a9 sign" }' as correct_in_utf8;
select json '{ "a":  "dollar \u0024 character" }' as correct_everywhere;
select json '{ "a":  "dollar \\u0024 character" }' as not_an_escape;
select json '{ "a":  "null \u0000 escape" }' as not_unescaped;
select json '{ "a":  "null \\u0000 escape" }' as not_an_escape;

select json '{ "a":  "the Copyright \u00a9 sign" }' ->> 'a' as correct_in_utf8;
select json '{ "a":  "dollar \u0024 character" }' ->> 'a' as correct_everywhere;
select json '{ "a":  "dollar \\u0024 character" }' ->> 'a' as not_an_escape;
select json '{ "a":  "null \u0000 escape" }' ->> 'a' as fails;
select json '{ "a":  "null \\u0000 escape" }' ->> 'a' as not_an_escape;

-- then jsonb

-- basic unicode input
SELECT '"\u"'::jsonb;			-- ERROR, incomplete escape
SELECT '"\u00"'::jsonb;			-- ERROR, incomplete escape
SELECT '"\u000g"'::jsonb;		-- ERROR, g is not a hex digit
SELECT '"\u0045"'::jsonb;		-- OK, legal escape
SELECT '"\u0000"'::jsonb;		-- ERROR, we don't support U+0000
-- use octet_length here so we don't get an odd unicode char in the
-- output
SELECT octet_length('"\uaBcD"'::jsonb::text); -- OK, uppercase and lower case both OK

-- handling of unicode surrogate pairs

SELECT octet_length((jsonb '{ "a":  "\ud83d\ude04\ud83d\udc36" }' -> 'a')::text) AS correct_in_utf8;
SELECT jsonb '{ "a":  "\ud83d\ud83d" }' -> 'a'; -- 2 high surrogates in a row
SELECT jsonb '{ "a":  "\ude04\ud83d" }' -> 'a'; -- surrogates in wrong order
SELECT jsonb '{ "a":  "\ud83dX" }' -> 'a'; -- orphan high surrogate
SELECT jsonb '{ "a":  "\ude04X" }' -> 'a'; -- orphan low surrogate

-- handling of simple unicode escapes

SELECT jsonb '{ "a":  "the Copyright \u00a9 sign" }' as correct_in_utf8;
SELECT jsonb '{ "a":  "dollar \u0024 character" }' as correct_everywhere;
SELECT jsonb '{ "a":  "dollar \\u0024 character" }' as not_an_escape;
SELECT jsonb '{ "a":  "null \u0000 escape" }' as fails;
SELECT jsonb '{ "a":  "null \\u0000 escape" }' as not_an_escape;

SELECT jsonb '{ "a":  "the Copyright \u00a9 sign" }' ->> 'a' as correct_in_utf8;
SELECT jsonb '{ "a":  "dollar \u0024 character" }' ->> 'a' as correct_everywhere;
SELECT jsonb '{ "a":  "dollar \\u0024 character" }' ->> 'a' as not_an_escape;
SELECT jsonb '{ "a":  "null \u0000 escape" }' ->> 'a' as fails;
SELECT jsonb '{ "a":  "null \\u0000 escape" }' ->> 'a' as not_an_escape;

-- soft error for input-time failure

select * from pg_input_error_info('{ "a":  "\ud83d\ude04\ud83d\udc36" }', 'jsonb');
