--
-- encoding-sensitive tests for jsonpath
--

-- We provide expected-results files for UTF8 (jsonpath_encoding.out)
-- and for SQL_ASCII (jsonpath_encoding_1.out).  Skip otherwise.
SELECT getdatabaseencoding() NOT IN ('UTF8', 'SQL_ASCII')
       AS skip_test \gset
\if :skip_test
\quit
\endif

SELECT getdatabaseencoding();           -- just to label the results files

-- checks for double-quoted values

-- basic unicode input
SELECT '"\u"'::jsonpath;		-- ERROR, incomplete escape
SELECT '"\u00"'::jsonpath;		-- ERROR, incomplete escape
SELECT '"\u000g"'::jsonpath;	-- ERROR, g is not a hex digit
SELECT '"\u0000"'::jsonpath;	-- OK, legal escape
SELECT '"\uaBcD"'::jsonpath;	-- OK, uppercase and lower case both OK

-- handling of unicode surrogate pairs
select '"\ud83d\ude04\ud83d\udc36"'::jsonpath as correct_in_utf8;
select '"\ud83d\ud83d"'::jsonpath; -- 2 high surrogates in a row
select '"\ude04\ud83d"'::jsonpath; -- surrogates in wrong order
select '"\ud83dX"'::jsonpath; -- orphan high surrogate
select '"\ude04X"'::jsonpath; -- orphan low surrogate

--handling of simple unicode escapes
select '"the Copyright \u00a9 sign"'::jsonpath as correct_in_utf8;
select '"dollar \u0024 character"'::jsonpath as correct_everywhere;
select '"dollar \\u0024 character"'::jsonpath as not_an_escape;
select '"null \u0000 escape"'::jsonpath as not_unescaped;
select '"null \\u0000 escape"'::jsonpath as not_an_escape;

-- checks for quoted key names

-- basic unicode input
SELECT '$."\u"'::jsonpath;		-- ERROR, incomplete escape
SELECT '$."\u00"'::jsonpath;	-- ERROR, incomplete escape
SELECT '$."\u000g"'::jsonpath;	-- ERROR, g is not a hex digit
SELECT '$."\u0000"'::jsonpath;	-- OK, legal escape
SELECT '$."\uaBcD"'::jsonpath;	-- OK, uppercase and lower case both OK

-- handling of unicode surrogate pairs
select '$."\ud83d\ude04\ud83d\udc36"'::jsonpath as correct_in_utf8;
select '$."\ud83d\ud83d"'::jsonpath; -- 2 high surrogates in a row
select '$."\ude04\ud83d"'::jsonpath; -- surrogates in wrong order
select '$."\ud83dX"'::jsonpath; -- orphan high surrogate
select '$."\ude04X"'::jsonpath; -- orphan low surrogate

--handling of simple unicode escapes
select '$."the Copyright \u00a9 sign"'::jsonpath as correct_in_utf8;
select '$."dollar \u0024 character"'::jsonpath as correct_everywhere;
select '$."dollar \\u0024 character"'::jsonpath as not_an_escape;
select '$."null \u0000 escape"'::jsonpath as not_unescaped;
select '$."null \\u0000 escape"'::jsonpath as not_an_escape;
