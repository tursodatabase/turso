--jsonpath io

select ''::jsonpath;
select '$'::jsonpath;
select 'strict $'::jsonpath;
select 'lax $'::jsonpath;
select '$.a'::jsonpath;
select '$.a.v'::jsonpath;
select '$.a.*'::jsonpath;
select '$.*[*]'::jsonpath;
select '$.a[*]'::jsonpath;
select '$.a[*][*]'::jsonpath;
select '$[*]'::jsonpath;
select '$[0]'::jsonpath;
select '$[*][0]'::jsonpath;
select '$[*].a'::jsonpath;
select '$[*][0].a.b'::jsonpath;
select '$.a.**.b'::jsonpath;
select '$.a.**{2}.b'::jsonpath;
select '$.a.**{2 to 2}.b'::jsonpath;
select '$.a.**{2 to 5}.b'::jsonpath;
select '$.a.**{0 to 5}.b'::jsonpath;
select '$.a.**{5 to last}.b'::jsonpath;
select '$.a.**{last}.b'::jsonpath;
select '$.a.**{last to 5}.b'::jsonpath;
select '$+1'::jsonpath;
select '$-1'::jsonpath;
select '$--+1'::jsonpath;
select '$.a/+-1'::jsonpath;
select '1 * 2 + 4 % -3 != false'::jsonpath;

select '"\b\f\r\n\t\v\"\''\\"'::jsonpath;
select '"\x50\u0067\u{53}\u{051}\u{00004C}"'::jsonpath;
select '$.foo\x50\u0067\u{53}\u{051}\u{00004C}\t\"bar'::jsonpath;
select '"\z"'::jsonpath;  -- unrecognized escape is just the literal char

select '$.g ? ($.a == 1)'::jsonpath;
select '$.g ? (@ == 1)'::jsonpath;
select '$.g ? (@.a == 1)'::jsonpath;
select '$.g ? (@.a == 1 || @.a == 4)'::jsonpath;
select '$.g ? (@.a == 1 && @.a == 4)'::jsonpath;
select '$.g ? (@.a == 1 || @.a == 4 && @.b == 7)'::jsonpath;
select '$.g ? (@.a == 1 || !(@.a == 4) && @.b == 7)'::jsonpath;
select '$.g ? (@.a == 1 || !(@.x >= 123 || @.a == 4) && @.b == 7)'::jsonpath;
select '$.g ? (@.x >= @[*]?(@.a > "abc"))'::jsonpath;
select '$.g ? ((@.x >= 123 || @.a == 4) is unknown)'::jsonpath;
select '$.g ? (exists (@.x))'::jsonpath;
select '$.g ? (exists (@.x ? (@ == 14)))'::jsonpath;
select '$.g ? ((@.x >= 123 || @.a == 4) && exists (@.x ? (@ == 14)))'::jsonpath;
select '$.g ? (+@.x >= +-(+@.a + 2))'::jsonpath;

select '$a'::jsonpath;
select '$a.b'::jsonpath;
select '$a[*]'::jsonpath;
select '$.g ? (@.zip == $zip)'::jsonpath;
select '$.a[1,2, 3 to 16]'::jsonpath;
select '$.a[$a + 1, ($b[*]) to -($[0] * 2)]'::jsonpath;
select '$.a[$.a.size() - 3]'::jsonpath;
select 'last'::jsonpath;
select '"last"'::jsonpath;
select '$.last'::jsonpath;
select '$ ? (last > 0)'::jsonpath;
select '$[last]'::jsonpath;
select '$[$[0] ? (last > 0)]'::jsonpath;

select 'null.type()'::jsonpath;
select '1.type()'::jsonpath;
select '(1).type()'::jsonpath;
select '1.2.type()'::jsonpath;
select '"aaa".type()'::jsonpath;
select 'true.type()'::jsonpath;
select '$.double().floor().ceiling().abs()'::jsonpath;
select '$.keyvalue().key'::jsonpath;
select '$.datetime()'::jsonpath;
select '$.datetime("datetime template")'::jsonpath;
select '$.bigint().integer().number().decimal()'::jsonpath;
select '$.boolean()'::jsonpath;
select '$.date()'::jsonpath;
select '$.decimal(4,2)'::jsonpath;
select '$.string()'::jsonpath;
select '$.time()'::jsonpath;
select '$.time(6)'::jsonpath;
select '$.time_tz()'::jsonpath;
select '$.time_tz(4)'::jsonpath;
select '$.timestamp()'::jsonpath;
select '$.timestamp(2)'::jsonpath;
select '$.timestamp_tz()'::jsonpath;
select '$.timestamp_tz(0)'::jsonpath;

select '$ ? (@ starts with "abc")'::jsonpath;
select '$ ? (@ starts with $var)'::jsonpath;

select '$ ? (@ like_regex "(invalid pattern")'::jsonpath;
select '$ ? (@ like_regex "pattern")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "i")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "is")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "isim")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "xsms")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "q")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "iq")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "smixq")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "a")'::jsonpath;

select '$ < 1'::jsonpath;
select '($ < 1) || $.a.b <= $x'::jsonpath;
select '@ + 1'::jsonpath;

select '($).a.b'::jsonpath;
select '($.a.b).c.d'::jsonpath;
select '($.a.b + -$.x.y).c.d'::jsonpath;
select '(-+$.a.b).c.d'::jsonpath;
select '1 + ($.a.b + 2).c.d'::jsonpath;
select '1 + ($.a.b > 2).c.d'::jsonpath;
select '($)'::jsonpath;
select '(($))'::jsonpath;
select '((($ + 1)).a + ((2)).b ? ((((@ > 1)) || (exists(@.c)))))'::jsonpath;

select '$ ? (@.a < 1)'::jsonpath;
select '$ ? (@.a < -1)'::jsonpath;
select '$ ? (@.a < +1)'::jsonpath;
select '$ ? (@.a < .1)'::jsonpath;
select '$ ? (@.a < -.1)'::jsonpath;
select '$ ? (@.a < +.1)'::jsonpath;
select '$ ? (@.a < 0.1)'::jsonpath;
select '$ ? (@.a < -0.1)'::jsonpath;
select '$ ? (@.a < +0.1)'::jsonpath;
select '$ ? (@.a < 10.1)'::jsonpath;
select '$ ? (@.a < -10.1)'::jsonpath;
select '$ ? (@.a < +10.1)'::jsonpath;
select '$ ? (@.a < 1e1)'::jsonpath;
select '$ ? (@.a < -1e1)'::jsonpath;
select '$ ? (@.a < +1e1)'::jsonpath;
select '$ ? (@.a < .1e1)'::jsonpath;
select '$ ? (@.a < -.1e1)'::jsonpath;
select '$ ? (@.a < +.1e1)'::jsonpath;
select '$ ? (@.a < 0.1e1)'::jsonpath;
select '$ ? (@.a < -0.1e1)'::jsonpath;
select '$ ? (@.a < +0.1e1)'::jsonpath;
select '$ ? (@.a < 10.1e1)'::jsonpath;
select '$ ? (@.a < -10.1e1)'::jsonpath;
select '$ ? (@.a < +10.1e1)'::jsonpath;
select '$ ? (@.a < 1e-1)'::jsonpath;
select '$ ? (@.a < -1e-1)'::jsonpath;
select '$ ? (@.a < +1e-1)'::jsonpath;
select '$ ? (@.a < .1e-1)'::jsonpath;
select '$ ? (@.a < -.1e-1)'::jsonpath;
select '$ ? (@.a < +.1e-1)'::jsonpath;
select '$ ? (@.a < 0.1e-1)'::jsonpath;
select '$ ? (@.a < -0.1e-1)'::jsonpath;
select '$ ? (@.a < +0.1e-1)'::jsonpath;
select '$ ? (@.a < 10.1e-1)'::jsonpath;
select '$ ? (@.a < -10.1e-1)'::jsonpath;
select '$ ? (@.a < +10.1e-1)'::jsonpath;
select '$ ? (@.a < 1e+1)'::jsonpath;
select '$ ? (@.a < -1e+1)'::jsonpath;
select '$ ? (@.a < +1e+1)'::jsonpath;
select '$ ? (@.a < .1e+1)'::jsonpath;
select '$ ? (@.a < -.1e+1)'::jsonpath;
select '$ ? (@.a < +.1e+1)'::jsonpath;
select '$ ? (@.a < 0.1e+1)'::jsonpath;
select '$ ? (@.a < -0.1e+1)'::jsonpath;
select '$ ? (@.a < +0.1e+1)'::jsonpath;
select '$ ? (@.a < 10.1e+1)'::jsonpath;
select '$ ? (@.a < -10.1e+1)'::jsonpath;
select '$ ? (@.a < +10.1e+1)'::jsonpath;

-- numeric literals

select '0'::jsonpath;
select '00'::jsonpath;
select '0755'::jsonpath;
select '0.0'::jsonpath;
select '0.000'::jsonpath;
select '0.000e1'::jsonpath;
select '0.000e2'::jsonpath;
select '0.000e3'::jsonpath;
select '0.0010'::jsonpath;
select '0.0010e-1'::jsonpath;
select '0.0010e+1'::jsonpath;
select '0.0010e+2'::jsonpath;
select '.001'::jsonpath;
select '.001e1'::jsonpath;
select '1.'::jsonpath;
select '1.e1'::jsonpath;
select '1a'::jsonpath;
select '1e'::jsonpath;
select '1.e'::jsonpath;
select '1.2a'::jsonpath;
select '1.2e'::jsonpath;
select '1.2.e'::jsonpath;
select '(1.2).e'::jsonpath;
select '1e3'::jsonpath;
select '1.e3'::jsonpath;
select '1.e3.e'::jsonpath;
select '1.e3.e4'::jsonpath;
select '1.2e3'::jsonpath;
select '1.2e3a'::jsonpath;
select '1.2.e3'::jsonpath;
select '(1.2).e3'::jsonpath;
select '1..e'::jsonpath;
select '1..e3'::jsonpath;
select '(1.).e'::jsonpath;
select '(1.).e3'::jsonpath;
select '1?(2>3)'::jsonpath;

-- nondecimal
select '0b100101'::jsonpath;
select '0o273'::jsonpath;
select '0x42F'::jsonpath;

-- error cases
select '0b'::jsonpath;
select '1b'::jsonpath;
select '0b0x'::jsonpath;

select '0o'::jsonpath;
select '1o'::jsonpath;
select '0o0x'::jsonpath;

select '0x'::jsonpath;
select '1x'::jsonpath;
select '0x0y'::jsonpath;

-- underscores
select '1_000_000'::jsonpath;
select '1_2_3'::jsonpath;
select '0x1EEE_FFFF'::jsonpath;
select '0o2_73'::jsonpath;
select '0b10_0101'::jsonpath;

select '1_000.000_005'::jsonpath;
select '1_000.'::jsonpath;
select '.000_005'::jsonpath;
select '1_000.5e0_1'::jsonpath;

-- error cases
select '_100'::jsonpath;
select '100_'::jsonpath;
select '100__000'::jsonpath;

select '_1_000.5'::jsonpath;
select '1_000_.5'::jsonpath;
select '1_000._5'::jsonpath;
select '1_000.5_'::jsonpath;
select '1_000.5e_1'::jsonpath;

-- underscore after prefix not allowed in JavaScript (but allowed in SQL)
select '0b_10_0101'::jsonpath;
select '0o_273'::jsonpath;
select '0x_42F'::jsonpath;


-- test non-error-throwing API

SELECT str as jsonpath,
       pg_input_is_valid(str,'jsonpath') as ok,
       errinfo.sql_error_code,
       errinfo.message,
       errinfo.detail,
       errinfo.hint
FROM unnest(ARRAY['$ ? (@ like_regex "pattern" flag "smixq")'::text,
                  '$ ? (@ like_regex "pattern" flag "a")',
                  '@ + 1',
                  '00',
                  '1a']) str,
     LATERAL pg_input_error_info(str, 'jsonpath') as errinfo;
