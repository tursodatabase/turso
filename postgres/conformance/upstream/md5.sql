--
-- MD5 test suite - from IETF RFC 1321
-- (see: https://www.rfc-editor.org/rfc/rfc1321)
--

-- (The md5() function will error in OpenSSL FIPS mode.  By keeping
-- this test in a separate file, it is easier to manage variant
-- results.)

select md5('') = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a') = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc') = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest') = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz') = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890') = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";

select md5(''::bytea) = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a'::bytea) = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc'::bytea) = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest'::bytea) = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz'::bytea) = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'::bytea) = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890'::bytea) = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";
