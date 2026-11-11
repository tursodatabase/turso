These certificates and the private key are public test fixtures, generated with
OpenSSL using ECDSA P-256 and SHA-256. They are valid from 2020-01-01 to 2120-01-01.

`ca.pem` signs `localhost.pem`, whose only subject alternative name is
`DNS:localhost`. `untrusted-ca.pem` is an unrelated CA. `localhost.key` is the
unencrypted PKCS#8 key for the localhost server. Do not use these keys outside tests.
