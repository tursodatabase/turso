use crate::storage::sqlite3_ondisk::DatabaseHeader;

use super::*;
use rand::Rng;
const DEFAULT_ENCRYPTED_PAGE_SIZE: usize = 4096;

macro_rules! test_cipher_wrapper {
    ($test_name:ident, $cipher_type:ty, $key_gen:expr, $nonce_size:literal, $message:literal) => {
        #[test]
        fn $test_name() {
            let key = EncryptionKey::from_hex_string(&$key_gen()).unwrap();
            let cipher = <$cipher_type>::new(&key);

            let plaintext = $message.as_bytes();
            let ad = b"additional data";

            let (ciphertext, nonce) = cipher.encrypt(plaintext, ad).unwrap();
            assert_eq!(nonce.len(), $nonce_size);
            assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

            let decrypted = cipher.decrypt(&ciphertext, &nonce, ad).unwrap();
            assert_eq!(decrypted, plaintext);
        }
    };
}

macro_rules! test_aes_cipher_wrapper {
    ($test_name:ident, $cipher_type:ty, $key_gen:expr, $nonce_size:literal, $message:literal) => {
        #[test]
        fn $test_name() {
            let key = EncryptionKey::from_hex_string(&$key_gen()).unwrap();
            let cipher = <$cipher_type>::new(&key).unwrap();

            let plaintext = $message.as_bytes();
            let ad = b"additional data";

            let (ciphertext, nonce) = cipher.encrypt(plaintext, ad).unwrap();
            assert_eq!(nonce.len(), $nonce_size);
            assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

            let decrypted = cipher.decrypt(&ciphertext, &nonce, ad).unwrap();
            assert_eq!(decrypted, plaintext);
        }
    };
}

macro_rules! test_raw_encryption {
    ($test_name:ident, $cipher_mode:expr, $key_gen:expr, $nonce_size:literal, $message:literal) => {
        #[test]
        fn $test_name() {
            let key = EncryptionKey::from_hex_string(&$key_gen()).unwrap();
            let ctx =
                EncryptionContext::new($cipher_mode, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

            let plaintext = $message.as_bytes();
            let (ciphertext, nonce) = ctx.encrypt_raw(plaintext).unwrap();

            assert_eq!(nonce.len(), $nonce_size);
            assert_ne!(ciphertext[..plaintext.len()], plaintext[..]);

            let decrypted = ctx.decrypt_raw(&ciphertext, &nonce).unwrap();
            assert_eq!(decrypted, plaintext);

            let mut in_place_ciphertext = plaintext.to_vec();
            let mut tag = vec![0; ctx.tag_size()];
            let mut nonce = vec![0; ctx.nonce_size()];
            ctx.encrypt_chunk_in_place(&mut in_place_ciphertext, b"", &mut tag, &mut nonce)
                .unwrap();
            in_place_ciphertext.extend_from_slice(&tag);
            let decrypted = ctx.decrypt_raw(&in_place_ciphertext, &nonce).unwrap();
            assert_eq!(decrypted, plaintext);
        }
    };
}

fn generate_random_hex_key() -> String {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    hex::encode(bytes)
}

fn generate_random_hex_key_128() -> String {
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    hex::encode(bytes)
}

fn create_test_page_1() -> Vec<u8> {
    let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
    page[..SQLITE_HEADER.len()].copy_from_slice(SQLITE_HEADER);
    let mut rng = rand::rng();
    // 48 is the max reserved bytes we might need for metadata with any cipher
    rng.fill(&mut page[SQLITE_HEADER.len()..DEFAULT_ENCRYPTED_PAGE_SIZE - 48]);
    page
}

test_aes_cipher_wrapper!(
    test_aes128gcm_cipher_wrapper,
    Aes128GcmCipher,
    generate_random_hex_key_128,
    12,
    "Hello, AES-128-GCM!"
);

test_raw_encryption!(
    test_aes128gcm_raw_encryption,
    CipherMode::Aes128Gcm,
    generate_random_hex_key_128,
    12,
    "Hello, AES-128-GCM!"
);

#[test]
fn test_page_1_encrypt_decrypt_round_trip_with_ad() {
    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_data = create_test_page_1();
    let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();
    assert_ne!(
        &page_data[0..DatabaseHeader::SIZE],
        &encrypted[0..DatabaseHeader::SIZE],
        "Encrypted data should be different from the page data"
    );
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);

    // check that header is readable directly from disk (not encrypted)
    assert_eq!(&encrypted[..5], b"Turso");
    assert_eq!(encrypted[5], TURSO_VERSION);
    assert_eq!(encrypted[6], CipherMode::Aegis256.cipher_id());

    // header should be unencrypted, but data after DatabaseHeader::SIZE should be different
    assert_eq!(&encrypted[16..100], &page_data[16..100]); // header portion
    assert_ne!(&encrypted[100..200], &page_data[100..200]); // some encrypted portion

    // decrypt page 1
    let decrypted = ctx.decrypt_page(&encrypted, 1).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);

    // check that SQLite header was restored
    assert_eq!(&decrypted[..SQLITE_HEADER.len()], SQLITE_HEADER);
    assert_eq!(decrypted, page_data);
}

#[test]
fn test_turso_header_validation() {
    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    // test cipher_id conversion
    assert_eq!(CipherMode::Aes128Gcm.cipher_id(), 1);
    assert_eq!(CipherMode::Aes256Gcm.cipher_id(), 2);
    assert_eq!(CipherMode::Aegis256.cipher_id(), 3);
    assert_eq!(CipherMode::Aegis128L.cipher_id(), 6);

    // test from_cipher_id conversion
    assert_eq!(
        CipherMode::from_cipher_id(1).unwrap(),
        CipherMode::Aes128Gcm
    );
    assert_eq!(CipherMode::from_cipher_id(3).unwrap(), CipherMode::Aegis256);
    assert!(CipherMode::from_cipher_id(99).is_err());

    // test header creation
    let header = ctx.create_turso_header();
    assert_eq!(&header[..5], b"Turso");
    assert_eq!(header[5], TURSO_VERSION);
    assert_eq!(header[6], 3); // AEGIS-256
    assert_eq!(&header[7..], &[0u8; 9]); // unused bytes are zero
}

#[test]
fn test_invalid_turso_header_fails_decrypt() {
    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_data = create_test_page_1();
    let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();

    // corrupt the header prefix
    let mut corrupted = encrypted.clone();
    corrupted[0] = b'V'; // make `Turso` to `Vurso`
    assert!(ctx.decrypt_page(&corrupted, 1).is_err());

    // test with wrong cipher ID
    let mut wrong_cipher = encrypted;
    wrong_cipher[6] = 99; // invalid cipher ID
    assert!(ctx.decrypt_page(&wrong_cipher, 1).is_err());
}

#[test]
fn test_associated_data_validation() {
    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_data = create_test_page_1();
    let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();

    // modify a byte in the preserved header portion (bytes 16-100)
    let mut corrupted_ad = encrypted;
    corrupted_ad[50] ^= 1; // flip one bit in the associated data portion

    // this should fail decryption because associated data doesn't match
    let decrypt_result = ctx.decrypt_page(&corrupted_ad, 1);
    assert!(
        decrypt_result.is_err(),
        "Decryption should fail with corrupted associated data"
    );
}

#[test]
fn test_turso_header_corruption_detection() {
    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_data = create_test_page_1();
    let encrypted = ctx.encrypt_page(&page_data, 1).unwrap();

    let mut corrupted_turso_header = encrypted;
    corrupted_turso_header[7] ^= 1;

    let decrypt_result = ctx.decrypt_page(&corrupted_turso_header, 1);
    assert!(
        decrypt_result.is_err(),
        "Decryption should fail with corrupted Turso header"
    );
}

#[test]
fn test_aes128gcm_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aes128Gcm;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aes128Gcm, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);
    assert_ne!(&encrypted[..], &page_data[..]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

#[test]
fn test_aes_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aes256Gcm;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aes256Gcm, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);
    assert_ne!(&encrypted[..], &page_data[..]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

test_cipher_wrapper!(
    test_aegis256_cipher_wrapper,
    Aegis256Cipher,
    generate_random_hex_key,
    32,
    "Hello, AEGIS-256!"
);

test_raw_encryption!(
    test_aegis256_raw_encryption,
    CipherMode::Aegis256,
    generate_random_hex_key,
    32,
    "Hello, AEGIS-256!"
);

#[test]
fn test_aegis256_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aegis256;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

test_cipher_wrapper!(
    test_aegis128x2_cipher_wrapper,
    Aegis128X2Cipher,
    generate_random_hex_key_128,
    16,
    "Hello, AEGIS-128X2!"
);

test_raw_encryption!(
    test_aegis128x2_raw_encryption,
    CipherMode::Aegis128X2,
    generate_random_hex_key_128,
    16,
    "Hello, AEGIS-128X2!"
);

#[test]
fn test_aegis128x2_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aegis128X2;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis128X2, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

test_cipher_wrapper!(
    test_aegis128l_cipher_wrapper,
    Aegis128LCipher,
    generate_random_hex_key_128,
    16,
    "Hello, AEGIS-128L!"
);

test_raw_encryption!(
    test_aegis128l_raw_encryption,
    CipherMode::Aegis128L,
    generate_random_hex_key_128,
    16,
    "Hello, AEGIS-128L!"
);

#[test]
fn test_aegis128l_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aegis128L;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis128L, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

test_cipher_wrapper!(
    test_aegis128x4_cipher_wrapper,
    Aegis128X4Cipher,
    generate_random_hex_key_128,
    16,
    "Hello, AEGIS-128X4!"
);

test_raw_encryption!(
    test_aegis128x4_raw_encryption,
    CipherMode::Aegis128X4,
    generate_random_hex_key_128,
    16,
    "Hello, AEGIS-128X4!"
);

#[test]
fn test_aegis128x4_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aegis128X4;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key_128()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis128X4, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

test_cipher_wrapper!(
    test_aegis256x2_cipher_wrapper,
    Aegis256X2Cipher,
    generate_random_hex_key,
    32,
    "Hello, AEGIS-256X2!"
);

test_raw_encryption!(
    test_aegis256x2_raw_encryption,
    CipherMode::Aegis256X2,
    generate_random_hex_key,
    32,
    "Hello, AEGIS-256X2!"
);

#[test]
fn test_aegis256x2_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aegis256X2;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256X2, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

test_cipher_wrapper!(
    test_aegis256x4_cipher_wrapper,
    Aegis256X4Cipher,
    generate_random_hex_key,
    32,
    "Hello, AEGIS-256X4!"
);

test_raw_encryption!(
    test_aegis256x4_raw_encryption,
    CipherMode::Aegis256X4,
    generate_random_hex_key,
    32,
    "Hello, AEGIS-256X4!"
);

#[test]
fn test_aegis256x4_encrypt_decrypt_round_trip() {
    let mut rng = rand::rng();
    let cipher_mode = CipherMode::Aegis256X4;
    let metadata_size = cipher_mode.metadata_size();
    let data_size = DEFAULT_ENCRYPTED_PAGE_SIZE - metadata_size;

    let page_data = {
        let mut page = vec![0u8; DEFAULT_ENCRYPTED_PAGE_SIZE];
        page.iter_mut()
            .take(data_size)
            .for_each(|byte| *byte = rng.random());
        page
    };

    let key = EncryptionKey::from_hex_string(&generate_random_hex_key()).unwrap();
    let ctx =
        EncryptionContext::new(CipherMode::Aegis256X4, &key, DEFAULT_ENCRYPTED_PAGE_SIZE).unwrap();

    let page_id = 42;
    let encrypted = ctx.encrypt_page(&page_data, page_id).unwrap();
    assert_eq!(encrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_ne!(&encrypted[..data_size], &page_data[..data_size]);

    let decrypted = ctx.decrypt_page(&encrypted, page_id).unwrap();
    assert_eq!(decrypted.len(), DEFAULT_ENCRYPTED_PAGE_SIZE);
    assert_eq!(decrypted, page_data);
}

#[test]
fn test_cipher_mode_string_parsing() {
    // Test AES-128-GCM
    let mode = CipherMode::try_from("aes128gcm").unwrap();
    assert_eq!(mode, CipherMode::Aes128Gcm);
    assert_eq!(mode.to_string(), "aes128gcm");
    assert_eq!(mode.required_key_size(), 16);
    assert_eq!(mode.nonce_size(), 12);
    assert_eq!(mode.tag_size(), 16);

    let mode = CipherMode::try_from("aes-128-gcm").unwrap();
    assert_eq!(mode, CipherMode::Aes128Gcm);

    let mode = CipherMode::try_from("aes_128_gcm").unwrap();
    assert_eq!(mode, CipherMode::Aes128Gcm);

    // Test AES-256-GCM
    let mode = CipherMode::try_from("aes256gcm").unwrap();
    assert_eq!(mode, CipherMode::Aes256Gcm);
    assert_eq!(mode.to_string(), "aes256gcm");
    assert_eq!(mode.required_key_size(), 32);
    assert_eq!(mode.nonce_size(), 12);

    // Test that all AEGIS variants can be parsed from strings
    let mode = CipherMode::try_from("aegis128x2").unwrap();
    assert_eq!(mode, CipherMode::Aegis128X2);
    assert_eq!(mode.to_string(), "aegis128x2");
    assert_eq!(mode.required_key_size(), 16);
    assert_eq!(mode.nonce_size(), 16);
    assert_eq!(mode.tag_size(), 16);

    let mode = CipherMode::try_from("aegis-128x2").unwrap();
    assert_eq!(mode, CipherMode::Aegis128X2);

    let mode = CipherMode::try_from("aegis_128x2").unwrap();
    assert_eq!(mode, CipherMode::Aegis128X2);

    // Test AEGIS-128L
    let mode = CipherMode::try_from("aegis128l").unwrap();
    assert_eq!(mode, CipherMode::Aegis128L);
    assert_eq!(mode.to_string(), "aegis128l");
    assert_eq!(mode.required_key_size(), 16);
    assert_eq!(mode.nonce_size(), 16);

    // Test AEGIS-128X4
    let mode = CipherMode::try_from("aegis128x4").unwrap();
    assert_eq!(mode, CipherMode::Aegis128X4);
    assert_eq!(mode.to_string(), "aegis128x4");
    assert_eq!(mode.required_key_size(), 16);
    assert_eq!(mode.nonce_size(), 16);

    // Test AEGIS-256X2
    let mode = CipherMode::try_from("aegis256x2").unwrap();
    assert_eq!(mode, CipherMode::Aegis256X2);
    assert_eq!(mode.to_string(), "aegis256x2");
    assert_eq!(mode.required_key_size(), 32);
    assert_eq!(mode.nonce_size(), 32);

    // Test AEGIS-256X4
    let mode = CipherMode::try_from("aegis256x4").unwrap();
    assert_eq!(mode, CipherMode::Aegis256X4);
    assert_eq!(mode.to_string(), "aegis256x4");
    assert_eq!(mode.required_key_size(), 32);
    assert_eq!(mode.nonce_size(), 32);
}
