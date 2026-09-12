use crate::Value;

use super::*;
use rstest::rstest;

#[rstest]
#[case(PageType::TableLeaf, 4096, 0, None)]
#[case(PageType::TableLeaf, 4096, 4061, None)]
#[case(PageType::TableLeaf, 4096, 4062, Some(493))]
#[case(PageType::TableLeaf, 4096, 4500, Some(493))]
#[case(PageType::TableLeaf, 4096, 4581, Some(493))]
#[case(PageType::TableLeaf, 4096, 5000, Some(912))]
#[case(PageType::TableLeaf, 4096, 8153, Some(4065))]
#[case(PageType::TableLeaf, 4096, 8154, Some(493))]
#[case(PageType::IndexLeaf, 4096, 1002, None)]
#[case(PageType::IndexLeaf, 4096, 1003, Some(493))]
#[case(PageType::IndexLeaf, 4096, 4581, Some(493))]
#[case(PageType::IndexLeaf, 4096, 5094, Some(1006))]
#[case(PageType::IndexLeaf, 4096, 5095, Some(493))]
#[case(PageType::IndexInterior, 4096, 5000, Some(912))]
#[case(PageType::TableLeaf, 512, 477, None)]
#[case(PageType::TableLeaf, 512, 478, Some(43))]
#[case(PageType::IndexLeaf, 512, 102, None)]
#[case(PageType::IndexLeaf, 512, 103, Some(43))]
#[case(PageType::TableLeaf, 65536, 65501, None)]
#[case(PageType::TableLeaf, 65536, 65502, Some(8203))]
fn test_payload_overflows(
    #[case] page_type: PageType,
    #[case] usable_size: usize,
    #[case] payload_size: usize,
    #[case] expected: Option<usize>,
) {
    let result = payload_overflows(
        payload_size,
        payload_overflow_threshold_max(page_type, usable_size),
        payload_overflow_threshold_min(page_type, usable_size),
        usable_size,
    );
    assert_eq!(result, expected);
}

#[rstest]
#[case(&[], SerialType::null(), Value::Null)]
#[case(&[255], SerialType::i8(), Value::from_i64(-1))]
#[case(&[0x12, 0x34], SerialType::i16(), Value::from_i64(0x1234))]
#[case(&[0xFE], SerialType::i8(), Value::from_i64(-2))]
#[case(&[0x12, 0x34, 0x56], SerialType::i24(), Value::from_i64(0x123456))]
#[case(&[0x12, 0x34, 0x56, 0x78], SerialType::i32(), Value::from_i64(0x12345678))]
#[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC], SerialType::i48(), Value::from_i64(0x123456789ABC)
)]
#[case(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xFF], SerialType::i64(), Value::from_i64(0x123456789ABCDEFF)
)]
#[case(&[0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18], SerialType::f64(), Value::from_f64(std::f64::consts::PI)
)]
#[case(&[1, 2], SerialType::const_int0(), Value::from_i64(0))]
#[case(&[65, 66], SerialType::const_int1(), Value::from_i64(1))]
#[case(
    &[1, 2, 3],
    SerialType::blob(3),
    Value::from_slice(&[1, 2, 3]).expect(crate::alloc::ALLOC_ERR_MSG)
)]
#[case(
    &[],
    SerialType::blob(0),
    Value::from_slice(&[]).expect(crate::alloc::ALLOC_ERR_MSG)
)] // empty blob
#[case(&[65, 66, 67], SerialType::text(3), Value::build_text("ABC"))]
#[case(&[0x80], SerialType::i8(), Value::from_i64(-128))]
#[case(&[0x80, 0], SerialType::i16(), Value::from_i64(-32768))]
#[case(&[0x80, 0, 0], SerialType::i24(), Value::from_i64(-8388608))]
#[case(&[0x80, 0, 0, 0], SerialType::i32(), Value::from_i64(-2147483648))]
#[case(&[0x80, 0, 0, 0, 0, 0], SerialType::i48(), Value::from_i64(-140737488355328))]
#[case(&[0x80, 0, 0, 0, 0, 0, 0, 0], SerialType::i64(), Value::from_i64(-9223372036854775808))]
#[case(&[0x7f], SerialType::i8(), Value::from_i64(127))]
#[case(&[0x7f, 0xff], SerialType::i16(), Value::from_i64(32767))]
#[case(&[0x7f, 0xff, 0xff], SerialType::i24(), Value::from_i64(8388607))]
#[case(&[0x7f, 0xff, 0xff, 0xff], SerialType::i32(), Value::from_i64(2147483647))]
#[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff], SerialType::i48(), Value::from_i64(140737488355327)
)]
#[case(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], SerialType::i64(), Value::from_i64(9223372036854775807)
)]
fn test_read_value(#[case] buf: &[u8], #[case] serial_type: SerialType, #[case] expected: Value) {
    let result = read_value(buf, serial_type).unwrap();
    assert_eq!(
        result.0.to_owned().expect(crate::alloc::ALLOC_ERR_MSG),
        expected
    );
}

#[test]
fn test_text_decoders_reject_invalid_utf8() {
    for result in [
        read_value(&[0xff], SerialType::text(1)),
        read_value_serial_type(&[0xff], 15),
    ] {
        assert!(
            matches!(
                result,
                Err(LimboError::Corrupt(ref message))
                    if message == "TEXT value contains invalid UTF-8"
            ),
            "unexpected result: {result:?}"
        );
    }
}

#[test]
fn test_serial_type_helpers() {
    assert_eq!(
        TryInto::<SerialType>::try_into(12u64).unwrap(),
        SerialType::blob(0)
    );
    assert_eq!(
        TryInto::<SerialType>::try_into(14u64).unwrap(),
        SerialType::blob(1)
    );
    assert_eq!(
        TryInto::<SerialType>::try_into(13u64).unwrap(),
        SerialType::text(0)
    );
    assert_eq!(
        TryInto::<SerialType>::try_into(15u64).unwrap(),
        SerialType::text(1)
    );
    assert_eq!(
        TryInto::<SerialType>::try_into(16u64).unwrap(),
        SerialType::blob(2)
    );
    assert_eq!(
        TryInto::<SerialType>::try_into(17u64).unwrap(),
        SerialType::text(2)
    );
}

#[rstest]
#[case(0, SerialType::null())]
#[case(1, SerialType::i8())]
#[case(2, SerialType::i16())]
#[case(3, SerialType::i24())]
#[case(4, SerialType::i32())]
#[case(5, SerialType::i48())]
#[case(6, SerialType::i64())]
#[case(7, SerialType::f64())]
#[case(8, SerialType::const_int0())]
#[case(9, SerialType::const_int1())]
#[case(12, SerialType::blob(0))]
#[case(13, SerialType::text(0))]
#[case(14, SerialType::blob(1))]
#[case(15, SerialType::text(1))]
fn test_parse_serial_type(#[case] input: u64, #[case] expected: SerialType) {
    let result = SerialType::try_from(input).unwrap();
    assert_eq!(result, expected);
}

#[test]
fn test_validate_serial_type() {
    for i in 0..=9 {
        let result = validate_serial_type(i);
        assert!(result.is_ok());
    }
    for i in 10..=11 {
        let result = validate_serial_type(i);
        assert!(result.is_err());
    }
    for i in 12..=1000 {
        let result = validate_serial_type(i);
        assert!(result.is_ok());
    }
}

#[rstest]
#[case(&[])] // empty buffer
#[case(&[0x80])] // truncated 1-byte with continuation
#[case(&[0x80, 0x80])] // truncated 2-byte
#[case(&[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])] // 9-byte truncated to 8
#[case(&[0x80; 9])] // bits set without end
fn test_read_varint_malformed_inputs(#[case] buf: &[u8]) {
    assert!(read_varint(buf).is_err());
}

#[test]
fn streaming_reader_ignores_uncommitted_checksums() {
    let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
    let file = io
        .open_file("streaming-reader-wal", crate::OpenFlags::Create, false)
        .unwrap();

    let page_size: usize = 1024;
    let buffer_pool = BufferPool::begin_init(&io, BufferPool::TEST_ARENA_SIZE);
    buffer_pool
        .finalize_with_page_size(page_size)
        .expect("initialize buffer pool");

    let mut wal_header = WalHeader {
        magic: WAL_MAGIC_LE,
        file_format: 3007000,
        page_size: page_size as u32,
        checkpoint_seq: 0,
        salt_1: 0x1234_5678,
        salt_2: 0x9abc_def0,
        checksum_1: 0,
        checksum_2: 0,
    };
    let header_prefix = &wal_header.as_bytes()[..WAL_HEADER_SIZE - 8];
    let use_native = (wal_header.magic & 1) != 0;
    let (c1, c2) = checksum_wal(header_prefix, &wal_header, (0, 0), use_native);
    wal_header.checksum_1 = c1;
    wal_header.checksum_2 = c2;
    io.wait_for_completion(begin_write_wal_header(file.as_ref(), &wal_header, None).unwrap())
        .unwrap();

    let page = vec![0xAB; page_size];
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let mut offset = WAL_HEADER_SIZE as u64;

    let (commit_checksum, commit_frame) = prepare_wal_frame(
        &buffer_pool,
        &wal_header,
        (wal_header.checksum_1, wal_header.checksum_2),
        wal_header.page_size,
        1,
        1,
        &page,
    );
    let commit_frame_clone = commit_frame.clone();
    let c = file
        .pwrite(
            offset,
            commit_frame,
            Completion::new_write(move |res| {
                assert_eq!(res.unwrap() as usize, frame_size);
                let _keep = commit_frame_clone.clone();
            }),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    offset += frame_size as u64;

    let (after_frame2_checksum, frame2) = prepare_wal_frame(
        &buffer_pool,
        &wal_header,
        commit_checksum,
        wal_header.page_size,
        2,
        0,
        &page,
    );
    let frame2_clone = frame2.clone();
    let c = file
        .pwrite(
            offset,
            frame2,
            Completion::new_write(move |res| {
                assert_eq!(res.unwrap() as usize, frame_size);
                let _keep = frame2_clone.clone();
            }),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();
    offset += frame_size as u64;

    let (after_frame3_checksum, frame3) = prepare_wal_frame(
        &buffer_pool,
        &wal_header,
        after_frame2_checksum,
        wal_header.page_size,
        3,
        0,
        &page,
    );
    let frame3_clone = frame3.clone();
    let c = file
        .pwrite(
            offset,
            frame3,
            Completion::new_write(move |res| {
                assert_eq!(res.unwrap() as usize, frame_size);
                let _keep = frame3_clone.clone();
            }),
        )
        .unwrap();
    io.wait_for_completion(c).unwrap();

    let shared = build_shared_wal(&file, &io).unwrap();
    let guard = shared.read();
    assert_eq!(guard.metadata.max_frame.load(Ordering::Acquire), 1);
    assert_eq!(guard.metadata.last_checksum, commit_checksum);

    // checksum should only include committed frame.
    assert_ne!(guard.metadata.last_checksum, after_frame3_checksum);

    let frame_cache = guard.runtime.frame_cache.lock();
    assert_eq!(frame_cache.get(&1), Some(&vec![1u64]));
    assert!(frame_cache.get(&2).is_none());
}

#[quickcheck_macros::quickcheck]
fn varint_len_matches_write_varint(value: u64) -> bool {
    let mut buf = [0u8; 9];
    let written = write_varint(&mut buf, value);
    varint_len(value) == written
}
