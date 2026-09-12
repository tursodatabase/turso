use super::*;

#[test]
fn visible_sqlite_header_requires_layout_fields() {
    let err = PageCodecHeaderInfo::from_visible_sqlite_header(&[0; 20]).unwrap_err();
    assert!(matches!(
        err,
        LimboError::InvalidArgument(message)
            if message == "page codec bootstrap requires at least 21 bytes, got 20"
    ));

    let mut prefix = [0; 21];
    prefix[16..18].copy_from_slice(&4096u16.to_be_bytes());
    prefix[20] = 7;
    assert_eq!(
        PageCodecHeaderInfo::from_visible_sqlite_header(&prefix).unwrap(),
        PageCodecHeaderInfo {
            page_size: 4096,
            reserved_space: 7,
        }
    );
}

#[test]
fn visible_sqlite_header_decodes_maximum_page_size_sentinel() {
    let mut prefix = [0; 21];
    prefix[16..18].copy_from_slice(&1u16.to_be_bytes());
    prefix[20] = u8::MAX;

    assert_eq!(
        PageCodecHeaderInfo::from_visible_sqlite_header(&prefix).unwrap(),
        PageCodecHeaderInfo {
            page_size: 65_536,
            reserved_space: u8::MAX,
        }
    );
}

#[test]
fn page_codec_context_checks_persistent_page_number_width() {
    for location in [PageLocation::Database, PageLocation::Wal] {
        assert_eq!(
            PageCodecContext::from_page_idx(1, location).unwrap(),
            PageCodecContext::new(1, location)
        );
        assert_eq!(
            PageCodecContext::from_page_idx(u32::MAX as usize, location).unwrap(),
            PageCodecContext::new(u32::MAX, location)
        );

        #[cfg(target_pointer_width = "64")]
        assert!(matches!(
            PageCodecContext::from_page_idx(u32::MAX as usize + 1, location),
            Err(LimboError::IntegerOverflow)
        ));
    }
}
