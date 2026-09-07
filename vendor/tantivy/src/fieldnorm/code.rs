#[inline]
pub fn id_to_fieldnorm(id: u8) -> u32 {
    FIELD_NORMS_TABLE[id as usize]
}

#[inline]
pub fn fieldnorm_to_id(fieldnorm: u32) -> u8 {
    FIELD_NORMS_TABLE
        .binary_search(&fieldnorm)
        .unwrap_or_else(|idx| idx - 1) as u8
}

pub const FIELD_NORMS_TABLE: [u32; 256] = [
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    42,
    44,
    46,
    48,
    50,
    52,
    54,
    56,
    60,
    64,
    68,
    72,
    76,
    80,
    84,
    88,
    96,
    104,
    112,
    120,
    128,
    136,
    144,
    152,
    168,
    184,
    200,
    216,
    232,
    248,
    264,
    280,
    312,
    344,
    376,
    408,
    440,
    472,
    504,
    536,
    600,
    664,
    728,
    792,
    856,
    920,
    984,
    1_048,
    1_176,
    1_304,
    1_432,
    1_560,
    1_688,
    1_816,
    1_944,
    2_072,
    2_328,
    2_584,
    2_840,
    3_096,
    3_352,
    3_608,
    3_864,
    4_120,
    4_632,
    5_144,
    5_656,
    6_168,
    6_680,
    7_192,
    7_704,
    8_216,
    9_240,
    10_264,
    11_288,
    12_312,
    13_336,
    14_360,
    15_384,
    16_408,
    18_456,
    20_504,
    22_552,
    24_600,
    26_648,
    28_696,
    30_744,
    32_792,
    36_888,
    40_984,
    45_080,
    49_176,
    53_272,
    57_368,
    61_464,
    65_560,
    73_752,
    81_944,
    90_136,
    98_328,
    106_520,
    114_712,
    122_904,
    131_096,
    147_480,
    163_864,
    180_248,
    196_632,
    213_016,
    229_400,
    245_784,
    262_168,
    294_936,
    327_704,
    360_472,
    393_240,
    426_008,
    458_776,
    491_544,
    524_312,
    589_848,
    655_384,
    720_920,
    786_456,
    851_992,
    917_528,
    983_064,
    1_048_600,
    1_179_672,
    1_310_744,
    1_441_816,
    1_572_888,
    1_703_960,
    1_835_032,
    1_966_104,
    2_097_176,
    2_359_320,
    2_621_464,
    2_883_608,
    3_145_752,
    3_407_896,
    3_670_040,
    3_932_184,
    4_194_328,
    4_718_616,
    5_242_904,
    5_767_192,
    6_291_480,
    6_815_768,
    7_340_056,
    7_864_344,
    8_388_632,
    9_437_208,
    10_485_784,
    11_534_360,
    12_582_936,
    13_631_512,
    14_680_088,
    15_728_664,
    16_777_240,
    18_874_392,
    20_971_544,
    23_068_696,
    25_165_848,
    27_263_000,
    29_360_152,
    31_457_304,
    33_554_456,
    37_748_760,
    41_943_064,
    46_137_368,
    50_331_672,
    54_525_976,
    58_720_280,
    62_914_584,
    67_108_888,
    75_497_496,
    83_886_104,
    92_274_712,
    100_663_320,
    109_051_928,
    117_440_536,
    125_829_144,
    134_217_752,
    150_994_968,
    167_772_184,
    184_549_400,
    201_326_616,
    218_103_832,
    234_881_048,
    251_658_264,
    268_435_480,
    301_989_912,
    335_544_344,
    369_098_776,
    402_653_208,
    436_207_640,
    469_762_072,
    503_316_504,
    536_870_936,
    603_979_800,
    671_088_664,
    738_197_528,
    805_306_392,
    872_415_256,
    939_524_120,
    1_006_632_984,
    1_073_741_848,
    1_207_959_576,
    1_342_177_304,
    1_476_395_032,
    1_610_612_760,
    1_744_830_488,
    1_879_048_216,
    2_013_265_944,
];

#[cfg(test)]
mod tests {

    use super::{fieldnorm_to_id, id_to_fieldnorm, FIELD_NORMS_TABLE};

    #[test]
    fn test_decode_code() {
        assert_eq!(fieldnorm_to_id(0), 0);
        assert_eq!(fieldnorm_to_id(1), 1);
        for i in 0..41 {
            assert_eq!(fieldnorm_to_id(i), i as u8);
        }
        assert_eq!(fieldnorm_to_id(41), 40);
        assert_eq!(fieldnorm_to_id(42), 41);
        for id in 43..256 {
            let field_norm = FIELD_NORMS_TABLE[id];
            assert_eq!(id_to_fieldnorm(id as u8), field_norm);
            assert_eq!(fieldnorm_to_id(field_norm), id as u8);
            assert_eq!(fieldnorm_to_id(field_norm - 1), id as u8 - 1);
            assert_eq!(fieldnorm_to_id(field_norm + 1), id as u8);
        }
    }

    #[test]
    fn test_u32_max() {
        assert_eq!(fieldnorm_to_id(u32::MAX), u8::MAX);
    }

    #[test]
    fn test_fieldnorm_byte() {
        // const expression are not really a thing
        // yet... Therefore we do things the other way around.

        // The array is defined as a const,
        // and we check in the unit test that the const
        // value is matching the logic.
        const IDENTITY_PART: u8 = 24u8;
        fn decode_field_norm_exp_part(b: u8) -> u32 {
            let bits = (b & 0b00000111) as u32;
            let shift = b >> 3;
            if shift == 0 {
                bits
            } else {
                (bits | 8u32) << ((shift - 1u8) as u32)
            }
        }
        fn decode_fieldnorm_byte(b: u8) -> u32 {
            if b < IDENTITY_PART {
                b as u32
            } else {
                (IDENTITY_PART as u32) + decode_field_norm_exp_part(b - IDENTITY_PART)
            }
        }
        for i in 0..256 {
            assert_eq!(FIELD_NORMS_TABLE[i], decode_fieldnorm_byte(i as u8));
        }
    }
}
