pub const CCLASS_SILENT: u8 = 0;
pub const CCLASS_VOWEL: u8 = 1;
pub const CCLASS_B: u8 = 2;
pub const CCLASS_Y: u8 = 9;
pub const CCLASS_L: u8 = 6;
pub const CCLASS_R: u8 = 7;
//pub const CCLASS_M: u8 = 8;
pub const CCLASS_DIGIT: u8 = 10;
pub const CCLASS_SPACE: u8 = 11;
pub const CCLASS_OTHER: u8 = 12;
pub const MID_CLASS: [u8; 128] = [
    12, 12, 12, 12, 12, 12, 12, 12, 12, 11, 12, 12, 11, 11, 12, 12, //
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 11, //
    12, 12, 12, 12, 12, 12, 0, 12, 12, 12, 12, 12, 12, 12, 12, 12, //
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 12, 12, 12, 12, 12, 12, //
    12, 1, 2, 3, 4, 1, 2, 3, 0, 1, 3, 3, 6, 8, 8, 1, //
    2, 3, 7, 3, 4, 1, 2, 2, 3, 1, 3, 12, 12, 12, 12, 12, //
    12, 1, 2, 3, 4, 1, 2, 3, 0, 1, 3, 3, 6, 8, 8, 1, //
    2, 3, 7, 3, 4, 1, 2, 2, 3, 1, 3, 12, 12, 12, 12, 12, //
];

pub const INIT_CLASS: [u8; 128] = [
    12, 12, 12, 12, 12, 12, 12, 12, 12, 11, 12, 12, 11, 11, 12, 12, //
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 11, //
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, //
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 12, 12, 12, 12, 12, 12, //
    12, 1, 2, 3, 4, 1, 2, 3, 0, 1, 3, 3, 6, 8, 8, 1, //
    2, 3, 7, 3, 4, 1, 2, 2, 3, 9, 3, 12, 12, 12, 12, 12, //
    12, 1, 2, 3, 4, 1, 2, 3, 0, 1, 3, 3, 6, 8, 8, 1, //
    2, 3, 7, 3, 4, 1, 2, 2, 3, 9, 3, 12, 12, 12, 12, 12, //
];

// Based on: const unsigned char className[] = ".ABCDHLRMY9 ?";
pub const CLASS_NAME: [u8; 13] = [
    b'.', // CCLASS_SILENT (0) -> .
    b'A', // CCLASS_VOWEL (1) -> A
    b'B', // CCLASS_B (2) -> B
    b'C', // CCLASS_C (3) -> C
    b'D', // CCLASS_D (4) -> D
    b'H', // CCLASS_H (5) -> H
    b'L', // CCLASS_L (6) -> L
    b'R', // CCLASS_R (7) -> R
    b'M', // CCLASS_M (8) -> M
    b'Y', // CCLASS_Y (9) -> Y
    b'9', // CCLASS_DIGIT (10) -> 9
    b' ', // CCLASS_SPACE (11) -> space
    b'?', // CCLASS_OTHER (12) -> ?
];

pub const SCRIPT_LATIN: u32 = 0x0001;
pub const SCRIPT_CYRILLIC: u32 = 0x0002;
pub const SCRIPT_GREEK: u32 = 0x0004;
pub const SCRIPT_HEBREW: u32 = 0x0008;
pub const SCRIPT_ARABIC: u32 = 0x0010;
