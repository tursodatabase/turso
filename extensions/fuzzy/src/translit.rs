use crate::common::*;

static TRANSLIT_UTF8_LOOKUP: [u8; 64] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
];

#[derive(Copy, Clone, Debug)]
struct Transliteration {
    c_from: u16,
    c_to0: u8,
    c_to1: u8,
    c_to2: u8,
    c_to3: u8,
}

impl Transliteration {
    const fn new(c_from: u16, c_to0: u8, c_to1: u8, c_to2: u8, c_to3: u8) -> Self {
        Self {
            c_from,
            c_to0,
            c_to1,
            c_to2,
            c_to3,
        }
    }
}

static TRANSLIT: [Transliteration; 389] = [
    Transliteration::new(0x00A0, b' ', 0x00, 0x00, 0x00), /*   to   */
    Transliteration::new(0x00B5, b'u', 0x00, 0x00, 0x00), /* µ to u */
    Transliteration::new(0x00C0, b'A', 0x00, 0x00, 0x00), /* À to A */
    Transliteration::new(0x00C1, b'A', 0x00, 0x00, 0x00), /* Á to A */
    Transliteration::new(0x00C2, b'A', 0x00, 0x00, 0x00), /* Â to A */
    Transliteration::new(0x00C3, b'A', 0x00, 0x00, 0x00), /* Ã to A */
    Transliteration::new(0x00C4, b'A', b'e', 0x00, 0x00), /* Ä to Ae */
    Transliteration::new(0x00C5, b'A', b'a', 0x00, 0x00), /* Å to Aa */
    Transliteration::new(0x00C6, b'A', b'E', 0x00, 0x00), /* Æ to AE */
    Transliteration::new(0x00C7, b'C', 0x00, 0x00, 0x00), /* Ç to C */
    Transliteration::new(0x00C8, b'E', 0x00, 0x00, 0x00), /* È to E */
    Transliteration::new(0x00C9, b'E', 0x00, 0x00, 0x00), /* É to E */
    Transliteration::new(0x00CA, b'E', 0x00, 0x00, 0x00), /* Ê to E */
    Transliteration::new(0x00CB, b'E', 0x00, 0x00, 0x00), /* Ë to E */
    Transliteration::new(0x00CC, b'I', 0x00, 0x00, 0x00), /* Ì to I */
    Transliteration::new(0x00CD, b'I', 0x00, 0x00, 0x00), /* Í to I */
    Transliteration::new(0x00CE, b'I', 0x00, 0x00, 0x00), /* Î to I */
    Transliteration::new(0x00CF, b'I', 0x00, 0x00, 0x00), /* Ï to I */
    Transliteration::new(0x00D0, b'D', 0x00, 0x00, 0x00), /* Ð to D */
    Transliteration::new(0x00D1, b'N', 0x00, 0x00, 0x00), /* Ñ to N */
    Transliteration::new(0x00D2, b'O', 0x00, 0x00, 0x00), /* Ò to O */
    Transliteration::new(0x00D3, b'O', 0x00, 0x00, 0x00), /* Ó to O */
    Transliteration::new(0x00D4, b'O', 0x00, 0x00, 0x00), /* Ô to O */
    Transliteration::new(0x00D5, b'O', 0x00, 0x00, 0x00), /* Õ to O */
    Transliteration::new(0x00D6, b'O', b'e', 0x00, 0x00), /* Ö to Oe */
    Transliteration::new(0x00D7, b'x', 0x00, 0x00, 0x00), /* × to x */
    Transliteration::new(0x00D8, b'O', 0x00, 0x00, 0x00), /* Ø to O */
    Transliteration::new(0x00D9, b'U', 0x00, 0x00, 0x00), /* Ù to U */
    Transliteration::new(0x00DA, b'U', 0x00, 0x00, 0x00), /* Ú to U */
    Transliteration::new(0x00DB, b'U', 0x00, 0x00, 0x00), /* Û to U */
    Transliteration::new(0x00DC, b'U', b'e', 0x00, 0x00), /* Ü to Ue */
    Transliteration::new(0x00DD, b'Y', 0x00, 0x00, 0x00), /* Ý to Y */
    Transliteration::new(0x00DE, b'T', b'h', 0x00, 0x00), /* Þ to Th */
    Transliteration::new(0x00DF, b's', b's', 0x00, 0x00), /* ß to ss */
    Transliteration::new(0x00E0, b'a', 0x00, 0x00, 0x00), /* à to a */
    Transliteration::new(0x00E1, b'a', 0x00, 0x00, 0x00), /* á to a */
    Transliteration::new(0x00E2, b'a', 0x00, 0x00, 0x00), /* â to a */
    Transliteration::new(0x00E3, b'a', 0x00, 0x00, 0x00), /* ã to a */
    Transliteration::new(0x00E4, b'a', b'e', 0x00, 0x00), /* ä to ae */
    Transliteration::new(0x00E5, b'a', b'a', 0x00, 0x00), /* å to aa */
    Transliteration::new(0x00E6, b'a', b'e', 0x00, 0x00), /* æ to ae */
    Transliteration::new(0x00E7, b'c', 0x00, 0x00, 0x00), /* ç to c */
    Transliteration::new(0x00E8, b'e', 0x00, 0x00, 0x00), /* è to e */
    Transliteration::new(0x00E9, b'e', 0x00, 0x00, 0x00), /* é to e */
    Transliteration::new(0x00EA, b'e', 0x00, 0x00, 0x00), /* ê to e */
    Transliteration::new(0x00EB, b'e', 0x00, 0x00, 0x00), /* ë to e */
    Transliteration::new(0x00EC, b'i', 0x00, 0x00, 0x00), /* ì to i */
    Transliteration::new(0x00ED, b'i', 0x00, 0x00, 0x00), /* í to i */
    Transliteration::new(0x00EE, b'i', 0x00, 0x00, 0x00), /* î to i */
    Transliteration::new(0x00EF, b'i', 0x00, 0x00, 0x00), /* ï to i */
    Transliteration::new(0x00F0, b'd', 0x00, 0x00, 0x00), /* ð to d */
    Transliteration::new(0x00F1, b'n', 0x00, 0x00, 0x00), /* ñ to n */
    Transliteration::new(0x00F2, b'o', 0x00, 0x00, 0x00), /* ò to o */
    Transliteration::new(0x00F3, b'o', 0x00, 0x00, 0x00), /* ó to o */
    Transliteration::new(0x00F4, b'o', 0x00, 0x00, 0x00), /* ô to o */
    Transliteration::new(0x00F5, b'o', 0x00, 0x00, 0x00), /* õ to o */
    Transliteration::new(0x00F6, b'o', b'e', 0x00, 0x00), /* ö to oe */
    Transliteration::new(0x00F7, b':', 0x00, 0x00, 0x00), /* ÷ to : */
    Transliteration::new(0x00F8, b'o', 0x00, 0x00, 0x00), /* ø to o */
    Transliteration::new(0x00F9, b'u', 0x00, 0x00, 0x00), /* ù to u */
    Transliteration::new(0x00FA, b'u', 0x00, 0x00, 0x00), /* ú to u */
    Transliteration::new(0x00FB, b'u', 0x00, 0x00, 0x00), /* û to u */
    Transliteration::new(0x00FC, b'u', b'e', 0x00, 0x00), /* ü to ue */
    Transliteration::new(0x00FD, b'y', 0x00, 0x00, 0x00), /* ý to y */
    Transliteration::new(0x00FE, b't', b'h', 0x00, 0x00), /* þ to th */
    Transliteration::new(0x00FF, b'y', 0x00, 0x00, 0x00), /* ÿ to y */
    Transliteration::new(0x0100, b'A', 0x00, 0x00, 0x00), /* Ā to A */
    Transliteration::new(0x0101, b'a', 0x00, 0x00, 0x00), /* ā to a */
    Transliteration::new(0x0102, b'A', 0x00, 0x00, 0x00), /* Ă to A */
    Transliteration::new(0x0103, b'a', 0x00, 0x00, 0x00), /* ă to a */
    Transliteration::new(0x0104, b'A', 0x00, 0x00, 0x00), /* Ą to A */
    Transliteration::new(0x0105, b'a', 0x00, 0x00, 0x00), /* ą to a */
    Transliteration::new(0x0106, b'C', 0x00, 0x00, 0x00), /* Ć to C */
    Transliteration::new(0x0107, b'c', 0x00, 0x00, 0x00), /* ć to c */
    Transliteration::new(0x0108, b'C', b'h', 0x00, 0x00), /* Ĉ to Ch */
    Transliteration::new(0x0109, b'c', b'h', 0x00, 0x00), /* ĉ to ch */
    Transliteration::new(0x010A, b'C', 0x00, 0x00, 0x00), /* Ċ to C */
    Transliteration::new(0x010B, b'c', 0x00, 0x00, 0x00), /* ċ to c */
    Transliteration::new(0x010C, b'C', 0x00, 0x00, 0x00), /* Č to C */
    Transliteration::new(0x010D, b'c', 0x00, 0x00, 0x00), /* č to c */
    Transliteration::new(0x010E, b'D', 0x00, 0x00, 0x00), /* Ď to D */
    Transliteration::new(0x010F, b'd', 0x00, 0x00, 0x00), /* ď to d */
    Transliteration::new(0x0110, b'D', 0x00, 0x00, 0x00), /* Đ to D */
    Transliteration::new(0x0111, b'd', 0x00, 0x00, 0x00), /* đ to d */
    Transliteration::new(0x0112, b'E', 0x00, 0x00, 0x00), /* Ē to E */
    Transliteration::new(0x0113, b'e', 0x00, 0x00, 0x00), /* ē to e */
    Transliteration::new(0x0114, b'E', 0x00, 0x00, 0x00), /* Ĕ to E */
    Transliteration::new(0x0115, b'e', 0x00, 0x00, 0x00), /* ĕ to e */
    Transliteration::new(0x0116, b'E', 0x00, 0x00, 0x00), /* Ė to E */
    Transliteration::new(0x0117, b'e', 0x00, 0x00, 0x00), /* ė to e */
    Transliteration::new(0x0118, b'E', 0x00, 0x00, 0x00), /* Ę to E */
    Transliteration::new(0x0119, b'e', 0x00, 0x00, 0x00), /* ę to e */
    Transliteration::new(0x011A, b'E', 0x00, 0x00, 0x00), /* Ě to E */
    Transliteration::new(0x011B, b'e', 0x00, 0x00, 0x00), /* ě to e */
    Transliteration::new(0x011C, b'G', b'h', 0x00, 0x00), /* Ĝ to Gh */
    Transliteration::new(0x011D, b'g', b'h', 0x00, 0x00), /* ĝ to gh */
    Transliteration::new(0x011E, b'G', 0x00, 0x00, 0x00), /* Ğ to G */
    Transliteration::new(0x011F, b'g', 0x00, 0x00, 0x00), /* ğ to g */
    Transliteration::new(0x0120, b'G', 0x00, 0x00, 0x00), /* Ġ to G */
    Transliteration::new(0x0121, b'g', 0x00, 0x00, 0x00), /* ġ to g */
    Transliteration::new(0x0122, b'G', 0x00, 0x00, 0x00), /* Ģ to G */
    Transliteration::new(0x0123, b'g', 0x00, 0x00, 0x00), /* ģ to g */
    Transliteration::new(0x0124, b'H', b'h', 0x00, 0x00), /* Ĥ to Hh */
    Transliteration::new(0x0125, b'h', b'h', 0x00, 0x00), /* ĥ to hh */
    Transliteration::new(0x0126, b'H', 0x00, 0x00, 0x00), /* Ħ to H */
    Transliteration::new(0x0127, b'h', 0x00, 0x00, 0x00), /* ħ to h */
    Transliteration::new(0x0128, b'I', 0x00, 0x00, 0x00), /* Ĩ to I */
    Transliteration::new(0x0129, b'i', 0x00, 0x00, 0x00), /* ĩ to i */
    Transliteration::new(0x012A, b'I', 0x00, 0x00, 0x00), /* Ī to I */
    Transliteration::new(0x012B, b'i', 0x00, 0x00, 0x00), /* ī to i */
    Transliteration::new(0x012C, b'I', 0x00, 0x00, 0x00), /* Ĭ to I */
    Transliteration::new(0x012D, b'i', 0x00, 0x00, 0x00), /* ĭ to i */
    Transliteration::new(0x012E, b'I', 0x00, 0x00, 0x00), /* Į to I */
    Transliteration::new(0x012F, b'i', 0x00, 0x00, 0x00), /* į to i */
    Transliteration::new(0x0130, b'I', 0x00, 0x00, 0x00), /* İ to I */
    Transliteration::new(0x0131, b'i', 0x00, 0x00, 0x00), /* ı to i */
    Transliteration::new(0x0132, b'I', b'J', 0x00, 0x00), /* Ĳ to IJ */
    Transliteration::new(0x0133, b'i', b'j', 0x00, 0x00), /* ĳ to ij */
    Transliteration::new(0x0134, b'J', b'h', 0x00, 0x00), /* Ĵ to Jh */
    Transliteration::new(0x0135, b'j', b'h', 0x00, 0x00), /* ĵ to jh */
    Transliteration::new(0x0136, b'K', 0x00, 0x00, 0x00), /* Ķ to K */
    Transliteration::new(0x0137, b'k', 0x00, 0x00, 0x00), /* ķ to k */
    Transliteration::new(0x0138, b'k', 0x00, 0x00, 0x00), /* ĸ to k */
    Transliteration::new(0x0139, b'L', 0x00, 0x00, 0x00), /* Ĺ to L */
    Transliteration::new(0x013A, b'l', 0x00, 0x00, 0x00), /* ĺ to l */
    Transliteration::new(0x013B, b'L', 0x00, 0x00, 0x00), /* Ļ to L */
    Transliteration::new(0x013C, b'l', 0x00, 0x00, 0x00), /* ļ to l */
    Transliteration::new(0x013D, b'L', 0x00, 0x00, 0x00), /* Ľ to L */
    Transliteration::new(0x013E, b'l', 0x00, 0x00, 0x00), /* ľ to l */
    Transliteration::new(0x013F, b'L', b'.', 0x00, 0x00), /* Ŀ to L. */
    Transliteration::new(0x0140, b'l', b'.', 0x00, 0x00), /* ŀ to l. */
    Transliteration::new(0x0141, b'L', 0x00, 0x00, 0x00), /* Ł to L */
    Transliteration::new(0x0142, b'l', 0x00, 0x00, 0x00), /* ł to l */
    Transliteration::new(0x0143, b'N', 0x00, 0x00, 0x00), /* Ń to N */
    Transliteration::new(0x0144, b'n', 0x00, 0x00, 0x00), /* ń to n */
    Transliteration::new(0x0145, b'N', 0x00, 0x00, 0x00), /* Ņ to N */
    Transliteration::new(0x0146, b'n', 0x00, 0x00, 0x00), /* ņ to n */
    Transliteration::new(0x0147, b'N', 0x00, 0x00, 0x00), /* Ň to N */
    Transliteration::new(0x0148, b'n', 0x00, 0x00, 0x00), /* ň to n */
    Transliteration::new(0x0149, b'\'', b'n', 0x00, 0x00), /* ŉ to 'n */
    Transliteration::new(0x014A, b'N', b'G', 0x00, 0x00), /* Ŋ to NG */
    Transliteration::new(0x014B, b'n', b'g', 0x00, 0x00), /* ŋ to ng */
    Transliteration::new(0x014C, b'O', 0x00, 0x00, 0x00), /* Ō to O */
    Transliteration::new(0x014D, b'o', 0x00, 0x00, 0x00), /* ō to o */
    Transliteration::new(0x014E, b'O', 0x00, 0x00, 0x00), /* Ŏ to O */
    Transliteration::new(0x014F, b'o', 0x00, 0x00, 0x00), /* ŏ to o */
    Transliteration::new(0x0150, b'O', 0x00, 0x00, 0x00), /* Ő to O */
    Transliteration::new(0x0151, b'o', 0x00, 0x00, 0x00), /* ő to o */
    Transliteration::new(0x0152, b'O', b'E', 0x00, 0x00), /* Œ to OE */
    Transliteration::new(0x0153, b'o', b'e', 0x00, 0x00), /* œ to oe */
    Transliteration::new(0x0154, b'R', 0x00, 0x00, 0x00), /* Ŕ to R */
    Transliteration::new(0x0155, b'r', 0x00, 0x00, 0x00), /* ŕ to r */
    Transliteration::new(0x0156, b'R', 0x00, 0x00, 0x00), /* Ŗ to R */
    Transliteration::new(0x0157, b'r', 0x00, 0x00, 0x00), /* ŗ to r */
    Transliteration::new(0x0158, b'R', 0x00, 0x00, 0x00), /* Ř to R */
    Transliteration::new(0x0159, b'r', 0x00, 0x00, 0x00), /* ř to r */
    Transliteration::new(0x015A, b'S', 0x00, 0x00, 0x00), /* Ś to S */
    Transliteration::new(0x015B, b's', 0x00, 0x00, 0x00), /* ś to s */
    Transliteration::new(0x015C, b'S', b'h', 0x00, 0x00), /* Ŝ to Sh */
    Transliteration::new(0x015D, b's', b'h', 0x00, 0x00), /* ŝ to sh */
    Transliteration::new(0x015E, b'S', 0x00, 0x00, 0x00), /* Ş to S */
    Transliteration::new(0x015F, b's', 0x00, 0x00, 0x00), /* ş to s */
    Transliteration::new(0x0160, b'S', 0x00, 0x00, 0x00), /* Š to S */
    Transliteration::new(0x0161, b's', 0x00, 0x00, 0x00), /* š to s */
    Transliteration::new(0x0162, b'T', 0x00, 0x00, 0x00), /* Ţ to T */
    Transliteration::new(0x0163, b't', 0x00, 0x00, 0x00), /* ţ to t */
    Transliteration::new(0x0164, b'T', 0x00, 0x00, 0x00), /* Ť to T */
    Transliteration::new(0x0165, b't', 0x00, 0x00, 0x00), /* ť to t */
    Transliteration::new(0x0166, b'T', 0x00, 0x00, 0x00), /* Ŧ to T */
    Transliteration::new(0x0167, b't', 0x00, 0x00, 0x00), /* ŧ to t */
    Transliteration::new(0x0168, b'U', 0x00, 0x00, 0x00), /* Ũ to U */
    Transliteration::new(0x0169, b'u', 0x00, 0x00, 0x00), /* ũ to u */
    Transliteration::new(0x016A, b'U', 0x00, 0x00, 0x00), /* Ū to U */
    Transliteration::new(0x016B, b'u', 0x00, 0x00, 0x00), /* ū to u */
    Transliteration::new(0x016C, b'U', 0x00, 0x00, 0x00), /* Ŭ to U */
    Transliteration::new(0x016D, b'u', 0x00, 0x00, 0x00), /* ŭ to u */
    Transliteration::new(0x016E, b'U', 0x00, 0x00, 0x00), /* Ů to U */
    Transliteration::new(0x016F, b'u', 0x00, 0x00, 0x00), /* ů to u */
    Transliteration::new(0x0170, b'U', 0x00, 0x00, 0x00), /* Ű to U */
    Transliteration::new(0x0171, b'u', 0x00, 0x00, 0x00), /* ű to u */
    Transliteration::new(0x0172, b'U', 0x00, 0x00, 0x00), /* Ų to U */
    Transliteration::new(0x0173, b'u', 0x00, 0x00, 0x00), /* ų to u */
    Transliteration::new(0x0174, b'W', 0x00, 0x00, 0x00), /* Ŵ to W */
    Transliteration::new(0x0175, b'w', 0x00, 0x00, 0x00), /* ŵ to w */
    Transliteration::new(0x0176, b'Y', 0x00, 0x00, 0x00), /* Ŷ to Y */
    Transliteration::new(0x0177, b'y', 0x00, 0x00, 0x00), /* ŷ to y */
    Transliteration::new(0x0178, b'Y', 0x00, 0x00, 0x00), /* Ÿ to Y */
    Transliteration::new(0x0179, b'Z', 0x00, 0x00, 0x00), /* Ź to Z */
    Transliteration::new(0x017A, b'z', 0x00, 0x00, 0x00), /* ź to z */
    Transliteration::new(0x017B, b'Z', 0x00, 0x00, 0x00), /* Ż to Z */
    Transliteration::new(0x017C, b'z', 0x00, 0x00, 0x00), /* ż to z */
    Transliteration::new(0x017D, b'Z', 0x00, 0x00, 0x00), /* Ž to Z */
    Transliteration::new(0x017E, b'z', 0x00, 0x00, 0x00), /* ž to z */
    Transliteration::new(0x017F, b's', 0x00, 0x00, 0x00), /* ſ to s */
    Transliteration::new(0x0192, b'f', 0x00, 0x00, 0x00), /* ƒ to f */
    Transliteration::new(0x0218, b'S', 0x00, 0x00, 0x00), /* Ș to S */
    Transliteration::new(0x0219, b's', 0x00, 0x00, 0x00), /* ș to s */
    Transliteration::new(0x021A, b'T', 0x00, 0x00, 0x00), /* Ț to T */
    Transliteration::new(0x021B, b't', 0x00, 0x00, 0x00), /* ț to t */
    Transliteration::new(0x0386, b'A', 0x00, 0x00, 0x00), /* Ά to A */
    Transliteration::new(0x0388, b'E', 0x00, 0x00, 0x00), /* Έ to E */
    Transliteration::new(0x0389, b'I', 0x00, 0x00, 0x00), /* Ή to I */
    Transliteration::new(0x038A, b'I', 0x00, 0x00, 0x00), /* Ί to I */
    Transliteration::new(0x038C, b'O', 0x00, 0x00, 0x00), /* Ό to O */
    Transliteration::new(0x038E, b'Y', 0x00, 0x00, 0x00), /* Ύ to Y */
    Transliteration::new(0x038F, b'O', 0x00, 0x00, 0x00), /* Ώ to O */
    Transliteration::new(0x0390, b'i', 0x00, 0x00, 0x00), /* ΐ to i */
    Transliteration::new(0x0391, b'A', 0x00, 0x00, 0x00), /* Α to A */
    Transliteration::new(0x0392, b'B', 0x00, 0x00, 0x00), /* Β to B */
    Transliteration::new(0x0393, b'G', 0x00, 0x00, 0x00), /* Γ to G */
    Transliteration::new(0x0394, b'D', 0x00, 0x00, 0x00), /* Δ to D */
    Transliteration::new(0x0395, b'E', 0x00, 0x00, 0x00), /* Ε to E */
    Transliteration::new(0x0396, b'Z', 0x00, 0x00, 0x00), /* Ζ to Z */
    Transliteration::new(0x0397, b'I', 0x00, 0x00, 0x00), /* Η to I */
    Transliteration::new(0x0398, b'T', b'h', 0x00, 0x00), /* Θ to Th */
    Transliteration::new(0x0399, b'I', 0x00, 0x00, 0x00), /* Ι to I */
    Transliteration::new(0x039A, b'K', 0x00, 0x00, 0x00), /* Κ to K */
    Transliteration::new(0x039B, b'L', 0x00, 0x00, 0x00), /* Λ to L */
    Transliteration::new(0x039C, b'M', 0x00, 0x00, 0x00), /* Μ to M */
    Transliteration::new(0x039D, b'N', 0x00, 0x00, 0x00), /* Ν to N */
    Transliteration::new(0x039E, b'X', 0x00, 0x00, 0x00), /* Ξ to X */
    Transliteration::new(0x039F, b'O', 0x00, 0x00, 0x00), /* Ο to O */
    Transliteration::new(0x03A0, b'P', 0x00, 0x00, 0x00), /* Π to P */
    Transliteration::new(0x03A1, b'R', 0x00, 0x00, 0x00), /* Ρ to R */
    Transliteration::new(0x03A3, b'S', 0x00, 0x00, 0x00), /* Σ to S */
    Transliteration::new(0x03A4, b'T', 0x00, 0x00, 0x00), /* Τ to T */
    Transliteration::new(0x03A5, b'Y', 0x00, 0x00, 0x00), /* Υ to Y */
    Transliteration::new(0x03A6, b'F', 0x00, 0x00, 0x00), /* Φ to F */
    Transliteration::new(0x03A7, b'C', b'h', 0x00, 0x00), /* Χ to Ch */
    Transliteration::new(0x03A8, b'P', b's', 0x00, 0x00), /* Ψ to Ps */
    Transliteration::new(0x03A9, b'O', 0x00, 0x00, 0x00), /* Ω to O */
    Transliteration::new(0x03AA, b'I', 0x00, 0x00, 0x00), /* Ϊ to I */
    Transliteration::new(0x03AB, b'Y', 0x00, 0x00, 0x00), /* Ϋ to Y */
    Transliteration::new(0x03AC, b'a', 0x00, 0x00, 0x00), /* ά to a */
    Transliteration::new(0x03AD, b'e', 0x00, 0x00, 0x00), /* έ to e */
    Transliteration::new(0x03AE, b'i', 0x00, 0x00, 0x00), /* ή to i */
    Transliteration::new(0x03AF, b'i', 0x00, 0x00, 0x00), /* ί to i */
    Transliteration::new(0x03B1, b'a', 0x00, 0x00, 0x00), /* α to a */
    Transliteration::new(0x03B2, b'b', 0x00, 0x00, 0x00), /* β to b */
    Transliteration::new(0x03B3, b'g', 0x00, 0x00, 0x00), /* γ to g */
    Transliteration::new(0x03B4, b'd', 0x00, 0x00, 0x00), /* δ to d */
    Transliteration::new(0x03B5, b'e', 0x00, 0x00, 0x00), /* ε to e */
    Transliteration::new(0x03B6, b'z', 0x00, 0x00, 0x00), /* ζ to z */
    Transliteration::new(0x03B7, b'i', 0x00, 0x00, 0x00), /* η to i */
    Transliteration::new(0x03B8, b't', b'h', 0x00, 0x00), /* θ to th */
    Transliteration::new(0x03B9, b'i', 0x00, 0x00, 0x00), /* ι to i */
    Transliteration::new(0x03BA, b'k', 0x00, 0x00, 0x00), /* κ to k */
    Transliteration::new(0x03BB, b'l', 0x00, 0x00, 0x00), /* λ to l */
    Transliteration::new(0x03BC, b'm', 0x00, 0x00, 0x00), /* μ to m */
    Transliteration::new(0x03BD, b'n', 0x00, 0x00, 0x00), /* ν to n */
    Transliteration::new(0x03BE, b'x', 0x00, 0x00, 0x00), /* ξ to x */
    Transliteration::new(0x03BF, b'o', 0x00, 0x00, 0x00), /* ο to o */
    Transliteration::new(0x03C0, b'p', 0x00, 0x00, 0x00), /* π to p */
    Transliteration::new(0x03C1, b'r', 0x00, 0x00, 0x00), /* ρ to r */
    Transliteration::new(0x03C3, b's', 0x00, 0x00, 0x00), /* σ to s */
    Transliteration::new(0x03C4, b't', 0x00, 0x00, 0x00), /* τ to t */
    Transliteration::new(0x03C5, b'y', 0x00, 0x00, 0x00), /* υ to y */
    Transliteration::new(0x03C6, b'f', 0x00, 0x00, 0x00), /* φ to f */
    Transliteration::new(0x03C7, b'c', b'h', 0x00, 0x00), /* χ to ch */
    Transliteration::new(0x03C8, b'p', b's', 0x00, 0x00), /* ψ to ps */
    Transliteration::new(0x03C9, b'o', 0x00, 0x00, 0x00), /* ω to o */
    Transliteration::new(0x03CA, b'i', 0x00, 0x00, 0x00), /* ϊ to i */
    Transliteration::new(0x03CB, b'y', 0x00, 0x00, 0x00), /* ϋ to y */
    Transliteration::new(0x03CC, b'o', 0x00, 0x00, 0x00), /* ό to o */
    Transliteration::new(0x03CD, b'y', 0x00, 0x00, 0x00), /* ύ to y */
    Transliteration::new(0x03CE, b'i', 0x00, 0x00, 0x00), /* ώ to i */
    Transliteration::new(0x0400, b'E', 0x00, 0x00, 0x00), /* Ѐ to E */
    Transliteration::new(0x0401, b'E', 0x00, 0x00, 0x00), /* Ё to E */
    Transliteration::new(0x0402, b'D', 0x00, 0x00, 0x00), /* Ђ to D */
    Transliteration::new(0x0403, b'G', 0x00, 0x00, 0x00), /* Ѓ to G */
    Transliteration::new(0x0404, b'E', 0x00, 0x00, 0x00), /* Є to E */
    Transliteration::new(0x0405, b'Z', 0x00, 0x00, 0x00), /* Ѕ to Z */
    Transliteration::new(0x0406, b'I', 0x00, 0x00, 0x00), /* І to I */
    Transliteration::new(0x0407, b'I', 0x00, 0x00, 0x00), /* Ї to I */
    Transliteration::new(0x0408, b'J', 0x00, 0x00, 0x00), /* Ј to J */
    Transliteration::new(0x0409, b'I', 0x00, 0x00, 0x00), /* Љ to I */
    Transliteration::new(0x040A, b'N', 0x00, 0x00, 0x00), /* Њ to N */
    Transliteration::new(0x040B, b'D', 0x00, 0x00, 0x00), /* Ћ to D */
    Transliteration::new(0x040C, b'K', 0x00, 0x00, 0x00), /* Ќ to K */
    Transliteration::new(0x040D, b'I', 0x00, 0x00, 0x00), /* Ѝ to I */
    Transliteration::new(0x040E, b'U', 0x00, 0x00, 0x00), /* Ў to U */
    Transliteration::new(0x040F, b'D', 0x00, 0x00, 0x00), /* Џ to D */
    Transliteration::new(0x0410, b'A', 0x00, 0x00, 0x00), /* А to A */
    Transliteration::new(0x0411, b'B', 0x00, 0x00, 0x00), /* Б to B */
    Transliteration::new(0x0412, b'V', 0x00, 0x00, 0x00), /* В to V */
    Transliteration::new(0x0413, b'G', 0x00, 0x00, 0x00), /* Г to G */
    Transliteration::new(0x0414, b'D', 0x00, 0x00, 0x00), /* Д to D */
    Transliteration::new(0x0415, b'E', 0x00, 0x00, 0x00), /* Е to E */
    Transliteration::new(0x0416, b'Z', b'h', 0x00, 0x00), /* Ж to Zh */
    Transliteration::new(0x0417, b'Z', 0x00, 0x00, 0x00), /* З to Z */
    Transliteration::new(0x0418, b'I', 0x00, 0x00, 0x00), /* И to I */
    Transliteration::new(0x0419, b'I', 0x00, 0x00, 0x00), /* Й to I */
    Transliteration::new(0x041A, b'K', 0x00, 0x00, 0x00), /* К to K */
    Transliteration::new(0x041B, b'L', 0x00, 0x00, 0x00), /* Л to L */
    Transliteration::new(0x041C, b'M', 0x00, 0x00, 0x00), /* М to M */
    Transliteration::new(0x041D, b'N', 0x00, 0x00, 0x00), /* Н to N */
    Transliteration::new(0x041E, b'O', 0x00, 0x00, 0x00), /* О to O */
    Transliteration::new(0x041F, b'P', 0x00, 0x00, 0x00), /* П to P */
    Transliteration::new(0x0420, b'R', 0x00, 0x00, 0x00), /* Р to R */
    Transliteration::new(0x0421, b'S', 0x00, 0x00, 0x00), /* С to S */
    Transliteration::new(0x0422, b'T', 0x00, 0x00, 0x00), /* Т to T */
    Transliteration::new(0x0423, b'U', 0x00, 0x00, 0x00), /* У to U */
    Transliteration::new(0x0424, b'F', 0x00, 0x00, 0x00), /* Ф to F */
    Transliteration::new(0x0425, b'K', b'h', 0x00, 0x00), /* Х to Kh */
    Transliteration::new(0x0426, b'T', b'c', 0x00, 0x00), /* Ц to Tc */
    Transliteration::new(0x0427, b'C', b'h', 0x00, 0x00), /* Ч to Ch */
    Transliteration::new(0x0428, b'S', b'h', 0x00, 0x00), /* Ш to Sh */
    Transliteration::new(0x0429, b'S', b'h', b'c', b'h'), /* Щ to Shch */
    Transliteration::new(0x042A, b'a', 0x00, 0x00, 0x00), /*  to A */
    Transliteration::new(0x042B, b'Y', 0x00, 0x00, 0x00), /* Ы to Y */
    Transliteration::new(0x042C, b'Y', 0x00, 0x00, 0x00), /*  to Y */
    Transliteration::new(0x042D, b'E', 0x00, 0x00, 0x00), /* Э to E */
    Transliteration::new(0x042E, b'I', b'u', 0x00, 0x00), /* Ю to Iu */
    Transliteration::new(0x042F, b'I', b'a', 0x00, 0x00), /* Я to Ia */
    Transliteration::new(0x0430, b'a', 0x00, 0x00, 0x00), /* а to a */
    Transliteration::new(0x0431, b'b', 0x00, 0x00, 0x00), /* б to b */
    Transliteration::new(0x0432, b'v', 0x00, 0x00, 0x00), /* в to v */
    Transliteration::new(0x0433, b'g', 0x00, 0x00, 0x00), /* г to g */
    Transliteration::new(0x0434, b'd', 0x00, 0x00, 0x00), /* д to d */
    Transliteration::new(0x0435, b'e', 0x00, 0x00, 0x00), /* е to e */
    Transliteration::new(0x0436, b'z', b'h', 0x00, 0x00), /* ж to zh */
    Transliteration::new(0x0437, b'z', 0x00, 0x00, 0x00), /* з to z */
    Transliteration::new(0x0438, b'i', 0x00, 0x00, 0x00), /* и to i */
    Transliteration::new(0x0439, b'i', 0x00, 0x00, 0x00), /* й to i */
    Transliteration::new(0x043A, b'k', 0x00, 0x00, 0x00), /* к to k */
    Transliteration::new(0x043B, b'l', 0x00, 0x00, 0x00), /* л to l */
    Transliteration::new(0x043C, b'm', 0x00, 0x00, 0x00), /* м to m */
    Transliteration::new(0x043D, b'n', 0x00, 0x00, 0x00), /* н to n */
    Transliteration::new(0x043E, b'o', 0x00, 0x00, 0x00), /* о to o */
    Transliteration::new(0x043F, b'p', 0x00, 0x00, 0x00), /* п to p */
    Transliteration::new(0x0440, b'r', 0x00, 0x00, 0x00), /* р to r */
    Transliteration::new(0x0441, b's', 0x00, 0x00, 0x00), /* с to s */
    Transliteration::new(0x0442, b't', 0x00, 0x00, 0x00), /* т to t */
    Transliteration::new(0x0443, b'u', 0x00, 0x00, 0x00), /* у to u */
    Transliteration::new(0x0444, b'f', 0x00, 0x00, 0x00), /* ф to f */
    Transliteration::new(0x0445, b'k', b'h', 0x00, 0x00), /* х to kh */
    Transliteration::new(0x0446, b't', b'c', 0x00, 0x00), /* ц to tc */
    Transliteration::new(0x0447, b'c', b'h', 0x00, 0x00), /* ч to ch */
    Transliteration::new(0x0448, b's', b'h', 0x00, 0x00), /* ш to sh */
    Transliteration::new(0x0449, b's', b'h', b'c', b'h'), /* щ to shch */
    Transliteration::new(0x044A, b'a', 0x00, 0x00, 0x00), /*  to a */
    Transliteration::new(0x044B, b'y', 0x00, 0x00, 0x00), /* ы to y */
    Transliteration::new(0x044C, b'y', 0x00, 0x00, 0x00), /*  to y */
    Transliteration::new(0x044D, b'e', 0x00, 0x00, 0x00), /* э to e */
    Transliteration::new(0x044E, b'i', b'u', 0x00, 0x00), /* ю to iu */
    Transliteration::new(0x044F, b'i', b'a', 0x00, 0x00), /* я to ia */
    Transliteration::new(0x0450, b'e', 0x00, 0x00, 0x00), /* ѐ to e */
    Transliteration::new(0x0451, b'e', 0x00, 0x00, 0x00), /* ё to e */
    Transliteration::new(0x0452, b'd', 0x00, 0x00, 0x00), /* ђ to d */
    Transliteration::new(0x0453, b'g', 0x00, 0x00, 0x00), /* ѓ to g */
    Transliteration::new(0x0454, b'e', 0x00, 0x00, 0x00), /* є to e */
    Transliteration::new(0x0455, b'z', 0x00, 0x00, 0x00), /* ѕ to z */
    Transliteration::new(0x0456, b'i', 0x00, 0x00, 0x00), /* і to i */
    Transliteration::new(0x0457, b'i', 0x00, 0x00, 0x00), /* ї to i */
    Transliteration::new(0x0458, b'j', 0x00, 0x00, 0x00), /* ј to j */
    Transliteration::new(0x0459, b'i', 0x00, 0x00, 0x00), /* љ to i */
    Transliteration::new(0x045A, b'n', 0x00, 0x00, 0x00), /* њ to n */
    Transliteration::new(0x045B, b'd', 0x00, 0x00, 0x00), /* ћ to d */
    Transliteration::new(0x045C, b'k', 0x00, 0x00, 0x00), /* ќ to k */
    Transliteration::new(0x045D, b'i', 0x00, 0x00, 0x00), /* ѝ to i */
    Transliteration::new(0x045E, b'u', 0x00, 0x00, 0x00), /* ў to u */
    Transliteration::new(0x045F, b'd', 0x00, 0x00, 0x00), /* џ to d */
    Transliteration::new(0x1E02, b'B', 0x00, 0x00, 0x00), /* Ḃ to B */
    Transliteration::new(0x1E03, b'b', 0x00, 0x00, 0x00), /* ḃ to b */
    Transliteration::new(0x1E0A, b'D', 0x00, 0x00, 0x00), /* Ḋ to D */
    Transliteration::new(0x1E0B, b'd', 0x00, 0x00, 0x00), /* ḋ to d */
    Transliteration::new(0x1E1E, b'F', 0x00, 0x00, 0x00), /* Ḟ to F */
    Transliteration::new(0x1E1F, b'f', 0x00, 0x00, 0x00), /* ḟ to f */
    Transliteration::new(0x1E40, b'M', 0x00, 0x00, 0x00), /* Ṁ to M */
    Transliteration::new(0x1E41, b'm', 0x00, 0x00, 0x00), /* ṁ to m */
    Transliteration::new(0x1E56, b'P', 0x00, 0x00, 0x00), /* Ṗ to P */
    Transliteration::new(0x1E57, b'p', 0x00, 0x00, 0x00), /* ṗ to p */
    Transliteration::new(0x1E60, b'S', 0x00, 0x00, 0x00), /* Ṡ to S */
    Transliteration::new(0x1E61, b's', 0x00, 0x00, 0x00), /* ṡ to s */
    Transliteration::new(0x1E6A, b'T', 0x00, 0x00, 0x00), /* Ṫ to T */
    Transliteration::new(0x1E6B, b't', 0x00, 0x00, 0x00), /* ṫ to t */
    Transliteration::new(0x1E80, b'W', 0x00, 0x00, 0x00), /* Ẁ to W */
    Transliteration::new(0x1E81, b'w', 0x00, 0x00, 0x00), /* ẁ to w */
    Transliteration::new(0x1E82, b'W', 0x00, 0x00, 0x00), /* Ẃ to W */
    Transliteration::new(0x1E83, b'w', 0x00, 0x00, 0x00), /* ẃ to w */
    Transliteration::new(0x1E84, b'W', 0x00, 0x00, 0x00), /* Ẅ to W */
    Transliteration::new(0x1E85, b'w', 0x00, 0x00, 0x00), /* ẅ to w */
    Transliteration::new(0x1EF2, b'Y', 0x00, 0x00, 0x00), /* Ỳ to Y */
    Transliteration::new(0x1EF3, b'y', 0x00, 0x00, 0x00), /* ỳ to y */
    Transliteration::new(0xFB00, b'f', b'f', 0x00, 0x00), /* ﬀ to ff */
    Transliteration::new(0xFB01, b'f', b'i', 0x00, 0x00), /* ﬁ to fi */
    Transliteration::new(0xFB02, b'f', b'l', 0x00, 0x00), /* ﬂ to fl */
    Transliteration::new(0xFB05, b's', b't', 0x00, 0x00), /* ﬅ to st */
    Transliteration::new(0xFB06, b's', b't', 0x00, 0x00), /* ﬆ to st */
];

/// Return the value of the first UTF-8 character in the string
fn utf8_read(z: &[u8]) -> (u32, usize) {
    if z.is_empty() {
        return (0, 0);
    }

    let first_byte = z[0];
    if first_byte < 0x80 {
        (first_byte as u32, 1)
    } else {
        let lookup_index = (first_byte - 0xc0) as usize;
        if lookup_index >= TRANSLIT_UTF8_LOOKUP.len() {
            return (first_byte as u32, 1);
        }

        let mut c = TRANSLIT_UTF8_LOOKUP[lookup_index] as u32;
        let mut i = 1;

        while i < z.len() && (z[i] & 0xc0) == 0x80 {
            c = (c << 6) + ((z[i] & 0x3f) as u32);
            i += 1;
        }

        (c, i)
    }
}

/// Find transliteration entry for a given Unicode character using binary search
fn find_translit(c: u32) -> Option<&'static Transliteration> {
    let c = c as u16; // Cast to u16 since our table uses u16
    TRANSLIT
        .binary_search_by_key(&c, |t| t.c_from)
        .ok()
        .map(|idx| &TRANSLIT[idx])
}

/// Convert the input string from UTF-8 into pure ASCII by converting
/// all non-ASCII characters to some combination of characters in the ASCII subset.
pub fn transliterate(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len() * 4);
    let mut pos = 0;

    while pos < input.len() {
        let (c, size) = utf8_read(&input[pos..]);
        pos += size;

        if c <= 127 {
            output.push(c as u8);
        } else if let Some(translit) = find_translit(c) {
            output.push(translit.c_to0);
            if translit.c_to1 != 0 {
                output.push(translit.c_to1);
                if translit.c_to2 != 0 {
                    output.push(translit.c_to2);
                    if translit.c_to3 != 0 {
                        output.push(translit.c_to3);
                    }
                }
            }
        } else {
            output.push(b'?');
        }
    }

    output
}

pub fn transliterate_str(input: &str) -> String {
    let result = transliterate(input.as_bytes());
    String::from_utf8(result).unwrap_or_else(|_| "?".to_string())
}

pub fn script_code(input: &[u8]) -> i32 {
    let mut pos = 0;
    let mut script_mask = 0;
    let mut seen_digit = false;

    while pos < input.len() {
        let (c, size) = utf8_read(&input[pos..]);
        pos += size;

        if c < 0x02af {
            if c >= 0x80 {
                script_mask |= SCRIPT_LATIN;
            } else if (c as u8).is_ascii_digit() {
                seen_digit = true;
            } else {
                script_mask |= SCRIPT_LATIN;
            }
        } else if (0x0400..=0x04ff).contains(&c) {
            script_mask |= SCRIPT_CYRILLIC;
        } else if (0x0386..=0x03ce).contains(&c) {
            script_mask |= SCRIPT_GREEK;
        } else if (0x0590..=0x05ff).contains(&c) {
            script_mask |= SCRIPT_HEBREW;
        } else if (0x0600..=0x06ff).contains(&c) {
            script_mask |= SCRIPT_ARABIC;
        }
    }

    if script_mask == 0 && seen_digit {
        script_mask = SCRIPT_LATIN;
    }

    match script_mask {
        0 => 999,
        SCRIPT_LATIN => 215,
        SCRIPT_CYRILLIC => 220,
        SCRIPT_GREEK => 200,
        SCRIPT_HEBREW => 125,
        SCRIPT_ARABIC => 160,
        _ => 998,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_read() {
        let input = "Café".as_bytes();
        let (c, size) = utf8_read(&input[0..]);
        assert_eq!(c, b'C' as u32);
        assert_eq!(size, 1);
        let (c, size) = utf8_read(&input[3..]);
        assert_eq!(c, 0x00E9); // é
        assert_eq!(size, 2);
    }

    #[test]
    fn test_transliterate_basic() {
        let result = transliterate_str("Café");
        assert_eq!(result, "Cafe");
        let result = transliterate_str("Naïve");
        assert_eq!(result, "Naive");
    }

    #[test]
    fn test_transliterate_german() {
        let result = transliterate_str("Müller");
        assert_eq!(result, "Mueller");
        let result = transliterate_str("Größe");
        assert_eq!(result, "Groesse");
    }

    #[test]
    fn test_script_code() {
        assert_eq!(script_code("Hello".as_bytes()), 215);
        assert_eq!(script_code("123".as_bytes()), 215);
        assert_eq!(script_code("привет".as_bytes()), 220);
        assert_eq!(script_code("γειά".as_bytes()), 200);
        assert_eq!(script_code("helloпривет".as_bytes()), 998);
    }
}
