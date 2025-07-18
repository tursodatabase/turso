use crate::json::error::{Error as PError, Result as PResult};
use crate::{bail_parse_error, LimboError, Result};
use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    fmt::Write,
    str::{from_utf8, from_utf8_unchecked},
};

use super::path::{JsonPath, PathElement};

const SIZE_MARKER_8BIT: u8 = 12;
const SIZE_MARKER_16BIT: u8 = 13;
const SIZE_MARKER_32BIT: u8 = 14;
const MAX_JSON_DEPTH: usize = 1000;
const INFINITY_CHAR_COUNT: u8 = 5;

const fn make_whitespace_table() -> [u8; 256] {
    let mut table = [0u8; 256];

    // Mark whitespace characters
    table[0x09] = 1; // Tab
    table[0x0A] = 1; // Line feed
    table[0x0D] = 1; // Carriage return
    table[0x20] = 1; // Space

    table
}

static WS_TABLE: [u8; 256] = make_whitespace_table();

const fn make_character_type_table() -> [u8; 256] {
    let mut table = [0u8; 256];

    // Mark whitespace characters
    table[0x09] = 1; // Tab
    table[0x0A] = 1; // Line feed
    table[0x0D] = 1; // Carriage return
    table[0x20] = 1; // Space

    // Mark numeric digits
    table[0x30] = 2; // 0
    table[0x31] = 2; // 1
    table[0x32] = 2; // 2
    table[0x33] = 2; // 3
    table[0x34] = 2; // 4
    table[0x35] = 2; // 5
    table[0x36] = 2; // 6
    table[0x37] = 2; // 7
    table[0x38] = 2; // 8
    table[0x39] = 2; // 9

    // Mark hex digits (a-f, A-F)
    table[0x41] = 3; // A
    table[0x42] = 3; // B
    table[0x43] = 3; // C
    table[0x44] = 3; // D
    table[0x45] = 3; // E
    table[0x46] = 3; // F
    table[0x61] = 3; // a
    table[0x62] = 3; // b
    table[0x63] = 3; // c
    table[0x64] = 3; // d
    table[0x65] = 3; // e
    table[0x66] = 3; // f

    table
}

static CHARACTER_TYPE: [u8; 256] = make_character_type_table();

const fn make_character_type_ok_table() -> [u8; 256] {
    let mut table = [0u8; 256];

    table[0x20] |= 4; // Space
    table[0x21] |= 4; // !
                      // Skipping 0x22 (") as it needs escaping
    table[0x23] |= 4; // #
    table[0x24] |= 4; // $
    table[0x25] |= 4; // %
    table[0x26] |= 4; // &
    table[0x27] |= 4; // '
    table[0x28] |= 4; // (
    table[0x29] |= 4; // )
    table[0x2A] |= 4; // *
    table[0x2B] |= 4; // +
    table[0x2C] |= 4; // ,
    table[0x2D] |= 4; // -
    table[0x2E] |= 4; // .
    table[0x2F] |= 4; // /
    table[0x30] |= 4; // 0
    table[0x31] |= 4; // 1
    table[0x32] |= 4; // 2
    table[0x33] |= 4; // 3
    table[0x34] |= 4; // 4
    table[0x35] |= 4; // 5
    table[0x36] |= 4; // 6
    table[0x37] |= 4; // 7
    table[0x38] |= 4; // 8
    table[0x39] |= 4; // 9
    table[0x3A] |= 4; // :
    table[0x3B] |= 4; // ;
    table[0x3C] |= 4; //
    table[0x3D] |= 4; // =
    table[0x3E] |= 4; // >
    table[0x3F] |= 4; // ?
    table[0x40] |= 4; // @
    table[0x41] |= 4; // A
    table[0x42] |= 4; // B
    table[0x43] |= 4; // C
    table[0x44] |= 4; // D
    table[0x45] |= 4; // E
    table[0x46] |= 4; // F
    table[0x47] |= 4; // G
    table[0x48] |= 4; // H
    table[0x49] |= 4; // I
    table[0x4A] |= 4; // J
    table[0x4B] |= 4; // K
    table[0x4C] |= 4; // L
    table[0x4D] |= 4; // M
    table[0x4E] |= 4; // N
    table[0x4F] |= 4; // O
    table[0x50] |= 4; // P
    table[0x51] |= 4; // Q
    table[0x52] |= 4; // R
    table[0x53] |= 4; // S
    table[0x54] |= 4; // T
    table[0x55] |= 4; // U
    table[0x56] |= 4; // V
    table[0x57] |= 4; // W
    table[0x58] |= 4; // X
    table[0x59] |= 4; // Y
    table[0x5A] |= 4; // Z
    table[0x5B] |= 4; // [
                      // Skipping 0x5C (\) as it needs escaping
    table[0x5D] |= 4; // ]
    table[0x5E] |= 4; // ^
    table[0x5F] |= 4; // _
    table[0x60] |= 4; // `
    table[0x61] |= 4; // a
    table[0x62] |= 4; // b
    table[0x63] |= 4; // c
    table[0x64] |= 4; // d
    table[0x65] |= 4; // e
    table[0x66] |= 4; // f
    table[0x67] |= 4; // g
    table[0x68] |= 4; // h
    table[0x69] |= 4; // i
    table[0x6A] |= 4; // j
    table[0x6B] |= 4; // k
    table[0x6C] |= 4; // l
    table[0x6D] |= 4; // m
    table[0x6E] |= 4; // n
    table[0x6F] |= 4; // o
    table[0x70] |= 4; // p
    table[0x71] |= 4; // q
    table[0x72] |= 4; // r
    table[0x73] |= 4; // s
    table[0x74] |= 4; // t
    table[0x75] |= 4; // u
    table[0x76] |= 4; // v
    table[0x77] |= 4; // w
    table[0x78] |= 4; // x
    table[0x79] |= 4; // y
    table[0x7A] |= 4; // z
    table[0x7B] |= 4; // {
    table[0x7C] |= 4; // |
    table[0x7D] |= 4; // }
    table[0x7E] |= 4; // ~

    table
}

static CHARACTER_TYPE_OK: [u8; 256] = make_character_type_ok_table();

#[derive(Debug, Clone, PartialEq)]
pub struct Jsonb {
    data: Vec<u8>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names, clippy::upper_case_acronyms)]
pub enum ElementType {
    NULL = 0,
    TRUE = 1,
    FALSE = 2,
    INT = 3,
    INT5 = 4,
    FLOAT = 5,
    FLOAT5 = 6,
    TEXT = 7,
    TEXTJ = 8,
    TEXT5 = 9,
    TEXTRAW = 10,
    ARRAY = 11,
    OBJECT = 12,
    RESERVED1 = 13,
    RESERVED2 = 14,
    RESERVED3 = 15,
}

pub enum JsonIndentation<'a> {
    Indentation(Cow<'a, str>),
    None,
}

impl JsonIndentation<'_> {
    pub fn is_pretty(&self) -> bool {
        match self {
            Self::Indentation(_) => true,
            Self::None => false,
        }
    }
}

impl ElementType {
    pub fn is_valid_key(&self) -> bool {
        matches!(self, Self::TEXT | Self::TEXT5 | Self::TEXTJ | Self::TEXTRAW)
    }
}

impl From<ElementType> for String {
    fn from(element_type: ElementType) -> String {
        match element_type {
            ElementType::ARRAY => "array".to_string(),
            ElementType::OBJECT => "object".to_string(),
            ElementType::NULL => "null".to_string(),
            ElementType::TRUE => "true".to_string(),
            ElementType::FALSE => "false".to_string(),
            ElementType::FLOAT | ElementType::FLOAT5 => "real".to_string(),
            ElementType::INT | ElementType::INT5 => "integer".to_string(),
            ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
                "text".to_string()
            }
            _ => unreachable!(),
        }
    }
}

impl TryFrom<u8> for ElementType {
    type Error = LimboError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::NULL),
            1 => Ok(Self::TRUE),
            2 => Ok(Self::FALSE),
            3 => Ok(Self::INT),
            4 => Ok(Self::INT5),
            5 => Ok(Self::FLOAT),
            6 => Ok(Self::FLOAT5),
            7 => Ok(Self::TEXT),
            8 => Ok(Self::TEXTJ),
            9 => Ok(Self::TEXT5),
            10 => Ok(Self::TEXTRAW),
            11 => Ok(Self::ARRAY),
            12 => Ok(Self::OBJECT),
            13 => Ok(Self::RESERVED1),
            14 => Ok(Self::RESERVED2),
            15 => Ok(Self::RESERVED3),
            _ => bail_parse_error!("Failed to recognize jsonvalue type"),
        }
    }
}

type PayloadSize = usize;

#[derive(Debug, Clone)]
pub enum ArrayPositionKind {
    SpecificIndex(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonLocationKind {
    ObjectProperty(usize),
    DocumentRoot,
    ArrayEntry,
}

#[derive(Debug, Clone)]
pub struct JsonTraversalResult {
    field_key_index: JsonLocationKind,
    pub field_value_index: usize,
    delta: isize,
    array_position_info: Option<ArrayPositionKind>,
}

#[derive(Debug, Clone)]
pub enum SegmentVariant<'a> {
    Single(&'a PathElement<'a>),
    KeyWithArrayIndex(&'a PathElement<'a>, &'a PathElement<'a>),
}

pub trait PathOperation {
    // Get the operation mode for this operation
    fn operation_mode(&self) -> PathOperationMode;

    // Execute the actual operation
    fn execute(&mut self, json: &mut Jsonb, stack: Vec<JsonTraversalResult>) -> Result<()>;

    // Name of the operation for logging/debugging
}

pub struct SetOperation {
    value: Jsonb,
    mode: PathOperationMode,
}

impl SetOperation {
    pub fn new(value: Jsonb) -> Self {
        Self {
            value,
            mode: PathOperationMode::Upsert,
        }
    }
}

impl PathOperation for SetOperation {
    fn operation_mode(&self) -> PathOperationMode {
        self.mode
    }

    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<JsonTraversalResult>) -> Result<()> {
        if stack.is_empty() {
            bail_parse_error!("Nothing to operate on!")
        }
        let value = &self.value.data;
        let target = stack.pop().unwrap();

        // handle array
        if target.has_specific_index() {
            let array_value_idx = target.get_array_index().unwrap();
            let obj_value_idx = target.field_value_index;
            let (JsonbHeader(_, obj_value_size), obj_value_header_size) =
                json.read_header(obj_value_idx)?;
            let (JsonbHeader(_, array_value_size), array_value_header_size) =
                json.read_header(array_value_idx)?;

            let delta =
                value.len() as isize - (array_value_size + array_value_header_size) as isize;

            let end_pos = array_value_idx + array_value_size + array_value_header_size;
            json.data
                .splice(array_value_idx..end_pos, value.iter().copied());

            // update parent
            let h_delta = if matches!(
                target.field_key_index,
                JsonLocationKind::ObjectProperty(_) | JsonLocationKind::DocumentRoot
            ) {
                let new_h_delta = json.write_element_header(
                    obj_value_idx,
                    ElementType::ARRAY,
                    (obj_value_size as isize + delta) as usize,
                    true,
                )?;
                (new_h_delta - obj_value_header_size) as isize
            } else {
                0
            };

            json.update_parent_references(stack, target.delta + delta + h_delta)?;
        } else {
            let old_value_idx = target.field_value_index;
            let (JsonbHeader(_, old_value_size), old_value_header_size) =
                json.read_header(old_value_idx)?;
            let delta = value.len() as isize - (old_value_header_size + old_value_size) as isize;

            let end_pos = old_value_idx + old_value_header_size + old_value_size;

            json.data
                .splice(old_value_idx..end_pos, value.iter().copied());

            json.update_parent_references(stack, delta + target.delta)?;
        }

        Ok(())
    }
}

pub struct DeleteOperation {
    mode: PathOperationMode,
}

impl DeleteOperation {
    pub fn new() -> Self {
        Self {
            mode: PathOperationMode::ReplaceExisting,
        }
    }
}

impl PathOperation for DeleteOperation {
    fn operation_mode(&self) -> PathOperationMode {
        self.mode
    }

    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<JsonTraversalResult>) -> Result<()> {
        if stack.is_empty() {
            bail_parse_error!("Nothing to operate on!")
        }

        let target = stack.pop().unwrap();

        // handle array
        if target.has_specific_index() {
            let array_value_idx = target.get_array_index().unwrap();

            let obj_value_idx = target.field_value_index;
            let (JsonbHeader(_, obj_value_size), obj_value_header_size) =
                json.read_header(obj_value_idx)?;
            let (JsonbHeader(_, array_value_size), array_value_header_size) =
                json.read_header(array_value_idx)?;
            let delta = 0 - (array_value_size + array_value_header_size) as isize;

            let end_pos = array_value_idx + array_value_size + array_value_header_size;
            json.data.drain(array_value_idx..end_pos);

            let h_delta = if matches!(
                target.field_key_index,
                JsonLocationKind::ObjectProperty(_) | JsonLocationKind::DocumentRoot
            ) {
                let new_h_delta = json.write_element_header(
                    obj_value_idx,
                    ElementType::ARRAY,
                    (obj_value_size as isize + delta) as usize,
                    true,
                )?;
                new_h_delta as isize - obj_value_header_size as isize
            } else {
                0
            };
            json.update_parent_references(stack, target.delta + delta + h_delta)?;
        } else if let JsonLocationKind::ObjectProperty(key_idx) = target.field_key_index {
            let value_idx = target.field_value_index;
            let (JsonbHeader(_, value_size), value_header_size) = json.read_header(value_idx)?;
            let (JsonbHeader(_, key_size), key_header_size) = json.read_header(key_idx)?;
            let delta = 0 - (value_header_size + value_size + key_size + key_header_size) as isize;

            let end_pos = key_idx + value_header_size + value_size + key_size + key_header_size;
            json.data.drain(key_idx..end_pos);

            json.update_parent_references(stack, delta + target.delta)?;
        } else {
            let nul = JsonbHeader::make_null().into_bytes();
            let nul_bytes = nul.as_bytes();
            json.data.clear();
            json.data.extend_from_slice(nul_bytes);
        }

        Ok(())
    }
}

pub struct ReplaceOperation {
    value: Jsonb,
    mode: PathOperationMode,
}

impl ReplaceOperation {
    pub fn new(value: Jsonb) -> Self {
        Self {
            value,
            mode: PathOperationMode::ReplaceExisting,
        }
    }
}

impl PathOperation for ReplaceOperation {
    fn operation_mode(&self) -> PathOperationMode {
        self.mode
    }

    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<JsonTraversalResult>) -> Result<()> {
        if stack.is_empty() {
            bail_parse_error!("Nothing to operate on!")
        }
        let value = &self.value.data;
        let target = stack.pop().unwrap();

        // handle array
        if target.has_specific_index() {
            let array_value_idx = target.get_array_index().unwrap();
            let obj_value_idx = target.field_value_index;
            let (JsonbHeader(_, obj_value_size), obj_value_header_size) =
                json.read_header(obj_value_idx)?;
            let (JsonbHeader(_, array_value_size), array_value_header_size) =
                json.read_header(array_value_idx)?;

            let delta =
                value.len() as isize - (array_value_size + array_value_header_size) as isize;

            let end_pos = array_value_idx + array_value_size + array_value_header_size;
            json.data
                .splice(array_value_idx..end_pos, value.iter().copied());

            // update parent
            let h_delta = if matches!(
                target.field_key_index,
                JsonLocationKind::ObjectProperty(_) | JsonLocationKind::DocumentRoot
            ) {
                let new_h_delta = json.write_element_header(
                    obj_value_idx,
                    ElementType::ARRAY,
                    (obj_value_size as isize + delta) as usize,
                    true,
                )?;
                (new_h_delta - obj_value_header_size) as isize
            } else {
                0
            };

            json.update_parent_references(stack, target.delta + delta + h_delta)?;
        } else {
            let old_value_idx = target.field_value_index;
            let (JsonbHeader(_, old_value_size), old_value_header_size) =
                json.read_header(old_value_idx)?;
            let delta = value.len() as isize - (old_value_header_size + old_value_size) as isize;

            let end_pos = old_value_idx + old_value_header_size + old_value_size;

            json.data
                .splice(old_value_idx..end_pos, value.iter().copied());

            json.update_parent_references(stack, delta + target.delta)?;
        }

        Ok(())
    }
}

pub struct InsertOperation {
    value: Jsonb,
    mode: PathOperationMode,
}

impl InsertOperation {
    pub fn new(value: Jsonb) -> Self {
        Self {
            value,
            mode: PathOperationMode::InsertNew,
        }
    }
}

impl PathOperation for InsertOperation {
    fn operation_mode(&self) -> PathOperationMode {
        self.mode
    }

    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<JsonTraversalResult>) -> Result<()> {
        if stack.is_empty() {
            bail_parse_error!("Nothing to operate on!")
        }
        let value = &self.value.data;
        let target = stack.pop().unwrap();

        // handle array
        if target.has_specific_index() {
            let array_value_idx = target.get_array_index().unwrap();
            let obj_value_idx = target.field_value_index;
            let (JsonbHeader(_, obj_value_size), obj_value_header_size) =
                json.read_header(obj_value_idx)?;
            let (JsonbHeader(_, array_value_size), array_value_header_size) =
                json.read_header(array_value_idx)?;

            let delta =
                value.len() as isize - (array_value_size + array_value_header_size) as isize;

            let end_pos = array_value_idx + array_value_size + array_value_header_size;
            json.data
                .splice(array_value_idx..end_pos, value.iter().copied());

            // update parent
            let h_delta = if matches!(
                target.field_key_index,
                JsonLocationKind::ObjectProperty(_) | JsonLocationKind::DocumentRoot
            ) {
                let new_h_delta = json.write_element_header(
                    obj_value_idx,
                    ElementType::ARRAY,
                    (obj_value_size as isize + delta) as usize,
                    true,
                )?;
                (new_h_delta - obj_value_header_size) as isize
            } else {
                0
            };

            json.update_parent_references(stack, target.delta + delta + h_delta)?;
        } else {
            let old_value_idx = target.field_value_index;
            let (JsonbHeader(_, old_value_size), old_value_header_size) =
                json.read_header(old_value_idx)?;
            let delta = value.len() as isize - (old_value_header_size + old_value_size) as isize;

            let end_pos = old_value_idx + old_value_header_size + old_value_size;

            json.data
                .splice(old_value_idx..end_pos, value.iter().copied());

            json.update_parent_references(stack, delta + target.delta)?;
        }

        Ok(())
    }
}

pub struct SearchOperation {
    value: Jsonb,
    mode: PathOperationMode,
}

impl SearchOperation {
    pub fn new(capacity: usize) -> Self {
        Self {
            mode: PathOperationMode::ReplaceExisting,
            value: Jsonb::new(capacity, None),
        }
    }

    pub fn result(self) -> Jsonb {
        self.value
    }
}

impl PathOperation for SearchOperation {
    fn operation_mode(&self) -> PathOperationMode {
        self.mode
    }

    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<JsonTraversalResult>) -> Result<()> {
        let target = stack.pop().unwrap();
        let idx = if let Some(idx) = target.get_array_index() {
            idx
        } else {
            target.field_value_index
        };
        let (JsonbHeader(_, size), header_size) = json.read_header(idx)?;
        self.value
            .data
            .extend_from_slice(&json.data[idx..idx + header_size + size]);

        Ok(())
    }
}

impl JsonTraversalResult {
    pub fn new(field_value_index: usize, field_key_index: JsonLocationKind, delta: isize) -> Self {
        Self {
            field_value_index,
            delta,
            field_key_index,
            array_position_info: None,
        }
    }

    pub fn with_array_index(
        field_value_index: usize,
        field_key_index: JsonLocationKind,
        delta: isize,
        index: usize,
    ) -> Self {
        Self {
            field_value_index,
            field_key_index,
            delta,
            array_position_info: Some(ArrayPositionKind::SpecificIndex(index)),
        }
    }

    pub fn has_specific_index(&self) -> bool {
        matches!(
            self.array_position_info,
            Some(ArrayPositionKind::SpecificIndex(_))
        )
    }

    pub fn get_array_index(&self) -> Option<usize> {
        match self.array_position_info {
            Some(ArrayPositionKind::SpecificIndex(idx)) => Some(idx),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathOperationMode {
    /// Only replace values if the complete path already exists
    ReplaceExisting,

    /// Only insert values if the path doesn't exist yet
    InsertNew,

    /// Either replace existing values or create new ones as needed
    Upsert,
}

impl PathOperationMode {
    /// Returns true if this mode allows replacing existing values
    pub fn allows_replace(&self) -> bool {
        matches!(self, Self::ReplaceExisting | Self::Upsert)
    }

    /// Returns true if this mode allows creating new paths
    pub fn allows_insert(&self) -> bool {
        matches!(self, Self::InsertNew | Self::Upsert)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JsonbHeader(ElementType, PayloadSize);

pub(crate) enum HeaderFormat {
    Inline([u8; 1]),    // Small payloads embedded directly in the header
    OneByte([u8; 2]),   // Medium payloads with 1-byte size field
    TwoBytes([u8; 3]),  // Large payloads with 2-byte size field
    FourBytes([u8; 5]), // Extra large payloads with 4-byte size field
}

impl HeaderFormat {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::OneByte(bytes) => bytes,
            Self::TwoBytes(bytes) => bytes,
            Self::FourBytes(bytes) => bytes,
        }
    }
}

impl JsonbHeader {
    fn new(element_type: ElementType, payload_size: PayloadSize) -> Self {
        Self(element_type, payload_size)
    }

    pub fn make_null() -> Self {
        Self(ElementType::NULL, 0)
    }

    pub fn make_obj() -> Self {
        Self(ElementType::OBJECT, 0)
    }

    fn from_slice(cursor: usize, slice: &[u8]) -> Result<(Self, usize)> {
        match slice.get(cursor) {
            Some(header_byte) => {
                // Extract first 4 bits (values 0-15)
                let element_type = header_byte & 15;
                if element_type > 12 {
                    bail_parse_error!("Invalid element type: {}", element_type);
                }
                // Get the last 4 bits for header_size
                let header_size = header_byte >> 4;
                let offset: usize;
                let total_size = match header_size {
                    size if size <= 11 => {
                        offset = 1;
                        size as usize
                    }

                    12 => match slice.get(cursor + 1) {
                        Some(value) => {
                            offset = 2;
                            *value as usize
                        }
                        None => bail_parse_error!("Failed to read 1-byte size"),
                    },

                    13 => match Self::get_size_bytes(slice, cursor + 1, 2) {
                        Ok(bytes) => {
                            offset = 3;
                            u16::from_be_bytes([bytes[0], bytes[1]]) as usize
                        }
                        Err(e) => return Err(e),
                    },

                    14 => match Self::get_size_bytes(slice, cursor + 1, 4) {
                        Ok(bytes) => {
                            offset = 5;
                            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
                        }
                        Err(e) => return Err(e),
                    },

                    _ => unreachable!(),
                };

                Ok((Self(element_type.try_into()?, total_size), offset))
            }
            None => bail_parse_error!("Failed to read header byte"),
        }
    }

    pub fn into_bytes(self) -> HeaderFormat {
        let (element_type, payload_size) = (self.0, self.1);

        match payload_size {
            // Small payload (fits in 4 bits)
            size if size <= 11 => {
                HeaderFormat::Inline([(element_type as u8) | ((size as u8) << 4)])
            }

            // Medium payload (fits in 1 byte)
            size if size <= 0xFF => {
                HeaderFormat::OneByte([(element_type as u8) | (SIZE_MARKER_8BIT << 4), size as u8])
            }

            // Large payload (fits in 2 bytes)
            size if size <= 0xFFFF => {
                let size_bytes = (size as u16).to_be_bytes();
                HeaderFormat::TwoBytes([
                    (element_type as u8) | (SIZE_MARKER_16BIT << 4),
                    size_bytes[0],
                    size_bytes[1],
                ])
            }

            // Extra large payload (fits in 4 bytes)
            size if size <= 0xFFFFFFFF => {
                let size_bytes = (size as u32).to_be_bytes();
                HeaderFormat::FourBytes([
                    (element_type as u8) | (SIZE_MARKER_32BIT << 4),
                    size_bytes[0],
                    size_bytes[1],
                    size_bytes[2],
                    size_bytes[3],
                ])
            }

            // Payload too large
            _ => panic!("Payload size too large for encoding"),
        }
    }

    fn get_size_bytes(slice: &[u8], start: usize, count: usize) -> Result<&[u8]> {
        match slice.get(start..start + count) {
            Some(bytes) => Ok(bytes),
            None => bail_parse_error!("Failed to read header size"),
        }
    }
}

impl Jsonb {
    pub fn new(capacity: usize, data: Option<&[u8]>) -> Self {
        if let Some(data) = data {
            return Self {
                data: data.to_vec(),
            };
        }
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn make_empty_array(size: usize) -> Self {
        let mut jsonb = Self {
            data: Vec::with_capacity(size),
        };
        jsonb
            .write_element_header(0, ElementType::ARRAY, 0, false)
            .unwrap();
        jsonb
    }

    pub fn make_empty_obj(size: usize) -> Self {
        let mut jsonb = Self {
            data: Vec::with_capacity(size),
        };
        jsonb
            .write_element_header(0, ElementType::OBJECT, 0, false)
            .unwrap();
        jsonb
    }

    pub fn append_to_array_unsafe(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn append_jsonb_to_end(&mut self, mut data: Vec<u8>) {
        self.data.append(&mut data);
    }

    pub fn finalize_unsafe(&mut self, element_type: ElementType) -> Result<()> {
        self.write_element_header(0, element_type, self.len() - 1, false)?;
        Ok(())
    }

    fn read_header(&self, cursor: usize) -> Result<(JsonbHeader, usize)> {
        let (header, offset) = JsonbHeader::from_slice(cursor, &self.data)?;

        Ok((header, offset))
    }

    pub fn is_valid(&self) -> Result<ElementType> {
        match self.read_header(0) {
            Ok((header, offset)) => {
                if self.data.get(offset..offset + header.1).is_some() {
                    Ok(header.0)
                } else {
                    bail_parse_error!("malformed JSON")
                }
            }
            Err(_) => bail_parse_error!("malformed JSON"),
        }
    }

    pub fn to_string(&self) -> Result<String> {
        let mut result = String::with_capacity(self.data.len() * 2);

        self.write_to_string(&mut result, JsonIndentation::None)?;

        Ok(result)
    }

    pub fn to_string_pretty(&self, indentation: Option<&str>) -> Result<String> {
        let mut result = String::with_capacity(self.data.len() * 2);
        let ind = if let Some(ind) = indentation {
            JsonIndentation::Indentation(Cow::Borrowed(ind))
        } else {
            JsonIndentation::Indentation(Cow::Borrowed("    "))
        };
        self.write_to_string(&mut result, ind)?;

        Ok(result)
    }

    fn write_to_string(&self, string: &mut String, indentation: JsonIndentation) -> Result<()> {
        let cursor = 0;
        let ind = indentation;
        let _ = self.serialize_value(string, cursor, 0, &ind);
        Ok(())
    }

    fn serialize_value(
        &self,
        string: &mut String,
        cursor: usize,
        depth: usize,
        delimiter: &JsonIndentation,
    ) -> Result<usize> {
        let (header, skip_header) = self.read_header(cursor)?;

        let cursor = cursor + skip_header;
        let current_cursor = match header {
            JsonbHeader(ElementType::OBJECT, len) => {
                self.serialize_object(string, cursor, len, depth, delimiter)?
            }
            JsonbHeader(ElementType::ARRAY, len) => {
                self.serialize_array(string, cursor, len, depth, delimiter)?
            }
            JsonbHeader(ElementType::TEXT, len)
            | JsonbHeader(ElementType::TEXTRAW, len)
            | JsonbHeader(ElementType::TEXTJ, len)
            | JsonbHeader(ElementType::TEXT5, len) => {
                self.serialize_string(string, cursor, len, &header.0, true)?
            }
            JsonbHeader(ElementType::INT, len)
            | JsonbHeader(ElementType::INT5, len)
            | JsonbHeader(ElementType::FLOAT, len)
            | JsonbHeader(ElementType::FLOAT5, len) => {
                self.serialize_number(string, cursor, len, &header.0)?
            }

            JsonbHeader(ElementType::TRUE, _) => self.serialize_boolean(string, cursor, true),
            JsonbHeader(ElementType::FALSE, _) => self.serialize_boolean(string, cursor, false),
            JsonbHeader(ElementType::NULL, _) => self.serialize_null(string, cursor),
            JsonbHeader(_, _) => {
                unreachable!();
            }
        };
        Ok(current_cursor)
    }

    fn serialize_object(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        mut depth: usize,
        indent: &JsonIndentation,
    ) -> Result<usize> {
        let end_cursor = cursor + len;
        let mut current_cursor = cursor;
        depth += 1;
        string.push('{');
        if indent.is_pretty() {
            string.push('\n');
        };
        while current_cursor < end_cursor {
            let (key_header, key_header_offset) = self.read_header(current_cursor)?;
            current_cursor += key_header_offset;
            let JsonbHeader(element_type, len) = key_header;
            if let JsonIndentation::Indentation(value) = indent {
                for _ in 0..depth {
                    string.push_str(value);
                }
            };
            match element_type {
                ElementType::TEXT
                | ElementType::TEXTRAW
                | ElementType::TEXTJ
                | ElementType::TEXT5 => {
                    current_cursor =
                        self.serialize_string(string, current_cursor, len, &element_type, true)?;
                }
                _ => bail_parse_error!("malformed JSON"),
            }

            string.push(':');
            if indent.is_pretty() {
                string.push(' ');
            }
            current_cursor = self.serialize_value(string, current_cursor, depth, indent)?;
            if current_cursor < end_cursor {
                string.push(',');
            }

            if indent.is_pretty() {
                string.push('\n');
            };
        }
        if let JsonIndentation::Indentation(value) = indent {
            for _ in 0..depth - 1 {
                string.push_str(value);
            }
        };
        string.push('}');

        Ok(current_cursor)
    }

    fn serialize_array(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        mut depth: usize,
        indent: &JsonIndentation,
    ) -> Result<usize> {
        let end_cursor = cursor + len;
        let mut current_cursor = cursor;
        depth += 1;
        string.push('[');
        if indent.is_pretty() {
            string.push('\n');
        };
        while current_cursor < end_cursor {
            if let JsonIndentation::Indentation(value) = indent {
                for _ in 0..depth {
                    string.push_str(value);
                }
            };
            current_cursor = self.serialize_value(string, current_cursor, depth, indent)?;
            if current_cursor < end_cursor {
                string.push(',');
            }
            if indent.is_pretty() {
                string.push('\n');
            };
        }
        if let JsonIndentation::Indentation(value) = indent {
            for _ in 0..depth - 1 {
                string.push_str(value);
            }
        };
        string.push(']');

        Ok(current_cursor)
    }

    fn serialize_string(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
        quote: bool,
    ) -> Result<usize> {
        let word_slice = &self.data[cursor..cursor + len];
        if quote {
            string.push('"');
        }

        match kind {
            // Can be serialized as is. Do not need escaping
            ElementType::TEXT => {
                let word = from_utf8(word_slice).map_err(|_| {
                    LimboError::ParseError("Failed to serialize string!".to_string())
                })?;
                string.push_str(word);
            }

            // Contain standard json escapes
            ElementType::TEXTJ => {
                let word = from_utf8(word_slice).map_err(|_| {
                    LimboError::ParseError("Failed to serialize string!".to_string())
                })?;
                string.push_str(word);
            }

            // We have to escape some JSON5 escape sequences
            ElementType::TEXT5 => {
                let mut i = 0;
                while i < word_slice.len() {
                    let ch = word_slice[i];

                    // Handle normal characters that don't need escaping
                    if is_json_ok(ch) || ch == b'\'' {
                        string.push(ch as char);
                        i += 1;
                        continue;
                    }

                    // Handle special cases
                    match ch {
                        // Double quotes need escaping
                        b'"' => {
                            string.push_str("\\\"");
                            i += 1;
                        }

                        // Control characters (0x00-0x1F)
                        ch if ch <= 0x1F => {
                            match ch {
                                // \b
                                0x08 => string.push_str("\\b"),
                                b'\t' => string.push_str("\\t"),
                                b'\n' => string.push_str("\\n"),
                                // \f
                                0x0C => string.push_str("\\f"),
                                b'\r' => string.push_str("\\r"),
                                _ => {
                                    // Format as \u00XX
                                    let hex = format!("\\u{ch:04x}");
                                    string.push_str(&hex);
                                }
                            }
                            i += 1;
                        }

                        // Handle escape sequences
                        b'\\' if i + 1 < word_slice.len() => {
                            let next_ch = word_slice[i + 1];
                            match next_ch {
                                // Single quote
                                b'\'' => {
                                    string.push('\'');
                                    i += 2;
                                }

                                // Vertical tab
                                b'v' => {
                                    string.push_str("\\u0009");
                                    i += 2;
                                }

                                // Hex escapes like \x27
                                b'x' if i + 3 < word_slice.len() => {
                                    string.push_str("\\u00");
                                    string.push(word_slice[i + 2] as char);
                                    string.push(word_slice[i + 3] as char);
                                    i += 4;
                                }

                                // Null character
                                b'0' => {
                                    string.push_str("\\u0000");
                                    i += 2;
                                }

                                // CR line continuation
                                b'\r' => {
                                    if i + 2 < word_slice.len() && word_slice[i + 2] == b'\n' {
                                        i += 3; // Skip CRLF
                                    } else {
                                        i += 2; // Skip CR
                                    }
                                }

                                // LF line continuation
                                b'\n' => {
                                    i += 2;
                                }

                                // Unicode line separators (U+2028 and U+2029)
                                0xe2 if i + 3 < word_slice.len()
                                    && word_slice[i + 2] == 0x80
                                    && (word_slice[i + 3] == 0xa8 || word_slice[i + 3] == 0xa9) =>
                                {
                                    i += 4;
                                }

                                // All other escapes pass through
                                _ => {
                                    string.push('\\');
                                    string.push(next_ch as char);
                                    i += 2;
                                }
                            }
                        }

                        // Default case - just push the character
                        _ => {
                            string.push(ch as char);
                            i += 1;
                        }
                    }
                }
            }

            ElementType::TEXTRAW => {
                let word = from_utf8(word_slice).map_err(|_| {
                    LimboError::ParseError("Failed to serialize string!".to_string())
                })?;

                for ch in word.chars() {
                    match ch {
                        '"' => string.push_str("\\\""),
                        '\\' => string.push_str("\\\\"),
                        '\x08' => string.push_str("\\b"),
                        '\x0C' => string.push_str("\\f"),
                        '\n' => string.push_str("\\n"),
                        '\r' => string.push_str("\\r"),
                        '\t' => string.push_str("\\t"),
                        c if c <= '\u{001F}' => {
                            string.push_str(&format!("\\u{:04x}", c as u32));
                        }
                        _ => string.push(ch),
                    }
                }
            }

            _ => {
                unreachable!()
            }
        }
        if quote {
            string.push('"');
        }

        Ok(cursor + len)
    }

    fn serialize_number(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
    ) -> Result<usize> {
        let current_cursor = cursor + len;
        let num_slice = from_utf8(&self.data[cursor..current_cursor])
            .map_err(|_| LimboError::ParseError("Failed to parse integer".to_string()))?;

        match kind {
            ElementType::INT | ElementType::FLOAT => {
                string.push_str(num_slice);
            }
            ElementType::INT5 => {
                self.serialize_int5(string, num_slice)?;
            }
            ElementType::FLOAT5 => {
                self.serialize_float5(string, num_slice)?;
            }
            _ => unreachable!(),
        }
        Ok(current_cursor)
    }

    fn serialize_int5(&self, string: &mut String, hex_str: &str) -> Result<()> {
        // Check if number is hex
        if hex_str.len() > 2
            && (hex_str[..2].eq_ignore_ascii_case("0x")
                || (hex_str.starts_with("-") || hex_str.starts_with("+"))
                    && hex_str[1..3].eq_ignore_ascii_case("0x"))
        {
            let (sign, hex_part) = if hex_str.starts_with("-0x") || hex_str.starts_with("-0X") {
                ("-", &hex_str[3..])
            } else if hex_str.starts_with("+0x") || hex_str.starts_with("+0X") {
                ("", &hex_str[3..])
            } else {
                ("", &hex_str[2..])
            };

            // Add sign
            string.push_str(sign);

            let mut value = 0u64;

            for ch in hex_part.chars() {
                if !ch.is_ascii_hexdigit() {
                    bail_parse_error!("Failed to parse hex digit: {}", hex_part);
                }

                if (value >> 60) != 0 {
                    string.push_str("9.0e999");
                    return Ok(());
                }

                value = value * 16 + ch.to_digit(16).unwrap_or(0) as u64;
            }
            write!(string, "{value}")
                .map_err(|_| LimboError::ParseError("Error writing string to json!".to_string()))?;
        } else {
            string.push_str(hex_str);
        }

        Ok(())
    }

    fn serialize_float5(&self, string: &mut String, float_str: &str) -> Result<()> {
        if float_str.len() < 2 {
            bail_parse_error!("Integer is less then 2 chars: {}", float_str);
        }
        match float_str {
            "9e999" | "-9e999" => {
                string.push_str(float_str);
            }
            val if val.starts_with("-.") => {
                string.push_str("-0.");
                string.push_str(&val[2..]);
            }
            val if val.starts_with("+.") => {
                string.push_str("0.");
                string.push_str(&val[2..]);
            }
            val if val.starts_with(".") => {
                string.push_str("0.");
                string.push_str(&val[1..]);
            }
            val if val
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-') =>
            {
                string.push_str(val);
                string.push('0');
            }
            _ => bail_parse_error!("Unable to serialize float5: {}", float_str),
        }

        Ok(())
    }

    fn serialize_boolean(&self, string: &mut String, cursor: usize, val: bool) -> usize {
        if val {
            string.push_str("true");
        } else {
            string.push_str("false");
        }

        cursor
    }

    fn serialize_null(&self, string: &mut String, cursor: usize) -> usize {
        string.push_str("null");
        cursor
    }

    fn deserialize_value(&mut self, input: &[u8], mut pos: usize, depth: usize) -> PResult<usize> {
        if depth > MAX_JSON_DEPTH {
            return Err(PError::Message {
                msg: "Too deep".to_string(),
                location: Some(pos),
            });
        }

        pos = skip_whitespace(input, pos);
        if pos >= input.len() {
            return Err(PError::Message {
                msg: "Unexpected end of input".to_string(),
                location: Some(pos),
            });
        }

        match input[pos] {
            b'{' => {
                pos += 1; // consume '{'
                pos = self.deserialize_obj(input, pos, depth + 1)?;
            }
            b'[' => {
                pos += 1; // consume '['
                pos = self.deserialize_array(input, pos, depth + 1)?;
            }
            b't' => {
                pos = self.deserialize_true(input, pos)?;
            }
            b'f' => {
                pos = self.deserialize_false(input, pos)?;
            }
            b'n' | b'N' => {
                pos = self.deserialize_null_or_nan(input, pos)?;
            }
            b'"' | b'\'' => {
                pos = self.deserialize_string(input, pos)?;
            }
            c if c.is_ascii_digit()
                || c == b'-'
                || c == b'+'
                || c == b'.'
                || c.eq_ignore_ascii_case(&b'i') =>
            {
                pos = self.deserialize_number(input, pos)?;
            }
            _ => {
                return Err(PError::Message {
                    msg: "Unexpected character".to_string(),
                    location: Some(pos),
                })
            }
        }

        Ok(pos)
    }

    fn deserialize_obj(&mut self, input: &[u8], mut pos: usize, depth: usize) -> PResult<usize> {
        if depth > MAX_JSON_DEPTH {
            return Err(PError::Message {
                msg: "Too deep".to_string(),
                location: Some(pos),
            });
        }
        if self.data.capacity() - self.data.len() < 50 {
            self.data.reserve(self.data.capacity());
        }
        if pos >= input.len() {
            return Err(PError::Message {
                msg: "Unexpected end of input".to_string(),
                location: Some(pos),
            });
        }

        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::OBJECT, 0, false)
            .map_err(|_| PError::Message {
                msg: "Failed to write header".to_string(),
                location: Some(pos),
            })?;
        let obj_start = self.len();
        let mut first = true;

        loop {
            pos = skip_whitespace(input, pos);
            if pos >= input.len() {
                return Err(PError::Message {
                    msg: "Unexpected end of input".to_string(),
                    location: Some(pos),
                });
            }

            match input[pos] {
                b'}' => {
                    pos += 1; // consume '}'
                    if first {
                        return Ok(pos);
                    } else {
                        let obj_size = self.len() - obj_start;
                        self.write_element_header(header_pos, ElementType::OBJECT, obj_size, false)
                            .map_err(|_| PError::Message {
                                msg: "Failed to write header".to_string(),
                                location: Some(pos),
                            })?;
                        return Ok(pos);
                    }
                }
                b',' if !first => {
                    pos += 1; // consume ','
                    pos = skip_whitespace(input, pos);
                    if input[pos] == b',' || input[pos] == b'{' {
                        return Err(PError::Message {
                            msg: "Two commas in a row".to_string(),
                            location: Some(pos),
                        });
                    }
                }
                _ => {
                    // Parse key (must be string)
                    pos = self.deserialize_string(input, pos)?;

                    pos = skip_whitespace(input, pos);
                    if pos >= input.len() || input[pos] != b':' {
                        return Err(PError::Message {
                            msg: "Expected : after object key".to_string(),
                            location: Some(pos),
                        });
                    }
                    pos += 1; // consume ':'

                    pos = skip_whitespace(input, pos);

                    // Parse value - can be any JSON value including another object
                    pos = self.deserialize_value(input, pos, depth + 1)?;
                    pos = skip_whitespace(input, pos);
                    if pos < input.len() && !matches!(input[pos], b',' | b'}') {
                        return Err(PError::Message {
                            msg: "Should be , or }}".to_string(),
                            location: Some(pos),
                        });
                    }
                    first = false;
                }
            }
        }
    }

    fn deserialize_array(&mut self, input: &[u8], mut pos: usize, depth: usize) -> PResult<usize> {
        if depth > MAX_JSON_DEPTH {
            return Err(PError::Message {
                msg: "Too deep".to_string(),
                location: Some(pos),
            });
        }

        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::ARRAY, 0, false)
            .map_err(|_| PError::Message {
                msg: "Failed to write header".to_string(),
                location: Some(pos),
            })?;
        let arr_start = self.len();
        let mut first = true;

        loop {
            pos = skip_whitespace(input, pos);
            if pos >= input.len() {
                return Err(PError::Message {
                    msg: "Unexpected end of input".to_string(),
                    location: Some(pos),
                });
            }

            match input[pos] {
                b']' => {
                    pos += 1; // consume ']'
                    if first {
                        return Ok(pos);
                    } else {
                        let arr_len = self.len() - arr_start;
                        self.write_element_header(header_pos, ElementType::ARRAY, arr_len, false)
                            .map_err(|_| PError::Message {
                                msg: "Failed to write header".to_string(),
                                location: Some(pos),
                            })?;
                        return Ok(pos);
                    }
                }
                b',' if !first => {
                    pos += 1; // consume ','
                    pos = skip_whitespace(input, pos);
                    if input[pos] == b',' {
                        return Err(PError::Message {
                            msg: "Two commas in a row".to_string(),
                            location: Some(pos),
                        });
                    }
                }
                _ => {
                    pos = skip_whitespace(input, pos);

                    // Parse array element
                    pos = self.deserialize_value(input, pos, depth + 1)?;

                    first = false;
                }
            }
        }
    }

    fn deserialize_string(&mut self, input: &[u8], mut pos: usize) -> PResult<usize> {
        if pos >= input.len() {
            return Err(PError::Message {
                msg: "Unexpected end of input".to_string(),
                location: Some(pos),
            });
        }

        let string_start = self.len();
        let quote = input[pos];
        pos += 1; // consume quote

        let quoted = quote == b'"' || quote == b'\'';
        let mut len = 0;

        if quoted {
            // Try to find the closing quote and check for simple string
            let mut end_pos = pos;
            let is_simple = true;

            while end_pos < input.len() {
                let c = input[end_pos];
                if c == quote {
                    // Found end of string - check if it's simple
                    if is_simple {
                        let len = end_pos - pos;
                        let header_pos = self.data.len();

                        // Write header and content
                        if len <= 11 {
                            self.data
                                .push((ElementType::TEXT as u8) | ((len as u8) << 4));
                        } else {
                            self.write_element_header(header_pos, ElementType::TEXT, len, false)
                                .map_err(|_| PError::Message {
                                    msg: "Failed to write header".to_string(),
                                    location: Some(pos),
                                })?;
                        }

                        self.data.extend_from_slice(&input[pos..end_pos]);
                        return Ok(end_pos + 1); // Skip past closing quote
                    }
                    break;
                } else if c == b'\\' || c < 32 {
                    // Not a simple string
                    break;
                }
                end_pos += 1;
            }
        }

        // Write placeholder header to be updated later
        self.write_element_header(string_start, ElementType::TEXT, 0, false)
            .map_err(|_| PError::Message {
                msg: "Failed to write header".to_string(),
                location: Some(pos),
            })?;

        if pos >= input.len() {
            return Err(PError::Message {
                msg: "Unexpected end of input".to_string(),
                location: Some(pos),
            });
        }

        let mut element_type = ElementType::TEXT;

        // Special case for unquoted JSON5 keys (identifiers)
        if !quoted {
            self.data.push(quote);
            len += 1;

            if pos < input.len() && input[pos] == b':' {
                self.write_element_header(string_start, element_type, len, false)
                    .map_err(|_| PError::Message {
                        msg: "Failed to write header".to_string(),
                        location: Some(pos),
                    })?;
                return Ok(pos);
            }
        }

        let mut escape_buffer = [0u8; 6]; // Buffer for escape sequences

        while pos < input.len() {
            let c = input[pos];
            pos += 1;

            if quoted && c == quote {
                break; // End of string
            } else if !quoted && (c == b'"' || c == b'\'') {
                return Err(PError::Message {
                    msg: "Unexpected input".to_string(),
                    location: Some(pos),
                });
            } else if c == b'\\' {
                // Handle escape sequences
                if pos >= input.len() {
                    return Err(PError::Message {
                        msg: "Unexpected end of input".to_string(),
                        location: Some(pos),
                    });
                }

                let esc = input[pos];
                pos += 1;

                match esc {
                    b'b' => {
                        self.data.extend_from_slice(b"\\b");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'f' => {
                        self.data.extend_from_slice(b"\\f");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'n' => {
                        self.data.extend_from_slice(b"\\n");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'r' => {
                        self.data.extend_from_slice(b"\\r");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b't' => {
                        self.data.extend_from_slice(b"\\t");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'\\' | b'"' | b'/' => {
                        self.data.push(b'\\');
                        self.data.push(esc);
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'u' => {
                        // Unicode escape sequence
                        if pos + 4 > input.len() {
                            return Err(PError::Message {
                                msg: "Incomplete unicode escape sequence".to_string(),
                                location: Some(pos),
                            });
                        }

                        escape_buffer[0] = b'\\';
                        escape_buffer[1] = b'u';

                        for i in 0..4 {
                            let h = input[pos + i];
                            if !is_hex_digit(h) {
                                return Err(PError::Message {
                                    msg: "Invalid unicode escape sequence".to_string(),
                                    location: Some(pos),
                                });
                            }
                            escape_buffer[2 + i] = h;
                        }

                        self.data.extend_from_slice(&escape_buffer[0..6]);
                        len += 6;
                        pos += 4;
                        element_type = ElementType::TEXTJ;
                    }
                    // JSON5 extensions
                    b'\n' => {
                        self.data.extend_from_slice(b"\\\n");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'\'' => {
                        self.data.extend_from_slice(b"\\\'");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'0' => {
                        self.data.extend_from_slice(b"\\0");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'v' => {
                        self.data.extend_from_slice(b"\\v");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'x' => {
                        // Hex escape sequence (JSON5)
                        if pos + 2 > input.len() {
                            return Err(PError::Message {
                                msg: "Incopmlete hex escape sequence".to_string(),
                                location: Some(pos),
                            });
                        }

                        escape_buffer[0] = b'\\';
                        escape_buffer[1] = b'x';

                        for i in 0..2 {
                            let h = input[pos + i];
                            if !is_hex_digit(h) {
                                return Err(PError::Message {
                                    msg: "Invalid hex escape sequence".to_string(),
                                    location: Some(pos),
                                });
                            }
                            escape_buffer[2 + i] = h;
                        }

                        self.data.extend_from_slice(&escape_buffer[0..4]);
                        len += 4;
                        pos += 2;
                        element_type = ElementType::TEXT5;
                    }

                    _ => {
                        return Err(PError::Message {
                            msg: "Invalid escape sequence".to_string(),
                            location: Some(pos),
                        });
                    }
                }
            } else if !quoted && (c == b':' || c.is_ascii_whitespace()) {
                // End of unquoted identifier
                pos -= 1; // Put back the terminating character
                break;
            } else if c <= 0x1F {
                // Control character
                element_type = ElementType::TEXT5;
                self.data.push(c);
                len += 1;
            } else {
                // Normal character
                self.data.push(c);
                len += 1;
            }
        }

        // Write final header with correct type and size
        self.write_element_header(string_start, element_type, len, false)
            .map_err(|_| PError::Message {
                msg: "Failed to write header".to_string(),
                location: Some(pos),
            })?;

        Ok(pos)
    }

    fn deserialize_number(&mut self, input: &[u8], mut pos: usize) -> PResult<usize> {
        let num_start = self.len();
        let mut len = 0;
        let mut is_float = false;
        let mut is_json5 = false;

        // Write placeholder header
        self.write_element_header(num_start, ElementType::INT, 0, false)
            .map_err(|_| PError::Message {
                msg: "Failed to write header".to_string(),
                location: Some(pos),
            })?;

        // Handle sign
        if pos < input.len() && (input[pos] == b'-' || input[pos] == b'+') {
            if input[pos] == b'+' {
                is_json5 = true;
                pos += 1;
            } else {
                self.data.push(input[pos]);
                pos += 1;
                len += 1;
            }
        }

        // Handle JSON5 float starting with dot
        if pos < input.len() && input[pos] == b'.' {
            is_json5 = true;
            is_float = true;
        }

        // Check for hex (JSON5)
        if pos < input.len() && input[pos] == b'0' && pos + 1 < input.len() {
            self.data.push(input[pos]);
            pos += 1;
            len += 1;

            if pos < input.len() && (input[pos] == b'x' || input[pos] == b'X') {
                // Hexadecimal number
                self.data.push(input[pos]);
                pos += 1;
                len += 1;

                let mut has_digit = false;
                while pos < input.len() && is_hex_digit(input[pos]) {
                    self.data.push(input[pos]);
                    pos += 1;
                    len += 1;
                    has_digit = true;
                }

                if !has_digit {
                    return Err(PError::Message {
                        msg: "Invalid hex digit".to_string(),
                        location: Some(pos),
                    });
                }

                self.write_element_header(num_start, ElementType::INT5, len, false)
                    .map_err(|_| PError::Message {
                        msg: "Unexpected input after json".to_string(),
                        location: Some(pos),
                    })?;
                return Ok(pos);
            } else if pos < input.len() && input[pos].is_ascii_digit() {
                // Leading zero followed by digit is not allowed in standard JSON
                return Err(PError::Message {
                    msg: "Leading zero is not allowed".to_string(),
                    location: Some(pos),
                });
            }
        }

        // Check for Infinity
        if pos < input.len() && (input[pos] == b'I' || input[pos] == b'i') {
            // Try to match "Infinity"
            let infinity = b"infinity";
            let mut i = 0;

            while i < infinity.len() && pos + i < input.len() {
                if input[pos + i].to_ascii_lowercase() != infinity[i] {
                    return Err(PError::Message {
                        msg: "Invalid number".to_string(),
                        location: Some(pos),
                    });
                }
                i += 1;
            }

            if i < infinity.len() {
                return Err(PError::Message {
                    msg: "incomplete infinity".to_string(),
                    location: Some(pos),
                });
            }

            pos += infinity.len();

            // Write Infinity as 9e999
            self.data.extend_from_slice(b"9e999");
            self.write_element_header(
                num_start,
                ElementType::FLOAT5,
                len + INFINITY_CHAR_COUNT as usize,
                false,
            )
            .map_err(|_| PError::Message {
                msg: "Failed to write header".to_string(),
                location: Some(pos),
            })?;

            return Ok(pos);
        }

        // Regular number parsing
        while pos < input.len() {
            match input[pos] {
                b'0'..=b'9' => {
                    self.data.push(input[pos]);
                    pos += 1;
                    len += 1;
                }
                b'.' => {
                    is_float = true;
                    self.data.push(input[pos]);
                    pos += 1;
                    len += 1;

                    // Check for trailing dot
                    if pos >= input.len() || !input[pos].is_ascii_digit() {
                        is_json5 = true;
                    }
                }
                b'e' | b'E' => {
                    is_float = true;
                    self.data.push(input[pos]);
                    pos += 1;
                    len += 1;

                    // Optional sign after exponent
                    if pos < input.len() && (input[pos] == b'+' || input[pos] == b'-') {
                        self.data.push(input[pos]);
                        pos += 1;
                        len += 1;
                    }
                }
                _ => break,
            }
        }

        // No digits found
        if len == 0 && (!is_json5 || !is_float) {
            return Err(PError::Message {
                msg: "Not a digigt".to_string(),
                location: Some(pos),
            });
        }

        // Determine the appropriate element type
        let element_type = if is_float {
            if is_json5 {
                ElementType::FLOAT5
            } else {
                ElementType::FLOAT
            }
        } else if is_json5 {
            ElementType::INT5
        } else {
            ElementType::INT
        };

        self.write_element_header(num_start, element_type, len, false)
            .map_err(|_| PError::Message {
                msg: "Unexpected input after json".to_string(),
                location: Some(pos),
            })?;

        Ok(pos)
    }

    fn deserialize_true(&mut self, input: &[u8], mut pos: usize) -> PResult<usize> {
        let true_lit = b"true";
        for i in 0..true_lit.len() {
            if pos + i >= input.len() || input[pos + i] != true_lit[i] {
                return Err(PError::Message {
                    msg: "Expected true".to_string(),
                    location: Some(pos),
                });
            }
        }

        pos += true_lit.len();
        self.data.push(ElementType::TRUE as u8);

        Ok(pos)
    }

    fn deserialize_false(&mut self, input: &[u8], mut pos: usize) -> PResult<usize> {
        let false_lit = b"false";
        for i in 0..false_lit.len() {
            if pos + i >= input.len() || input[pos + i] != false_lit[i] {
                return Err(PError::Message {
                    msg: "Expected false".to_string(),
                    location: Some(pos),
                });
            }
        }

        pos += false_lit.len();
        self.data.push(ElementType::FALSE as u8);

        Ok(pos)
    }

    pub fn deserialize_null_or_nan(&mut self, input: &[u8], mut pos: usize) -> PResult<usize> {
        // First check if we have enough bytes remaining
        if pos + 3 >= input.len() {
            return Err(PError::Message {
                msg: "Unexpected end of input".to_string(),
                location: Some(pos),
            });
        }

        // Fast path for "null"
        if pos + 4 <= input.len()
            && input[pos] == b'n'
            && input[pos + 1] == b'u'
            && input[pos + 2] == b'l'
            && input[pos + 3] == b'l'
        {
            pos += 4;
            self.data.push(ElementType::NULL as u8);
            return Ok(pos);
        }

        // Fast path for "nan"
        if pos + 3 <= input.len()
            && (input[pos] == b'n' || input[pos] == b'N')
            && (input[pos + 1] == b'a' || input[pos + 1] == b'A')
            && (input[pos + 2] == b'n' || input[pos + 2] == b'N')
        {
            pos += 3;
            self.data.push(ElementType::NULL as u8);
            return Ok(pos);
        }

        Err(PError::Message {
            msg: "Expected null or nan".to_string(),
            location: Some(pos),
        })
    }

    fn write_element_header(
        &mut self,
        cursor: usize,
        element_type: ElementType,
        payload_size: usize,
        size_might_change: bool,
    ) -> Result<usize> {
        if payload_size <= 11 && !size_might_change {
            let header_byte = (element_type as u8) | ((payload_size as u8) << 4);
            if cursor == self.len() {
                self.data.push(header_byte);
            } else {
                self.data[cursor] = header_byte;
            }
            return Ok(1);
        }

        let header = JsonbHeader::new(element_type, payload_size).into_bytes();
        let header_bytes = header.as_bytes();
        let header_len = header_bytes.len();

        if cursor == self.len() {
            self.data.extend_from_slice(header_bytes);
            return Ok(header_len);
        }

        let old_len = if size_might_change {
            let (_, offset) = self.read_header(cursor)?;
            offset
        } else {
            1
        };

        let new_len = header_bytes.len();

        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Greater => {
                self.data.splice(
                    cursor + old_len..cursor + old_len,
                    std::iter::repeat_n(0, new_len - old_len),
                );
            }
            std::cmp::Ordering::Less => {
                self.data.drain(cursor + new_len..cursor + old_len);
            }
            std::cmp::Ordering::Equal => {}
        }

        for (i, &byte) in header_bytes.iter().enumerate() {
            self.data[cursor + i] = byte;
        }

        Ok(new_len)
    }

    fn from_str(input: &str) -> PResult<Self> {
        let mut result = Self::new(input.len(), None);
        let input = input.as_bytes();

        if input.is_empty() {
            return Err(PError::Message {
                msg: "Unexpected input after json".to_string(),
                location: None,
            });
        }

        // Parse the first complete JSON value
        let mut pos = 0;
        pos = result.deserialize_value(input, pos, 0)?;

        // Skip any trailing whitespace
        pos = skip_whitespace(input, pos);

        // Check for any non-whitespace characters after the JSON value
        if pos < input.len() {
            return Err(PError::Message {
                msg: "Unexpected input after json".to_string(),
                location: Some(pos),
            });
        }

        Ok(result)
    }

    pub fn from_raw_data(data: &[u8]) -> Self {
        Self::new(data.len(), Some(data))
    }

    pub fn data(self) -> Vec<u8> {
        self.data
    }

    pub fn element_type_at(&self, idx: usize) -> Result<ElementType> {
        let (JsonbHeader(element_type, _), _) = self.read_header(idx)?;
        Ok(element_type)
    }

    pub fn array_len(&self) -> Result<usize> {
        let (header, header_skip) = self.read_header(0)?;
        if header.0 != ElementType::ARRAY {
            return Ok(0);
        }

        let mut count = 0;
        let mut pos = header_skip;
        while pos < header_skip + header.1 {
            pos = self.skip_element(pos)?;
            count += 1;
        }

        Ok(count)
    }

    pub fn navigate_path(
        &mut self,
        path: &JsonPath,
        mode: PathOperationMode,
    ) -> Result<Vec<JsonTraversalResult>> {
        let mut path_iter = path.elements.iter().peekable();
        let mut pos = 0;
        let mut stack: Vec<JsonTraversalResult> = Vec::with_capacity(path.elements.len());

        while let Some(current) = path_iter.next() {
            let next_is_array = matches!(path_iter.peek(), Some(PathElement::ArrayLocator(_)))
                && !matches!(current, PathElement::ArrayLocator(_));

            let result = if next_is_array {
                let array_locator = path_iter.next().unwrap();

                self.navigate_to_segment(
                    SegmentVariant::KeyWithArrayIndex(current, array_locator),
                    pos,
                    mode,
                )?
            } else {
                self.navigate_to_segment(SegmentVariant::Single(current), pos, mode)?
            };

            pos = match &result.array_position_info {
                Some(ArrayPositionKind::SpecificIndex(idx)) => *idx,
                None => result.field_value_index,
            };

            stack.push(result);
        }

        Ok(stack)
    }

    pub fn operate_on_path<T>(&mut self, path: &JsonPath, operation: &mut T) -> Result<()>
    where
        T: PathOperation,
    {
        let mode = operation.operation_mode();

        let stack = self.navigate_path(path, mode)?;

        operation.execute(self, stack)?;

        Ok(())
    }

    fn update_parent_references(
        &mut self,
        stack: Vec<JsonTraversalResult>,
        delta: isize,
    ) -> Result<()> {
        let mut delta = delta;
        let mut is_prev_arr = false;
        for parent in stack.iter().rev() {
            let (JsonbHeader(el_type, el_size), el_header_len) =
                self.read_header(parent.field_value_index)?;

            if el_type == ElementType::ARRAY && !is_prev_arr {
                is_prev_arr = true;
                let arr_element_idx = parent.get_array_index().unwrap();
                let (JsonbHeader(arr_el_type, arr_el_size), arr_el_header_len) =
                    self.read_header(arr_element_idx)?;

                let new_arr_el_header_len = self.write_element_header(
                    arr_element_idx,
                    arr_el_type,
                    (arr_el_size as isize + delta) as usize,
                    true,
                )?;

                delta += (new_arr_el_header_len - arr_el_header_len) as isize;
            } else {
                is_prev_arr = false;
            }
            let new_size = el_size as isize + delta;
            let new_header_size = self.write_element_header(
                parent.field_value_index,
                el_type,
                new_size as usize,
                true,
            )?;

            let header_diff = new_header_size as isize - el_header_len as isize;

            delta += parent.delta;
            delta += header_diff;
        }

        Ok(())
    }

    fn navigate_to_segment(
        &mut self,
        segment: SegmentVariant,
        mut pos: usize,
        mode: PathOperationMode,
    ) -> Result<JsonTraversalResult> {
        let (JsonbHeader(element_type, element_size), header_size) = self.read_header(pos)?;

        match segment {
            SegmentVariant::Single(PathElement::Root()) => {
                return Ok(JsonTraversalResult::new(
                    pos,
                    JsonLocationKind::DocumentRoot,
                    0,
                ));
            }
            SegmentVariant::Single(PathElement::ArrayLocator(idx)) => {
                let (JsonbHeader(root_type, root_size), root_header_size) =
                    self.read_header(pos)?;

                if root_type == ElementType::ARRAY {
                    let end_pos = pos + root_header_size + root_size;

                    match idx {
                        Some(idx) if *idx >= 0 => {
                            let mut count = 0;
                            let mut arr_pos = pos + root_header_size;

                            while arr_pos < end_pos && count != *idx as usize {
                                arr_pos = self.skip_element(arr_pos)?;
                                count += 1;
                            }

                            if mode.allows_insert() && arr_pos == end_pos {
                                let placeholder =
                                    JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();

                                self.data
                                    .splice(arr_pos..arr_pos, placeholder_bytes.iter().copied());

                                return Ok(JsonTraversalResult::with_array_index(
                                    pos + root_header_size,
                                    JsonLocationKind::ArrayEntry,
                                    placeholder_bytes.len() as isize,
                                    arr_pos,
                                ));
                            }

                            if arr_pos != end_pos && mode.allows_replace() {
                                return Ok(JsonTraversalResult::with_array_index(
                                    pos,
                                    JsonLocationKind::ArrayEntry,
                                    0,
                                    arr_pos,
                                ));
                            }

                            bail_parse_error!("Not found!");
                        }
                        Some(idx) if *idx < 0 => {
                            let mut idx_map: HashMap<i32, usize> = HashMap::with_capacity(100);
                            let mut element_idx = 0;
                            let mut arr_pos = pos + root_header_size;

                            while arr_pos < end_pos {
                                idx_map.insert(element_idx, arr_pos);
                                arr_pos = self.skip_element(arr_pos)?;
                                element_idx += 1;
                            }

                            let real_idx = element_idx + idx;

                            if let Some(index) = idx_map.get(&real_idx) {
                                return Ok(JsonTraversalResult::with_array_index(
                                    pos,
                                    JsonLocationKind::ArrayEntry,
                                    0,
                                    *index,
                                ));
                            } else {
                                bail_parse_error!("Element with negative index not found")
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    if root_type == ElementType::OBJECT
                        && root_size == 0
                        && (*idx == Some(0) || idx.is_none())
                        && mode.allows_insert()
                    {
                        let array = JsonbHeader::new(ElementType::ARRAY, 0).into_bytes();
                        let array_bytes = array.as_bytes();
                        let placeholder = JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                        let placeholder_bytes = placeholder.as_bytes();
                        self.data.splice(
                            pos..pos + root_header_size,
                            array_bytes
                                .iter()
                                .copied()
                                .chain(placeholder_bytes.iter().copied()),
                        );

                        return Ok(JsonTraversalResult::with_array_index(
                            pos,
                            JsonLocationKind::ArrayEntry,
                            placeholder_bytes.len() as isize,
                            pos + array_bytes.len(),
                        ));
                    };
                    bail_parse_error!("Root is not an array");
                }
            }
            SegmentVariant::Single(PathElement::Key(path_key, is_raw)) => {
                if element_type == ElementType::OBJECT {
                    let end_pos = pos + element_size + header_size;

                    pos += header_size;

                    while pos < end_pos {
                        let (JsonbHeader(key_type, key_len), key_header_len) =
                            self.read_header(pos)?;

                        if !key_type.is_valid_key() {
                            bail_parse_error!("Key should be string");
                        }

                        let key_start = pos + key_header_len;
                        let json_key = unsafe {
                            from_utf8_unchecked(&self.data[key_start..key_start + key_len])
                        };

                        if compare((json_key, key_type), (path_key, *is_raw)) {
                            if mode.allows_replace() {
                                let value_pos = pos + key_header_len + key_len;
                                let key_pos = pos;

                                return Ok(JsonTraversalResult::new(
                                    value_pos,
                                    JsonLocationKind::ObjectProperty(key_pos),
                                    0,
                                ));
                            } else {
                                bail_parse_error!("Cant replace")
                            }
                        } else {
                            pos += key_header_len + key_len;
                            pos = self.skip_element(pos)?;
                        }
                    }

                    if mode.allows_insert() {
                        let key_type = if *is_raw {
                            ElementType::TEXTRAW
                        } else {
                            ElementType::TEXT
                        };

                        let key_header = JsonbHeader::new(key_type, path_key.len()).into_bytes();
                        let key_header_bytes = key_header.as_bytes();
                        let key_bytes = path_key.as_bytes();
                        let value_header = JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                        let value_header_bytes = value_header.as_bytes();

                        self.data.splice(
                            pos..pos,
                            key_header_bytes
                                .iter()
                                .copied()
                                .chain(key_bytes.iter().copied())
                                .chain(value_header_bytes.iter().copied()),
                        );

                        let key_idx = pos;
                        let value_idx = pos + key_header_bytes.len() + key_bytes.len();
                        let delta =
                            key_header_bytes.len() + key_bytes.len() + value_header_bytes.len();

                        return Ok(JsonTraversalResult::new(
                            value_idx,
                            JsonLocationKind::ObjectProperty(key_idx),
                            delta as isize,
                        ));
                    } else {
                        bail_parse_error!("Mode does not allow insert cannot create new key!")
                    }
                } else {
                    bail_parse_error!("Looks like this is noop");
                }
            }
            SegmentVariant::KeyWithArrayIndex(
                PathElement::Root(),
                PathElement::ArrayLocator(idx),
            ) => {
                let (JsonbHeader(root_type, root_size), root_header_size) =
                    self.read_header(pos)?;

                if root_type == ElementType::ARRAY {
                    let end_pos = pos + root_header_size + root_size;

                    match idx {
                        Some(idx) if *idx >= 0 => {
                            let mut count = 0;
                            let mut arr_pos = pos + root_header_size;

                            while arr_pos < end_pos && count != *idx as usize {
                                arr_pos = self.skip_element(arr_pos)?;
                                count += 1;
                            }

                            if mode.allows_insert() && arr_pos == end_pos && count == *idx as usize
                            {
                                let placeholder =
                                    JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();

                                self.data
                                    .splice(arr_pos..arr_pos, placeholder_bytes.iter().copied());

                                return Ok(JsonTraversalResult::with_array_index(
                                    pos,
                                    JsonLocationKind::DocumentRoot,
                                    placeholder_bytes.len() as isize,
                                    arr_pos,
                                ));
                            }

                            if arr_pos != end_pos && mode.allows_replace() {
                                return Ok(JsonTraversalResult::with_array_index(
                                    pos,
                                    JsonLocationKind::DocumentRoot,
                                    0,
                                    arr_pos,
                                ));
                            }

                            bail_parse_error!("Not found!");
                        }
                        Some(idx) if *idx < 0 => {
                            let mut idx_map: HashMap<i32, usize> = HashMap::with_capacity(100);
                            let mut element_idx = 0;
                            let mut arr_pos = pos + root_header_size;

                            while arr_pos < end_pos {
                                idx_map.insert(element_idx, arr_pos);
                                arr_pos = self.skip_element(arr_pos)?;
                                element_idx += 1;
                            }

                            let real_idx = element_idx + idx;

                            if let Some(index) = idx_map.get(&real_idx) {
                                return Ok(JsonTraversalResult::with_array_index(
                                    pos,
                                    JsonLocationKind::DocumentRoot,
                                    0,
                                    *index,
                                ));
                            } else {
                                bail_parse_error!("Element with negative index not found")
                            }
                        }
                        _ => unreachable!(),
                    }
                } else {
                    bail_parse_error!("Root is not an array");
                }
            }
            SegmentVariant::KeyWithArrayIndex(
                PathElement::Key(path_key, is_raw),
                PathElement::ArrayLocator(idx),
            ) => {
                if element_type != ElementType::OBJECT {
                    bail_parse_error!("Not an object");
                }

                let end_pos = pos + header_size + element_size;

                let mut current_pos = pos + header_size;

                while current_pos < end_pos {
                    let (JsonbHeader(key_type, key_size), key_header_size) =
                        self.read_header(current_pos)?;

                    if !key_type.is_valid_key() {
                        bail_parse_error!("Key should be string")
                    }

                    let obj_key = unsafe {
                        from_utf8_unchecked(
                            &self.data[current_pos + key_header_size
                                ..current_pos + key_header_size + key_size],
                        )
                    };

                    if compare((obj_key, key_type), (path_key, *is_raw)) {
                        break;
                    } else {
                        current_pos =
                            self.skip_element(current_pos + key_size + key_header_size)?;
                    }
                }

                if current_pos == end_pos && mode.allows_insert() {
                    if idx.is_some() && idx.unwrap() != 0 {
                        bail_parse_error!("cant create new arr with idx");
                    }

                    let key_header_type = if *is_raw {
                        ElementType::TEXTRAW
                    } else {
                        ElementType::TEXT
                    };

                    let key_header = JsonbHeader::new(key_header_type, path_key.len()).into_bytes();
                    let key_header_bytes = key_header.as_bytes();
                    let key_bytes = path_key.as_bytes();
                    let array_header = JsonbHeader::new(ElementType::ARRAY, 1).into_bytes();
                    let array_header_bytes = array_header.as_bytes();
                    let array_value_header = JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                    let array_value_header_bytes = array_value_header.as_bytes();

                    let delta = key_header_bytes.len()
                        + key_bytes.len()
                        + array_header_bytes.len()
                        + array_value_header_bytes.len();

                    self.data.splice(
                        current_pos..current_pos,
                        key_header_bytes
                            .iter()
                            .copied()
                            .chain(key_bytes.iter().copied())
                            .chain(array_header_bytes.iter().copied())
                            .chain(array_value_header_bytes.iter().copied()),
                    );

                    let key_idx = current_pos;
                    let value_idx = current_pos + key_header_bytes.len() + key_bytes.len();
                    let array_idx = value_idx + array_header_bytes.len();

                    return Ok(JsonTraversalResult::with_array_index(
                        value_idx,
                        JsonLocationKind::ObjectProperty(key_idx),
                        delta as isize,
                        array_idx,
                    ));
                }

                if current_pos != end_pos && mode.allows_replace() {
                    let key_idx = current_pos;

                    current_pos = self.skip_element(current_pos)?;
                    let value_idx = current_pos;

                    let (JsonbHeader(value_type, value_size), value_header_size) =
                        self.read_header(value_idx)?;

                    if value_type != ElementType::ARRAY {
                        bail_parse_error!("Should be array")
                    }

                    let end_pos = current_pos + value_header_size + value_size;

                    match idx {
                        Some(idx) if *idx >= 0 => {
                            let mut count = 0;
                            let mut arr_pos = value_idx + value_header_size;

                            while arr_pos < end_pos && count != *idx as usize {
                                arr_pos = self.skip_element(arr_pos)?;
                                count += 1;
                            }

                            if mode.allows_insert() && arr_pos == end_pos && count == *idx as usize
                            {
                                let placeholder =
                                    JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();

                                self.data
                                    .splice(arr_pos..arr_pos, placeholder_bytes.iter().copied());
                                self.write_element_header(
                                    value_idx,
                                    ElementType::ARRAY,
                                    value_size + placeholder_bytes.len(),
                                    true,
                                )?;
                                return Ok(JsonTraversalResult::with_array_index(
                                    value_idx,
                                    JsonLocationKind::ObjectProperty(key_idx),
                                    placeholder_bytes.len() as isize,
                                    arr_pos,
                                ));
                            }

                            if arr_pos != end_pos && mode.allows_replace() {
                                return Ok(JsonTraversalResult::with_array_index(
                                    value_idx,
                                    JsonLocationKind::ObjectProperty(key_idx),
                                    0,
                                    arr_pos,
                                ));
                            }

                            bail_parse_error!("Not found!");
                        }
                        Some(idx) if *idx < 0 => {
                            let mut idx_map: HashMap<i32, usize> = HashMap::with_capacity(100);
                            let mut element_idx = 0;
                            let mut arr_pos = value_idx + value_header_size;

                            while arr_pos < end_pos {
                                idx_map.insert(element_idx, arr_pos);
                                arr_pos = self.skip_element(arr_pos)?;
                                element_idx += 1;
                            }

                            let real_idx = element_idx + idx;

                            if let Some(index) = idx_map.get(&real_idx) {
                                return Ok(JsonTraversalResult::with_array_index(
                                    value_idx,
                                    JsonLocationKind::ObjectProperty(key_idx),
                                    0,
                                    *index,
                                ));
                            } else {
                                bail_parse_error!(
                                    "ERROR: Element at negative index {} not found",
                                    idx
                                );
                            }
                        }
                        Some(_) => unreachable!(),
                        None => {
                            if mode.allows_insert() {
                                let placeholder =
                                    JsonbHeader::new(ElementType::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();
                                let insertion_point = value_idx + value_size + value_header_size;

                                self.data.insert(insertion_point, placeholder_bytes[0]);
                            } else {
                                bail_parse_error!("Cant insert")
                            }
                        }
                    }
                }
            }
            _ => {
                unreachable!()
            }
        };

        Err(LimboError::ParseError("Not found".to_string()))
    }

    fn skip_element(&self, mut pos: usize) -> Result<usize> {
        let (header, skip_header) = self.read_header(pos)?;
        pos += skip_header + header.1;
        Ok(pos)
    }

    // Primitive implementation could be optimized.
    pub fn patch(&mut self, patch: &Jsonb) -> Result<()> {
        let (patch_header, _) = patch.read_header(0)?;

        if patch_header.0 != ElementType::OBJECT {
            self.data.clear();
            self.data.extend_from_slice(&patch.data);
            return Ok(());
        }

        let result = self;

        let mut work_stack = VecDeque::with_capacity(10);
        work_stack.push_back((
            JsonPath {
                elements: vec![PathElement::Root()],
            },
            0,
        ));

        while let Some((path, patch_cursor)) = work_stack.pop_front() {
            let (patch_obj_header, patch_obj_header_size) = patch.read_header(patch_cursor)?;

            if patch_obj_header.0 != ElementType::OBJECT {
                continue;
            }

            let patch_end = patch_cursor + patch_obj_header_size + patch_obj_header.1;
            let mut patch_key_cursor = patch_cursor + patch_obj_header_size;

            let mut key_values = Vec::new();

            while patch_key_cursor < patch_end {
                let (key_header, key_header_size) = patch.read_header(patch_key_cursor)?;
                if !key_header.0.is_valid_key() {
                    return Err(LimboError::ParseError("Invalid key type".to_string()));
                }

                let key_start = patch_key_cursor + key_header_size;
                let key_text = unsafe {
                    from_utf8_unchecked(&patch.data[key_start..key_start + key_header.1])
                };

                // Read the value
                let value_cursor = key_start + key_header.1;
                let (value_header, value_header_size) = patch.read_header(value_cursor)?;
                let key_text = if matches!(
                    key_header.0,
                    ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW
                ) {
                    Cow::Owned(unescape_string(key_text))
                } else {
                    Cow::Borrowed(key_text)
                };

                key_values.push((
                    key_text,
                    value_header.0,
                    value_cursor,
                    value_header_size,
                    value_header.1,
                ));

                patch_key_cursor = value_cursor + value_header_size + value_header.1;
            }

            for (key_text, value_type, value_cursor, value_header_size, value_size) in key_values {
                // Create a path to this key
                let mut key_path = path.clone();

                key_path.elements.push(PathElement::Key(key_text, false));

                match value_type {
                    ElementType::NULL => {
                        let mut op = DeleteOperation::new();

                        let _ = result.operate_on_path(&key_path, &mut op);
                    }
                    ElementType::OBJECT => {
                        let value_data = &patch.data
                            [value_cursor..value_cursor + value_header_size + value_size];

                        let target_path_result =
                            result.navigate_path(&key_path, PathOperationMode::ReplaceExisting);

                        if target_path_result.is_ok() {
                            let target_stack = target_path_result.unwrap();
                            let target_value_idx = target_stack.last().unwrap().field_value_index;
                            let (target_header, _) = result.read_header(target_value_idx)?;

                            if target_header.0 == ElementType::OBJECT {
                                work_stack.push_back((key_path, value_cursor));
                            } else {
                                let patch_obj = Jsonb::new(value_data.len(), Some(value_data));
                                let mut op = ReplaceOperation::new(patch_obj);
                                result.operate_on_path(&key_path, &mut op)?;
                                let _ = result.operate_on_path(&key_path, &mut op);

                                work_stack.push_back((key_path, value_cursor));
                            }
                        } else {
                            let empty_obj = Jsonb::new(
                                1,
                                Some(JsonbHeader::make_obj().into_bytes().as_bytes()),
                            );
                            let mut op = SetOperation::new(empty_obj);
                            let _ = result.operate_on_path(&key_path, &mut op);

                            work_stack.push_back((key_path, value_cursor));
                        }
                    }
                    _ => {
                        let value_data = &patch.data
                            [value_cursor..value_cursor + value_header_size + value_size];
                        let patch_value = Jsonb::new(value_data.len(), Some(value_data));

                        let mut op = SetOperation::new(patch_value);

                        let _ = result.operate_on_path(&key_path, &mut op);
                    }
                }
            }
        }

        Ok(())
    }
}

impl std::str::FromStr for Jsonb {
    type Err = PError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::from_str(s)
    }
}

#[inline]
fn compare(key: (&str, ElementType), path_key: (&str, bool)) -> bool {
    let (key, element_type) = key;
    let (path_key, is_raw) = path_key;
    if !is_raw && element_type == ElementType::TEXT {
        if key.len() == path_key.len() {
            return key == path_key;
        } else {
            return false;
        }
    }
    if !is_raw {
        return unescape_string(key) == path_key;
    }
    match element_type {
        ElementType::TEXTJ | ElementType::TEXT5 | ElementType::TEXTRAW | ElementType::TEXT => {
            return unescape_string(key) == unescape_string(path_key);
        }
        _ => {}
    };

    false
}

#[inline]
pub fn unescape_string(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut code_point = String::with_capacity(5);

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some('"') => result.push('"'),
                Some('b') => result.push('\u{0008}'),
                Some('f') => result.push('\u{000C}'),
                Some('x') => {
                    code_point.clear();
                    for _ in 0..2 {
                        if let Some(hex_char) = chars.next() {
                            code_point.push(hex_char);
                        } else {
                            break;
                        }
                    }
                    if let Ok(code) = u16::from_str_radix(&code_point, 16) {
                        if let Some(ch) = char::from_u32(code as u32) {
                            result.push(ch)
                        }
                    }
                }
                // Handle \uXXXX format (JSON style)
                Some('u') => {
                    code_point.clear();
                    for _ in 0..4 {
                        if let Some(hex_char) = chars.next() {
                            code_point.push(hex_char);
                        } else {
                            break;
                        }
                    }

                    if let Ok(code) = u16::from_str_radix(&code_point, 16) {
                        // Check if this is a high surrogate
                        if matches!(code, 0xD800..=0xDBFF) {
                            if chars.next() == Some('\\') && chars.next() == Some('u') {
                                code_point.clear();
                                for _ in 0..4 {
                                    if let Some(hex_char) = chars.next() {
                                        code_point.push(hex_char);
                                    } else {
                                        break;
                                    }
                                }

                                if let Ok(low_code) = u16::from_str_radix(&code_point, 16) {
                                    if (0xDC00..=0xDFFF).contains(&low_code) {
                                        let high_ten_bits = (code - 0xD800) as u32;
                                        let low_ten_bits = (low_code - 0xDC00) as u32;
                                        let code_point = (high_ten_bits << 10) | low_ten_bits;
                                        let unicode_value = code_point + 0x10000;

                                        if let Some(unicode_char) = char::from_u32(unicode_value) {
                                            result.push(unicode_char);
                                        }
                                    } else {
                                        // If low surrogate is invalid, just push both as separate chars
                                        if let Some(c1) = char::from_u32(code as u32) {
                                            result.push(c1);
                                        }
                                        if let Some(c2) = char::from_u32(low_code as u32) {
                                            result.push(c2);
                                        }
                                    }
                                }
                            } else {
                                // No low surrogate, just push the high surrogate as is
                                if let Some(unicode_char) = char::from_u32(code as u32) {
                                    result.push(unicode_char);
                                }
                            }
                        } else {
                            // Not a surrogate pair, just a regular Unicode character
                            if let Some(unicode_char) = char::from_u32(code as u32) {
                                result.push(unicode_char);
                            }
                        }
                    }
                }

                Some(c) => {
                    // For any other escape sequence we don't recognize,
                    // just output the backslash and the character
                    result.push('\\');
                    result.push(c);
                }
                None => {
                    // Handle trailing backslash
                    result.push('\\');
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[inline]
pub fn skip_whitespace(input: &[u8], mut pos: usize) -> usize {
    let len = input.len();
    if pos >= len {
        return pos;
    }

    // Fast path for non-whitespace, non-comment
    if (WS_TABLE[input[pos] as usize] & 1) == 0 && input[pos] != b'/' {
        return pos;
    }

    // Process whitespace and comments
    while pos < len {
        let ch = input[pos];
        if (WS_TABLE[ch as usize] & 1) != 0 {
            // Skip whitespace
            pos += 1;
        } else if ch == b'/' && pos + 1 < len {
            // Handle JSON5 comments
            match input[pos + 1] {
                b'/' => {
                    // Line comment - skip until newline
                    pos += 2;
                    while pos < len && input[pos] != b'\n' {
                        pos += 1;
                    }
                    if pos < len {
                        pos += 1; // Skip the newline
                    }
                }
                b'*' => {
                    // Block comment - skip until "*/"
                    pos += 2;
                    while pos + 1 < len {
                        if input[pos] == b'*' && input[pos + 1] == b'/' {
                            pos += 2;
                            break;
                        }
                        pos += 1;
                    }
                }
                _ => {
                    // Not a comment
                    break;
                }
            }
        } else {
            // Not whitespace or comment
            break;
        }
    }

    pos
}

#[inline]
fn is_hex_digit(ch: u8) -> bool {
    (CHARACTER_TYPE[ch as usize] & 3) == 2 || (CHARACTER_TYPE[ch as usize] & 3) == 3
}

#[inline]
fn is_json_ok(ch: u8) -> bool {
    (CHARACTER_TYPE_OK[ch as usize] & 4) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_serialization() {
        // Create JSONB with null value
        let mut jsonb = Jsonb::new(10, None);
        jsonb.data.push(ElementType::NULL as u8);

        // Test serialization
        let json_str = jsonb.to_string().unwrap();
        assert_eq!(json_str, "null");

        // Test round-trip
        let reparsed = Jsonb::from_str("null").unwrap();
        assert_eq!(reparsed.data[0], ElementType::NULL as u8);
    }

    #[test]
    fn test_boolean_serialization() {
        // True
        let mut jsonb_true = Jsonb::new(10, None);
        jsonb_true.data.push(ElementType::TRUE as u8);
        assert_eq!(jsonb_true.to_string().unwrap(), "true");

        // False
        let mut jsonb_false = Jsonb::new(10, None);
        jsonb_false.data.push(ElementType::FALSE as u8);
        assert_eq!(jsonb_false.to_string().unwrap(), "false");

        // Round-trip
        let true_parsed = Jsonb::from_str("true").unwrap();
        assert_eq!(true_parsed.data[0], ElementType::TRUE as u8);

        let false_parsed = Jsonb::from_str("false").unwrap();
        assert_eq!(false_parsed.data[0], ElementType::FALSE as u8);
    }

    #[test]
    fn test_integer_serialization() {
        // Standard integer
        let parsed = Jsonb::from_str("42").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "42");

        // Negative integer
        let parsed = Jsonb::from_str("-123").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-123");

        // Zero
        let parsed = Jsonb::from_str("0").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "0");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::INT));
    }

    #[test]
    fn test_json5_integer_serialization() {
        // Hexadecimal notation
        let parsed = Jsonb::from_str("0x1A").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "26"); // Should convert to decimal

        // Positive sign (JSON5)
        let parsed = Jsonb::from_str("+42").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "42");

        // Negative hexadecimal
        let parsed = Jsonb::from_str("-0xFF").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-255");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::INT5));
    }

    #[test]
    fn test_float_serialization() {
        // Standard float
        let parsed = Jsonb::from_str("3.14159").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "3.14159");

        // Negative float
        let parsed = Jsonb::from_str("-2.718").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-2.718");

        // Scientific notation
        let parsed = Jsonb::from_str("6.022e23").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "6.022e23");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::FLOAT));
    }

    #[test]
    fn test_json5_float_serialization() {
        // Leading decimal point
        let parsed = Jsonb::from_str(".123").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "0.123");

        // Trailing decimal point
        let parsed = Jsonb::from_str("42.").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "42.0");

        // Plus sign in exponent
        let parsed = Jsonb::from_str("1.5e+10").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "1.5e+10");

        // Infinity
        let parsed = Jsonb::from_str("Infinity").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "9e999");

        // Negative Infinity
        let parsed = Jsonb::from_str("-Infinity").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-9e999");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::FLOAT5));
    }

    #[test]
    fn test_string_serialization() {
        // Simple string
        let parsed = Jsonb::from_str(r#""hello world""#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

        // String with escaped characters
        let parsed = Jsonb::from_str(r#""hello\nworld""#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello\nworld""#);

        // Unicode escape
        let parsed = Jsonb::from_str(r#""hello\u0020world""#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello\u0020world""#);

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::TEXTJ));
    }

    #[test]
    fn test_json5_string_serialization() {
        // Single quotes
        let parsed = Jsonb::from_str("'hello world'").unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

        // Hex escape
        let parsed = Jsonb::from_str(r#"'\x41\x42\x43'"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""\u0041\u0042\u0043""#);

        // Multiline string with line continuation
        let parsed = Jsonb::from_str(
            r#""hello \
world""#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

        // Escaped single quote
        let parsed = Jsonb::from_str(r#"'Don\'t worry'"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""Don't worry""#);

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::TEXT5));
    }

    #[test]
    fn test_array_serialization() {
        // Empty array
        let parsed = Jsonb::from_str("[]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[]");

        // Simple array
        let parsed = Jsonb::from_str("[1,2,3]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

        // Nested array
        let parsed = Jsonb::from_str("[[1,2],[3,4]]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[[1,2],[3,4]]");

        // Mixed types array
        let parsed = Jsonb::from_str(r#"[1,"text",true,null,{"key":"value"}]"#).unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"[1,"text",true,null,{"key":"value"}]"#
        );

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::ARRAY));
    }

    #[test]
    fn test_json5_array_serialization() {
        // Trailing comma
        let parsed = Jsonb::from_str("[1,2,3,]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

        // Comments in array
        let parsed = Jsonb::from_str("[1,/* comment */2,3]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

        // Line comment in array
        let parsed = Jsonb::from_str("[1,// line comment\n2,3]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");
    }

    #[test]
    fn test_object_serialization() {
        // Empty object
        let parsed = Jsonb::from_str("{}").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "{}");

        // Simple object
        let parsed = Jsonb::from_str(r#"{"key":"value"}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Multiple properties
        let parsed = Jsonb::from_str(r#"{"a":1,"b":2,"c":3}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2,"c":3}"#);

        // Nested object
        let parsed = Jsonb::from_str(r#"{"outer":{"inner":"value"}}"#).unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"{"outer":{"inner":"value"}}"#
        );

        // Mixed values
        let parsed =
            Jsonb::from_str(r#"{"str":"text","num":42,"bool":true,"null":null,"arr":[1,2]}"#)
                .unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"{"str":"text","num":42,"bool":true,"null":null,"arr":[1,2]}"#
        );

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::OBJECT));
    }

    #[test]
    fn test_json5_object_serialization() {
        // Unquoted keys
        let parsed = Jsonb::from_str("{key:\"value\"}").unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Trailing comma
        let parsed = Jsonb::from_str(r#"{"a":1,"b":2,}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2}"#);

        // Comments in object
        let parsed = Jsonb::from_str(r#"{"a":1,/*comment*/"b":2}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2}"#);

        // Single quotes for keys and values
        let parsed = Jsonb::from_str("{'a':'value'}").unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":"value"}"#);
    }

    #[test]
    fn test_complex_json() {
        let complex_json = r#"{
            "string": "Hello, world!",
            "number": 42,
            "float": 3.14159,
            "boolean": true,
            "null": null,
            "array": [1, 2, 3, "text", {"nested": "object"}],
            "object": {
                "key1": "value1",
                "key2": [4, 5, 6],
                "key3": {
                    "nested": true
                }
            }
        }"#;

        let parsed = Jsonb::from_str(complex_json).unwrap();
        // Round-trip test
        let reparsed = Jsonb::from_str(&parsed.to_string().unwrap()).unwrap();
        assert_eq!(parsed.to_string().unwrap(), reparsed.to_string().unwrap());
    }

    #[test]
    fn test_error_handling() {
        // Invalid JSON syntax
        assert!(Jsonb::from_str("{").is_err());
        assert!(Jsonb::from_str("[").is_err());
        assert!(Jsonb::from_str("}").is_err());
        assert!(Jsonb::from_str("]").is_err());

        assert!(Jsonb::from_str(r#"{"a":"55,"b":72}"#).is_err());

        assert!(Jsonb::from_str(r#"{"a":"55",,"b":72}"#).is_err());

        // Unclosed string
        assert!(Jsonb::from_str(r#"{"key":"value"#).is_err());

        // Invalid number format
        assert!(Jsonb::from_str("01234").is_err()); // Leading zero not allowed in JSON

        // Invalid escape sequence
        assert!(Jsonb::from_str(r#""\z""#).is_err());

        // Missing colon in object
        assert!(Jsonb::from_str(r#"{"key" "value"}"#).is_err());

        // Trailing characters
        assert!(Jsonb::from_str(r#"{"key":"value"} extra"#).is_err());
    }

    #[test]
    fn test_depth_limit() {
        // Create a JSON string that exceeds MAX_JSON_DEPTH
        let mut deep_json = String::from("[");
        for _ in 0..MAX_JSON_DEPTH + 1 {
            deep_json.push('[');
        }
        for _ in 0..MAX_JSON_DEPTH + 1 {
            deep_json.push(']');
        }
        deep_json.push(']');

        // Should fail due to exceeding depth limit
        assert!(Jsonb::from_str(&deep_json).is_err());
    }

    #[test]
    fn test_header_encoding() {
        // Small payload (fits in 4 bits)
        let header = JsonbHeader::new(ElementType::TEXT, 5);
        let bytes = header.into_bytes().as_bytes().to_vec();
        assert_eq!(bytes[0], (5 << 4) | (ElementType::TEXT as u8));

        // Medium payload (8-bit)
        let header = JsonbHeader::new(ElementType::TEXT, 200);
        let bytes = header.into_bytes().as_bytes().to_vec();
        assert_eq!(
            bytes[0],
            (SIZE_MARKER_8BIT << 4) | (ElementType::TEXT as u8)
        );
        assert_eq!(bytes[1], 200);

        // Large payload (16-bit)
        let header = JsonbHeader::new(ElementType::TEXT, 40000);
        let bytes = header.into_bytes().as_bytes().to_vec();
        assert_eq!(
            bytes[0],
            (SIZE_MARKER_16BIT << 4) | (ElementType::TEXT as u8)
        );
        assert_eq!(bytes[1], (40000 >> 8) as u8);
        assert_eq!(bytes[2], (40000 & 0xFF) as u8);

        // Extra large payload (32-bit)
        let header = JsonbHeader::new(ElementType::TEXT, 70000);
        let bytes = header.into_bytes().as_bytes().to_vec();
        assert_eq!(
            bytes[0],
            (SIZE_MARKER_32BIT << 4) | (ElementType::TEXT as u8)
        );
        assert_eq!(bytes[1], (70000 >> 24) as u8);
        assert_eq!(bytes[2], ((70000 >> 16) & 0xFF) as u8);
        assert_eq!(bytes[3], ((70000 >> 8) & 0xFF) as u8);
        assert_eq!(bytes[4], (70000 & 0xFF) as u8);
    }

    #[test]
    fn test_header_decoding() {
        // Create sample data with various headers
        let data = vec![
            (5 << 4) | (ElementType::TEXT as u8),
            (SIZE_MARKER_8BIT << 4) | (ElementType::ARRAY as u8),
            150,
            (SIZE_MARKER_16BIT << 4) | (ElementType::OBJECT as u8),
            0x98,
            0x68,
        ];

        // Parse and verify each header
        let (header1, offset1) = JsonbHeader::from_slice(0, &data).unwrap();
        assert_eq!(offset1, 1);
        assert_eq!(header1.0, ElementType::TEXT);
        assert_eq!(header1.1, 5);

        let (header2, offset2) = JsonbHeader::from_slice(1, &data).unwrap();
        assert_eq!(offset2, 2);
        assert_eq!(header2.0, ElementType::ARRAY);
        assert_eq!(header2.1, 150);

        let (header3, offset3) = JsonbHeader::from_slice(3, &data).unwrap();
        assert_eq!(offset3, 3);
        assert_eq!(header3.0, ElementType::OBJECT);
        assert_eq!(header3.1, 0x9868); // 39000
    }

    #[test]
    fn test_unicode_escapes() {
        // Basic unicode escape
        let parsed = Jsonb::from_str(r#""\u00A9""#).unwrap(); // Copyright symbol
        assert_eq!(parsed.to_string().unwrap(), r#""\u00A9""#);

        // Non-BMP character (surrogate pair)
        let parsed = Jsonb::from_str(r#""\uD83D\uDE00""#).unwrap(); // Smiley emoji
        assert_eq!(parsed.to_string().unwrap(), r#""\uD83D\uDE00""#);
    }

    #[test]
    fn test_json5_comments() {
        // Line comments
        let parsed = Jsonb::from_str(
            r#"{
            // This is a line comment
            "key": "value"
        }"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Block comments
        let parsed = Jsonb::from_str(
            r#"{
            /* This is a
               block comment */
            "key": "value"
        }"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Comments inside array
        let parsed = Jsonb::from_str(
            r#"[1, // Comment
                                       2, /* Another comment */ 3]"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");
    }

    #[test]
    fn test_whitespace_handling() {
        // Various whitespace patterns
        let json_with_whitespace = r#"
        {
            "key1"    :    "value1"   ,
             "key2": [   1,    2,    3   ]  ,
            "key3":   {
                "nested"   :   true
            }
        }
        "#;

        let parsed = Jsonb::from_str(json_with_whitespace).unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"{"key1":"value1","key2":[1,2,3],"key3":{"nested":true}}"#
        );
    }

    #[test]
    fn test_binary_roundtrip() {
        // Test that binary data can be round-tripped through the JSONB format
        let original = r#"{"test":"value","array":[1,2,3]}"#;
        let parsed = Jsonb::from_str(original).unwrap();
        let binary_data = parsed.data.clone();

        // Create a new Jsonb from the binary data
        let from_binary = Jsonb::new(0, Some(&binary_data));
        assert_eq!(from_binary.to_string().unwrap(), original);
    }

    #[test]
    fn test_large_json() {
        // Generate a large JSON with many elements
        let mut large_array = String::from("[");
        for i in 0..1000 {
            large_array.push_str(&format!("{i}"));
            if i < 999 {
                large_array.push(',');
            }
        }
        large_array.push(']');

        let parsed = Jsonb::from_str(&large_array).unwrap();
        assert!(parsed.to_string().unwrap().starts_with("[0,1,2,"));
        assert!(parsed.to_string().unwrap().ends_with("998,999]"));
    }

    #[test]
    fn test_jsonb_is_valid() {
        // Valid JSONB
        let jsonb = Jsonb::from_str(r#"{"test":"value"}"#).unwrap();
        assert!(jsonb.is_valid().is_ok());

        // Invalid JSONB (manually corrupted)
        let mut invalid = jsonb.data.clone();
        if !invalid.is_empty() {
            invalid[0] = 0xFF; // Invalid element type
            let jsonb = Jsonb::new(0, Some(&invalid));
            assert!(jsonb.is_valid().is_err());
        }
    }

    #[test]
    fn test_special_characters_in_strings() {
        // Test handling of various special characters
        let json = r#"{
            "escaped_quotes": "He said \"Hello\"",
            "backslashes": "C:\\Windows\\System32",
            "control_chars": "\b\f\n\r\t",
            "unicode": "\u00A9 2023"
        }"#;

        let parsed = Jsonb::from_str(json).unwrap();
        let result = parsed.to_string().unwrap();

        assert!(result.contains(r#""escaped_quotes":"He said \"Hello\"""#));
        assert!(result.contains(r#""backslashes":"C:\\Windows\\System32""#));
        assert!(result.contains(r#""control_chars":"\b\f\n\r\t""#));
        assert!(result.contains(r#""unicode":"\u00A9 2023""#));
    }
}

#[cfg(test)]
mod path_operations_tests {
    use super::*;
    use crate::json::path::{JsonPath, PathElement};
    use std::borrow::Cow;

    // Helper function to create a simple JsonPath
    fn create_path(elements: Vec<PathElement>) -> JsonPath {
        JsonPath { elements }
    }

    #[test]
    fn test_navigate_root_path() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a path to the root
        let path = create_path(vec![PathElement::Root()]);

        // Navigate to the path
        let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

        // Verify navigation succeeds
        assert!(result.is_ok());
        let stack = result.unwrap();
        assert_eq!(stack.len(), 1);
        assert_eq!(stack[0].field_value_index, 0);
        assert_eq!(stack[0].field_key_index, JsonLocationKind::DocumentRoot);
    }

    #[test]
    fn test_navigate_object_property() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a path to the "name" property
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("name"), false),
        ]);

        // Navigate to the path
        let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

        // Verify navigation succeeds and points to the correct value
        assert!(result.is_ok());
        let stack = result.unwrap();
        assert_eq!(stack.len(), 2);

        // Verify we can get the value at this position
        let name_index = stack[1].field_value_index;
        let (header, header_size) = jsonb.read_header(name_index).unwrap();
        assert_eq!(header.0, ElementType::TEXT);

        // Extract the actual string value to verify
        let text_bytes = &jsonb.data[name_index + header_size..name_index + header_size + header.1];
        let text = std::str::from_utf8(text_bytes).unwrap();
        assert_eq!(text, "John");
    }

    #[test]
    fn test_navigate_nested_object_property() {
        let json_str = r#"{"person": {"name": "John", "age": 30}}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a path to the nested "name" property
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("person"), false),
            PathElement::Key(Cow::Borrowed("name"), false),
        ]);

        // Navigate to the path
        let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

        // Verify navigation succeeds
        assert!(result.is_ok());
        let stack = result.unwrap();
        assert_eq!(stack.len(), 3);
    }

    #[test]
    fn test_navigate_array_element() {
        let json_str = r#"{"items": [10, 20, 30]}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a path to the second array element (index 1)
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("items"), false),
            PathElement::ArrayLocator(Some(1)),
        ]);

        // Navigate to the path
        let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

        // Verify navigation succeeds
        assert!(result.is_ok());
        let stack = result.unwrap();
        assert_eq!(stack.len(), 2);

        // Verify we can get the value at the array position
        assert!(stack[1].has_specific_index());
        let array_element_index = stack[1].get_array_index().unwrap();
        let (header, header_size) = jsonb.read_header(array_element_index).unwrap();
        assert_eq!(header.0, ElementType::INT);

        // Extract the actual integer value to verify
        let int_bytes = &jsonb.data
            [array_element_index + header_size..array_element_index + header_size + header.1];
        let int_str = std::str::from_utf8(int_bytes).unwrap();
        assert_eq!(int_str, "20");
    }

    #[test]
    fn test_navigate_negative_array_index() {
        let json_str = r#"{"items": [10, 20, 30]}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a path to the last array element (index -1)
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("items"), false),
            PathElement::ArrayLocator(Some(-1)),
        ]);

        // Navigate to the path
        let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

        // Verify navigation succeeds
        assert!(result.is_ok());
        let stack = result.unwrap();
        assert_eq!(stack.len(), 2);

        // Verify we can get the value at the array position
        assert!(stack[1].has_specific_index());
    }

    #[test]
    fn test_set_operation() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a new value to set
        let new_value = Jsonb::from_str("\"Jane\"").unwrap();
        let mut operation = SetOperation::new(new_value);

        // Create a path to the "name" property
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("name"), false),
        ]);

        // Execute the operation
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Verify the value was updated
        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(updated_json, r#"{"name":"Jane","age":30}"#);
    }

    #[test]
    fn test_insert_operation() {
        let json_str = r#"{"name": "John"}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a new value to insert
        let new_value = Jsonb::from_str("30").unwrap();
        let mut operation = InsertOperation::new(new_value);

        // Create a path to a new "age" property
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("age"), false),
        ]);

        // Execute the operation
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Verify the value was inserted
        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(updated_json, r#"{"name":"John","age":30}"#);
    }

    #[test]
    fn test_delete_operation() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a delete operation
        let mut operation = DeleteOperation::new();

        // Create a path to the "age" property
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("age"), false),
        ]);

        // Execute the operation
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Verify the property was deleted
        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(updated_json, r#"{"name":"John"}"#);
    }

    #[test]
    fn test_replace_operation() {
        let json_str = r#"{"items": [10, 20, 30]}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a new value to replace with
        let new_value = Jsonb::from_str("50").unwrap();
        let mut operation = ReplaceOperation::new(new_value);

        // Create a path to the second array element (index 1)
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("items"), false),
            PathElement::ArrayLocator(Some(1)),
        ]);

        // Execute the operation
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Verify the value was replaced
        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(updated_json, r#"{"items":[10,50,30]}"#);
    }

    #[test]
    fn test_search_operation() {
        let json_str = r#"{"person": {"name": "John", "age": 30}}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a search operation
        let mut operation = SearchOperation::new(100);

        // Create a path to the "person" property
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("person"), false),
        ]);

        // Execute the operation
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Get the search result
        let search_result = operation.result();
        let result_str = search_result.to_string().unwrap();

        // Verify the search found the correct value
        assert_eq!(result_str, r#"{"name":"John","age":30}"#);
    }

    #[test]
    fn test_error_for_nonexistent_path() {
        let json_str = r#"{"name": "John", "age": 30}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a new value to set
        let new_value = Jsonb::from_str("\"Doe\"").unwrap();
        let mut operation = ReplaceOperation::new(new_value);

        // Create a path to a non-existent property with ReplaceExisting mode
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("surname"), false),
        ]);

        // Execute the operation - should fail because path doesn't exist
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_err());
    }

    #[test]
    fn test_deep_nested_path() {
        let json_str = r#"{"level1": {"level2": {"level3": {"value": 42}}}}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Create a new value to set
        let new_value = Jsonb::from_str("100").unwrap();
        let mut operation = SetOperation::new(new_value);

        // Create a deeply nested path
        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("level1"), false),
            PathElement::Key(Cow::Borrowed("level2"), false),
            PathElement::Key(Cow::Borrowed("level3"), false),
            PathElement::Key(Cow::Borrowed("value"), false),
        ]);

        // Execute the operation
        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Verify the deep value was updated
        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(
            updated_json,
            r#"{"level1":{"level2":{"level3":{"value":100}}}}"#
        );
    }

    #[test]
    fn test_path_modes() {
        // Test the different path operation modes

        // 1. ReplaceExisting mode - should fail when path doesn't exist
        let json_str = r#"{"name": "John"}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        let mut operation = SetOperation::new(Jsonb::from_str("30").unwrap());
        operation.mode = PathOperationMode::ReplaceExisting;

        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("age"), false),
        ]);

        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_err());

        // 2. InsertNew mode - should succeed for new paths
        let json_str = r#"{"name": "John"}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        let mut operation = InsertOperation::new(Jsonb::from_str("30").unwrap());
        operation.mode = PathOperationMode::InsertNew;

        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("age"), false),
        ]);

        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(updated_json, r#"{"name":"John","age":30}"#);

        // 3. InsertNew mode - should fail when path already exists
        let mut operation = InsertOperation::new(Jsonb::from_str("31").unwrap());
        operation.mode = PathOperationMode::InsertNew;

        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_err());

        // 4. Upsert mode - should work for both existing and new paths
        let json_str = r#"{"name": "John", "age": 30}"#;
        let mut jsonb = Jsonb::from_str(json_str).unwrap();

        // Update existing value with Upsert
        let mut operation = SetOperation::new(Jsonb::from_str("31").unwrap());
        operation.mode = PathOperationMode::Upsert;

        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("age"), false),
        ]);

        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        // Insert new value with Upsert
        let mut operation = SetOperation::new(Jsonb::from_str("\"Doe\"").unwrap());
        operation.mode = PathOperationMode::Upsert;

        let path = create_path(vec![
            PathElement::Root(),
            PathElement::Key(Cow::Borrowed("surname"), false),
        ]);

        let result = jsonb.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        let updated_json = jsonb.to_string().unwrap();
        assert_eq!(updated_json, r#"{"name":"John","age":31,"surname":"Doe"}"#);
    }
}
