use core::fmt;

use turso_parser::ast::Expr;

use crate::schema::{affinity::Affinity, collation::CollationSeq};

#[derive(Debug, Clone)]
pub struct Column {
    pub name: Option<String>,
    pub ty_str: String,
    pub default: Option<Box<Expr>>,
    raw: u16,
}

// flags
const F_PRIMARY_KEY: u16 = 1;
const F_ROWID_ALIAS: u16 = 2;
const F_NOTNULL: u16 = 4;
const F_UNIQUE: u16 = 8;
const F_HIDDEN: u16 = 16;

// pack Type and Collation in the remaining bits
const TYPE_SHIFT: u16 = 5;
const TYPE_MASK: u16 = 0b111 << TYPE_SHIFT;
const COLL_SHIFT: u16 = TYPE_SHIFT + 3;
const COLL_MASK: u16 = 0b11 << COLL_SHIFT;

impl Column {
    pub fn affinity(&self) -> Affinity {
        Affinity::affinity(&self.ty_str)
    }
    pub const fn new_default_text(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
    ) -> Self {
        Self::new(
            name,
            ty_str,
            default,
            Type::Text,
            None,
            false,
            false,
            false,
            false,
            false,
        )
    }
    pub const fn new_default_integer(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
    ) -> Self {
        Self::new(
            name,
            ty_str,
            default,
            Type::Integer,
            None,
            false,
            false,
            false,
            false,
            false,
        )
    }
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
        ty: Type,
        col: Option<CollationSeq>,
        primary_key: bool,
        rowid_alias: bool,
        notnull: bool,
        unique: bool,
        hidden: bool,
    ) -> Self {
        let mut raw = 0u16;
        raw |= (ty as u16) << TYPE_SHIFT;
        if let Some(c) = col {
            raw |= (c as u16) << COLL_SHIFT;
        }
        if primary_key {
            raw |= F_PRIMARY_KEY
        }
        if rowid_alias {
            raw |= F_ROWID_ALIAS
        }
        if notnull {
            raw |= F_NOTNULL
        }
        if unique {
            raw |= F_UNIQUE
        }
        if hidden {
            raw |= F_HIDDEN
        }
        Self {
            name,
            ty_str,
            default,
            raw,
        }
    }
    #[inline]
    pub const fn ty(&self) -> Type {
        let v = ((self.raw & TYPE_MASK) >> TYPE_SHIFT) as u8;
        Type::from_bits(v)
    }

    #[inline]
    pub const fn set_ty(&mut self, ty: Type) {
        self.raw = (self.raw & !TYPE_MASK) | (((ty as u16) << TYPE_SHIFT) & TYPE_MASK);
    }

    #[inline]
    pub const fn collation_opt(&self) -> Option<CollationSeq> {
        if self.has_explicit_collation() {
            Some(self.collation())
        } else {
            None
        }
    }

    #[inline]
    pub const fn collation(&self) -> CollationSeq {
        let v = ((self.raw & COLL_MASK) >> COLL_SHIFT) as u8;
        CollationSeq::from_bits(v)
    }

    #[inline]
    pub const fn has_explicit_collation(&self) -> bool {
        let v = ((self.raw & COLL_MASK) >> COLL_SHIFT) as u8;
        v != CollationSeq::Unset as u8
    }

    #[inline]
    pub const fn set_collation(&mut self, c: Option<CollationSeq>) {
        if let Some(c) = c {
            self.raw = (self.raw & !COLL_MASK) | (((c as u16) << COLL_SHIFT) & COLL_MASK);
        }
    }

    #[inline]
    pub fn primary_key(&self) -> bool {
        self.raw & F_PRIMARY_KEY != 0
    }
    #[inline]
    pub const fn is_rowid_alias(&self) -> bool {
        self.raw & F_ROWID_ALIAS != 0
    }
    #[inline]
    pub const fn notnull(&self) -> bool {
        self.raw & F_NOTNULL != 0
    }
    #[inline]
    pub const fn unique(&self) -> bool {
        self.raw & F_UNIQUE != 0
    }
    #[inline]
    pub const fn hidden(&self) -> bool {
        self.raw & F_HIDDEN != 0
    }

    #[inline]
    pub const fn set_primary_key(&mut self, v: bool) {
        self.set_flag(F_PRIMARY_KEY, v);
    }
    #[inline]
    pub const fn set_rowid_alias(&mut self, v: bool) {
        self.set_flag(F_ROWID_ALIAS, v);
    }
    #[inline]
    pub const fn set_notnull(&mut self, v: bool) {
        self.set_flag(F_NOTNULL, v);
    }
    #[inline]
    pub const fn set_unique(&mut self, v: bool) {
        self.set_flag(F_UNIQUE, v);
    }
    #[inline]
    pub const fn set_hidden(&mut self, v: bool) {
        self.set_flag(F_HIDDEN, v);
    }

    #[inline]
    const fn set_flag(&mut self, mask: u16, val: bool) {
        if val {
            self.raw |= mask
        } else {
            self.raw &= !mask
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Null = 0,
    Text = 1,
    Numeric = 2,
    Integer = 3,
    Real = 4,
    Blob = 5,
}

impl Type {
    #[inline]
    const fn from_bits(bits: u8) -> Self {
        match bits {
            0 => Type::Null,
            1 => Type::Text,
            2 => Type::Numeric,
            3 => Type::Integer,
            4 => Type::Real,
            5 => Type::Blob,
            _ => Type::Null,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Null => "",
            Self::Text => "TEXT",
            Self::Numeric => "NUMERIC",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
        };
        write!(f, "{s}")
    }
}
