use bitflags::bitflags;

bitflags! {
    /// Storage class flags that represent the possible storage classes an expression can yield.
    /// Used to combine column affinities across UNION/INTERSECT/EXCEPT arms.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StorageClassMask: u8 {
        const NUMERIC = 0x01;
        const TEXT = 0x02;
        const BLOB = 0x04;
    }
}

impl StorageClassMask {
    pub const fn from_numeric() -> Self {
        Self::NUMERIC
    }

    pub const fn from_text() -> Self {
        Self::TEXT
    }

    pub const fn from_blob() -> Self {
        Self::BLOB
    }

    pub const fn from_null() -> Self {
        Self::empty()
    }

    pub fn has_numeric(&self) -> bool {
        self.contains(Self::NUMERIC)
    }

    pub fn has_text(&self) -> bool {
        self.contains(Self::TEXT)
    }
}
