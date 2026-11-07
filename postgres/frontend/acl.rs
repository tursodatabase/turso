//! Access control lists in the text form of PostgreSQL's `aclitem[]`, as
//! `pg_class.relacl` shows them: `{owner=arwdDxtm/owner,reader=r/owner}`.
//! The relation catalog stores the lists in this form, so that `pg_class`
//! can show them as they are and tools such as `pg_dump` read them as
//! PostgreSQL's.

use std::fmt;

use turso_core::schema::RelationKind;
use turso_parser::ast::Privilege;

/// PostgreSQL's `ACL_ALL_RIGHTS_STR`: every privilege letter, in the order
/// an `aclitem` lists them.
const PRIVILEGE_LETTERS: &str = "arwdDxtXUCTcsAm";

/// The privileges a relation's owner has by default: PostgreSQL's
/// `ACL_ALL_RIGHTS_RELATION`.
const RELATION_PRIVILEGES: &str = "arwdDxtm";

/// The privileges a sequence's owner has by default: PostgreSQL's
/// `ACL_ALL_RIGHTS_SEQUENCE`.
const SEQUENCE_PRIVILEGES: &str = "rwU";

/// The grantee of `PUBLIC`, which an `aclitem` writes as an empty name.
pub(crate) const PUBLIC_GRANTEE: &str = "";

/// A set of privileges, one bit per letter of [`PRIVILEGE_LETTERS`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Privileges {
    granted: u16,
    /// The privileges held `WITH GRANT OPTION`, written with a `*`.
    grantable: u16,
}

impl Privileges {
    fn from_letters(letters: &str) -> Option<Self> {
        let mut privileges = Self::default();
        let mut last_bit = None;
        for letter in letters.chars() {
            if letter == '*' {
                privileges.grantable |= last_bit?;
                continue;
            }
            let bit = 1 << PRIVILEGE_LETTERS.find(letter)?;
            privileges.granted |= bit;
            last_bit = Some(bit);
        }
        Some(privileges)
    }

    fn of(privilege: Privilege) -> Self {
        let letter = match privilege {
            Privilege::Select => 'r',
        };
        Self::from_letters(&letter.to_string()).expect("a privilege has a letter")
    }

    fn union(self, other: Self) -> Self {
        Self {
            granted: self.granted | other.granted,
            grantable: self.grantable | other.grantable,
        }
    }
}

impl fmt::Display for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, letter) in PRIVILEGE_LETTERS.chars().enumerate() {
            let bit = 1 << index;
            if self.granted & bit != 0 {
                write!(f, "{letter}")?;
                if self.grantable & bit != 0 {
                    write!(f, "*")?;
                }
            }
        }
        Ok(())
    }
}

/// One entry of a list: what `grantor` gave `grantee`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AclItem {
    /// A role name, or [`PUBLIC_GRANTEE`].
    grantee: String,
    privileges: Privileges,
    grantor: String,
}

impl fmt::Display for AclItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_role_name(f, &self.grantee)?;
        write!(f, "={}/", self.privileges)?;
        write_role_name(f, &self.grantor)
    }
}

/// Writes `name` as PostgreSQL's `putid` does: quoted when it has a
/// character other than a letter, digit or underscore, with `"` doubled.
fn write_role_name(f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
    let plain = name
        .chars()
        .all(|letter| letter.is_ascii_alphanumeric() || letter == '_');
    if plain {
        return write!(f, "{name}");
    }
    write!(f, "\"{}\"", name.replace('"', "\"\""))
}

/// Reads a name written by [`write_role_name`] from the start of `text`,
/// returning it and the rest of the text.
fn read_role_name(text: &str) -> Option<(String, &str)> {
    let Some(mut rest) = text.strip_prefix('"') else {
        let end = text
            .find(|letter: char| !(letter.is_ascii_alphanumeric() || letter == '_'))
            .unwrap_or(text.len());
        return Some((text[..end].to_string(), &text[end..]));
    };
    let mut name = String::new();
    loop {
        let quote = rest.find('"')?;
        name.push_str(&rest[..quote]);
        rest = &rest[quote + 1..];
        match rest.strip_prefix('"') {
            Some(after_doubled_quote) => {
                name.push('"');
                rest = after_doubled_quote;
            }
            None => return Some((name, rest)),
        }
    }
}

/// The access control list of a relation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Acl {
    items: Vec<AclItem>,
}

impl Acl {
    /// The list a relation has before any `GRANT`, which PostgreSQL's
    /// `acldefault` gives: the owner holds every privilege of the kind.
    pub(crate) fn default_for(kind: RelationKind, owner: &str) -> Self {
        let letters = match kind {
            RelationKind::Sequence => SEQUENCE_PRIVILEGES,
            RelationKind::Table
            | RelationKind::View
            | RelationKind::MaterializedView
            | RelationKind::Index => RELATION_PRIVILEGES,
        };
        Self {
            items: vec![AclItem {
                grantee: owner.to_string(),
                privileges: Privileges::from_letters(letters).expect("the default letters"),
                grantor: owner.to_string(),
            }],
        }
    }

    /// Parses the text form, `{item,...}`.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let inner = text.strip_prefix('{')?.strip_suffix('}')?;
        let mut items = Vec::new();
        for element in split_array_elements(inner)? {
            let (grantee, rest) = read_role_name(&element)?;
            let rest = rest.strip_prefix('=')?;
            let (letters, grantor) = rest.split_once('/')?;
            let privileges = Privileges::from_letters(letters)?;
            let (grantor, rest) = read_role_name(grantor)?;
            if !rest.is_empty() {
                return None;
            }
            items.push(AclItem {
                grantee,
                privileges,
                grantor,
            });
        }
        Some(Self { items })
    }

    /// Adds `privileges` from `grantor` for `grantee`, into the entry the two
    /// already share or a new one at the end, as PostgreSQL's
    /// `merge_acl_with_grant` does.
    pub(crate) fn grant(&mut self, grantee: &str, privileges: &[Privilege], grantor: &str) {
        let granted = privileges
            .iter()
            .fold(Privileges::default(), |all, privilege| {
                all.union(Privileges::of(*privilege))
            });
        let item = self
            .items
            .iter_mut()
            .find(|item| item.grantee == grantee && item.grantor == grantor);
        match item {
            Some(item) => item.privileges = item.privileges.union(granted),
            None => self.items.push(AclItem {
                grantee: grantee.to_string(),
                privileges: granted,
                grantor: grantor.to_string(),
            }),
        }
    }

    /// Whether the list gives `role`, by name or through `PUBLIC`,
    /// `privilege`.
    pub(crate) fn grants(&self, role: &str, privilege: Privilege) -> bool {
        let wanted = Privileges::of(privilege).granted;
        self.items.iter().any(|item| {
            (item.grantee == role || item.grantee == PUBLIC_GRANTEE)
                && item.privileges.granted & wanted == wanted
        })
    }

    /// Whether the list gives `role`, by name or through `PUBLIC`, any
    /// privilege at all.
    pub(crate) fn gives_anything_to(&self, role: &str) -> bool {
        self.items.iter().any(|item| {
            (item.grantee == role || item.grantee == PUBLIC_GRANTEE)
                && item.privileges != Privileges::default()
        })
    }

    /// Makes `new_owner` stand wherever `old_owner` stood, as grantee and as
    /// grantor, as PostgreSQL's `aclnewowner` does. Two entries that become
    /// the same pair merge into the earlier one.
    pub(crate) fn change_owner(&mut self, old_owner: &str, new_owner: &str) {
        for item in &mut self.items {
            if item.grantee == old_owner {
                item.grantee = new_owner.to_string();
            }
            if item.grantor == old_owner {
                item.grantor = new_owner.to_string();
            }
        }
        let mut merged: Vec<AclItem> = Vec::with_capacity(self.items.len());
        for item in self.items.drain(..) {
            match merged
                .iter_mut()
                .find(|earlier| earlier.grantee == item.grantee && earlier.grantor == item.grantor)
            {
                Some(earlier) => earlier.privileges = earlier.privileges.union(item.privileges),
                None => merged.push(item),
            }
        }
        self.items = merged;
    }
}

impl fmt::Display for Acl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{")?;
        for (index, item) in self.items.iter().enumerate() {
            if index > 0 {
                write!(f, ",")?;
            }
            write_array_element(f, &item.to_string())?;
        }
        write!(f, "}}")
    }
}

/// Writes one element of an array's text form, quoted when PostgreSQL's
/// `array_out` would quote it.
fn write_array_element(f: &mut fmt::Formatter<'_>, element: &str) -> fmt::Result {
    let needs_quotes = element.is_empty()
        || element.eq_ignore_ascii_case("NULL")
        || element
            .chars()
            .any(|letter| matches!(letter, '"' | '\\' | '{' | '}' | ',') || letter.is_whitespace());
    if !needs_quotes {
        return write!(f, "{element}");
    }
    write!(f, "\"")?;
    for letter in element.chars() {
        if matches!(letter, '"' | '\\') {
            write!(f, "\\")?;
        }
        write!(f, "{letter}")?;
    }
    write!(f, "\"")
}

/// Splits the inside of an array's text form into its elements, undoing the
/// quoting of [`write_array_element`].
fn split_array_elements(inner: &str) -> Option<Vec<String>> {
    let mut elements = Vec::new();
    if inner.is_empty() {
        return Some(elements);
    }
    let mut letters = inner.chars().peekable();
    loop {
        let mut element = String::new();
        if letters.peek() == Some(&'"') {
            letters.next();
            loop {
                match letters.next()? {
                    '"' => break,
                    '\\' => element.push(letters.next()?),
                    letter => element.push(letter),
                }
            }
        } else {
            while let Some(&letter) = letters.peek() {
                if letter == ',' {
                    break;
                }
                element.push(letter);
                letters.next();
            }
        }
        elements.push(element);
        match letters.next() {
            None => return Some(elements),
            Some(',') => continue,
            Some(_) => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_list_gives_the_owner_everything() {
        assert_eq!(
            Acl::default_for(RelationKind::Table, "postgres").to_string(),
            "{postgres=arwdDxtm/postgres}"
        );
        assert_eq!(
            Acl::default_for(RelationKind::Sequence, "alice").to_string(),
            "{alice=rwU/alice}"
        );
    }

    #[test]
    fn a_grant_adds_an_entry_once() {
        let mut acl = Acl::default_for(RelationKind::Table, "postgres");
        acl.grant("reader", &[Privilege::Select], "postgres");
        acl.grant("reader", &[Privilege::Select], "postgres");
        acl.grant(PUBLIC_GRANTEE, &[Privilege::Select], "postgres");
        acl.grant("postgres", &[Privilege::Select], "postgres");
        assert_eq!(
            acl.to_string(),
            "{postgres=arwdDxtm/postgres,reader=r/postgres,=r/postgres}"
        );
    }

    #[test]
    fn the_list_says_whom_it_gives_privileges() {
        let mut acl = Acl::default_for(RelationKind::Table, "postgres");
        assert!(acl.gives_anything_to("postgres"));
        assert!(acl.grants("postgres", Privilege::Select));
        assert!(!acl.gives_anything_to("reader"));
        assert!(!acl.grants("reader", Privilege::Select));
        acl.grant("reader", &[Privilege::Select], "postgres");
        assert!(acl.gives_anything_to("reader"));
        assert!(acl.grants("reader", Privilege::Select));
        assert!(!acl.gives_anything_to("stranger"));
        acl.grant(PUBLIC_GRANTEE, &[Privilege::Select], "postgres");
        assert!(acl.gives_anything_to("stranger"));
        assert!(acl.grants("stranger", Privilege::Select));
    }

    #[test]
    fn a_new_owner_takes_the_old_owners_place() {
        let mut acl = Acl::default_for(RelationKind::Table, "postgres");
        acl.grant("reader", &[Privilege::Select], "postgres");
        acl.grant("alice", &[Privilege::Select], "postgres");
        acl.change_owner("postgres", "alice");
        assert_eq!(acl.to_string(), "{alice=arwdDxtm/alice,reader=r/alice}");
    }

    #[test]
    fn the_text_form_round_trips_with_quoting() {
        for text in [
            "{}",
            "{postgres=arwdDxtm/postgres}",
            "{postgres=arwdDxtm/postgres,reader=r*/postgres,=r/postgres}",
            "{Bob=r/postgres}",
            "{\"\\\"my role\\\"=r/postgres\"}",
            "{\"\\\"say \\\"\\\"hi\\\"\\\"\\\"=r/postgres\"}",
            "{\"\\\"a,b\\\"=r/postgres\"}",
        ] {
            let acl = Acl::parse(text).unwrap_or_else(|| panic!("{text} does not parse"));
            assert_eq!(acl.to_string(), text);
        }
        let mut acl = Acl::default_for(RelationKind::Table, "my role");
        acl.grant("say \"hi\"", &[Privilege::Select], "my role");
        let text = acl.to_string();
        assert_eq!(
            text,
            "{\"\\\"my role\\\"=arwdDxtm/\\\"my role\\\"\",\"\\\"say \\\"\\\"hi\\\"\\\"\\\"=r/\\\"my role\\\"\"}"
        );
        assert_eq!(Acl::parse(&text).unwrap(), acl);
    }

    #[test]
    fn malformed_text_does_not_parse() {
        for text in [
            "",
            "postgres=r/postgres",
            "{postgres=r}",
            "{postgres=q/postgres}",
            "{postgres=r/postgres x}",
            "{\"unterminated=r/postgres}",
            "{postgres=*r/postgres}",
        ] {
            assert!(Acl::parse(text).is_none(), "{text} parses");
        }
    }
}
