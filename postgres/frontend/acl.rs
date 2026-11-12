//! Access control lists, PostgreSQL's `aclitem[]`. The relation catalog
//! stores a relation's list with role oids, as PostgreSQL's `aclitem` holds
//! them, so renaming or dropping a role never has to rewrite a list;
//! `pg_class.relacl` shows it with role names, in PostgreSQL's text form:
//! `{owner=arwdDxtm/owner,reader=r/owner}`.

use std::fmt;

use turso_core::schema::RelationKind;
use turso_core::{parse_text_array_of_texts, text_array_literal, Value};
use turso_parser::ast;

/// The oid that stands for `PUBLIC`, every role, which the text form writes
/// as an empty name. PostgreSQL's `ACL_ID_PUBLIC`.
pub(crate) const PUBLIC_OID: i64 = 0;

bitflags::bitflags! {
    /// A set of privileges, each with PostgreSQL's letter.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub(crate) struct Privileges: u16 {
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const TRUNCATE = 1 << 4;
        const REFERENCES = 1 << 5;
        const TRIGGER = 1 << 6;
        const EXECUTE = 1 << 7;
        const USAGE = 1 << 8;
        const CREATE = 1 << 9;
        const TEMPORARY = 1 << 10;
        const CONNECT = 1 << 11;
        const SET = 1 << 12;
        const ALTER_SYSTEM = 1 << 13;
        const MAINTAIN = 1 << 14;
    }
}

/// Each privilege with its letter, in the order an `aclitem` lists them:
/// PostgreSQL's `ACL_ALL_RIGHTS_STR`.
const PRIVILEGE_LETTERS: [(Privileges, char); 15] = [
    (Privileges::INSERT, 'a'),
    (Privileges::SELECT, 'r'),
    (Privileges::UPDATE, 'w'),
    (Privileges::DELETE, 'd'),
    (Privileges::TRUNCATE, 'D'),
    (Privileges::REFERENCES, 'x'),
    (Privileges::TRIGGER, 't'),
    (Privileges::EXECUTE, 'X'),
    (Privileges::USAGE, 'U'),
    (Privileges::CREATE, 'C'),
    (Privileges::TEMPORARY, 'T'),
    (Privileges::CONNECT, 'c'),
    (Privileges::SET, 's'),
    (Privileges::ALTER_SYSTEM, 'A'),
    (Privileges::MAINTAIN, 'm'),
];

impl Privileges {
    /// The privileges a relation's owner has by default: PostgreSQL's
    /// `ACL_ALL_RIGHTS_RELATION`.
    pub(crate) const RELATION: Self = Self::INSERT
        .union(Self::SELECT)
        .union(Self::UPDATE)
        .union(Self::DELETE)
        .union(Self::TRUNCATE)
        .union(Self::REFERENCES)
        .union(Self::TRIGGER)
        .union(Self::MAINTAIN);

    /// The privileges a sequence's owner has by default: PostgreSQL's
    /// `ACL_ALL_RIGHTS_SEQUENCE`.
    pub(crate) const SEQUENCE: Self = Self::SELECT.union(Self::UPDATE).union(Self::USAGE);

    /// Every privilege that a relation of `kind` has.
    fn all_on(kind: RelationKind) -> Self {
        match kind {
            RelationKind::Sequence => Self::SEQUENCE,
            RelationKind::Table
            | RelationKind::View
            | RelationKind::MaterializedView
            | RelationKind::Index => Self::RELATION,
        }
    }

    /// The privileges that `GRANT privileges` gives on a relation of `kind`.
    /// As in PostgreSQL, `ALL` is every privilege of the kind, and a
    /// privilege the kind does not have, such as INSERT on a sequence, is
    /// left out.
    pub(crate) fn granted_on(kind: RelationKind, privileges: &[ast::Privilege]) -> Self {
        let all = Self::all_on(kind);
        let granted = privileges.iter().fold(Self::empty(), |granted, privilege| {
            granted
                | match privilege {
                    ast::Privilege::Select => Self::SELECT,
                    ast::Privilege::Insert => Self::INSERT,
                    ast::Privilege::Update => Self::UPDATE,
                    ast::Privilege::Delete => Self::DELETE,
                    ast::Privilege::Truncate => Self::TRUNCATE,
                    ast::Privilege::References => Self::REFERENCES,
                    ast::Privilege::Trigger => Self::TRIGGER,
                    ast::Privilege::Maintain => Self::MAINTAIN,
                    ast::Privilege::Usage => Self::USAGE,
                    ast::Privilege::All => all,
                }
        });
        granted & all
    }

    /// Reads the letters of an `aclitem`, such as `arwdDxtm`. A `*` after a
    /// letter, the grant option, is not stored, since GRANT cannot give it.
    fn from_letters(letters: &str) -> Option<Self> {
        let mut privileges = Self::empty();
        for letter in letters.chars() {
            let (privilege, _) = PRIVILEGE_LETTERS
                .iter()
                .find(|(_, known)| *known == letter)?;
            privileges |= *privilege;
        }
        Some(privileges)
    }
}

impl fmt::Display for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (privilege, letter) in PRIVILEGE_LETTERS {
            if self.contains(privilege) {
                write!(f, "{letter}")?;
            }
        }
        Ok(())
    }
}

/// One entry of a list: what `grantor` gave `grantee`, both role oids, the
/// grantee [`PUBLIC_OID`] for everyone.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AclItem {
    grantee: i64,
    privileges: Privileges,
    grantor: i64,
}

impl AclItem {
    /// The `aclitem` text, `grantee=privileges/grantor`, naming the roles as
    /// `role_name` does.
    fn text(&self, role_name: &dyn Fn(i64) -> String) -> String {
        let grantee = if self.grantee == PUBLIC_OID {
            String::new()
        } else {
            role_name(self.grantee)
        };
        format!("{grantee}={}/{}", self.privileges, role_name(self.grantor))
    }

    fn parse(text: &str) -> Option<Self> {
        let (grantee, rest) = text.split_once('=')?;
        let (letters, grantor) = rest.split_once('/')?;
        Some(Self {
            grantee: if grantee.is_empty() {
                PUBLIC_OID
            } else {
                grantee.parse().ok()?
            },
            privileges: Privileges::from_letters(letters)?,
            grantor: grantor.parse().ok()?,
        })
    }
}

/// Writes a role name as PostgreSQL's `putid` does: quoted when it has a
/// character other than a letter, digit or underscore, with `"` doubled.
pub(crate) fn acl_role_name(name: &str) -> String {
    if name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return name.to_string();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The access control list of a relation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Acl {
    items: Vec<AclItem>,
}

impl Acl {
    /// The list a relation has before any `GRANT`, which PostgreSQL's
    /// `acldefault` gives: the owner holds every privilege of the kind.
    pub(crate) fn default_for(kind: RelationKind, owner: i64) -> Self {
        Self {
            items: vec![AclItem {
                grantee: owner,
                privileges: Privileges::all_on(kind),
                grantor: owner,
            }],
        }
    }

    /// Reads the stored form, which [`Acl::stored_text`] writes.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let items = parse_text_array_of_texts(text)?
            .iter()
            .map(|element| match element {
                Value::Text(item) => AclItem::parse(item.as_str()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { items })
    }

    /// The list as the relation catalog stores it, with role oids.
    pub(crate) fn stored_text(&self) -> String {
        self.text(&|oid| oid.to_string())
    }

    /// The list as `pg_class.relacl` shows it, with the role names that
    /// `role_name` gives, quoted as PostgreSQL quotes them.
    pub(crate) fn display_text(&self, role_name: &dyn Fn(i64) -> String) -> String {
        self.text(&|oid| acl_role_name(&role_name(oid)))
    }

    fn text(&self, role_name: &dyn Fn(i64) -> String) -> String {
        let items: Vec<String> = self.items.iter().map(|item| item.text(role_name)).collect();
        text_array_literal(items.iter().map(String::as_str))
    }

    /// Adds `granted` from `grantor` for `grantee`, into the entry the two
    /// already share or a new one at the end, as PostgreSQL's
    /// `merge_acl_with_grant` does.
    pub(crate) fn grant(&mut self, grantee: i64, granted: Privileges, grantor: i64) {
        if granted.is_empty() {
            return;
        }
        let item = self
            .items
            .iter_mut()
            .find(|item| item.grantee == grantee && item.grantor == grantor);
        match item {
            Some(item) => item.privileges |= granted,
            None => self.items.push(AclItem {
                grantee,
                privileges: granted,
                grantor,
            }),
        }
    }

    /// Adds every entry of `other`, as [`Acl::grant`] adds one.
    pub(crate) fn merge(&mut self, other: &Acl) {
        for item in &other.items {
            self.grant(item.grantee, item.privileges, item.grantor);
        }
    }

    /// Orders the entries by grantee, then grantor, then privileges. As in
    /// PostgreSQL, default privileges, and the lists they give new
    /// relations, are kept in this order.
    pub(crate) fn sort(&mut self) {
        self.items
            .sort_by_key(|item| (item.grantee, item.grantor, item.privileges.bits()));
    }

    /// Whether the list gives `role`, by oid or through `PUBLIC`, every
    /// privilege of `wanted`.
    pub(crate) fn grants(&self, role: i64, wanted: Privileges) -> bool {
        self.items
            .iter()
            .filter(|item| item.grantee == role || item.grantee == PUBLIC_OID)
            .fold(Privileges::empty(), |given, item| given | item.privileges)
            .contains(wanted)
    }

    /// Whether the list gives `role`, by oid or through `PUBLIC`, any
    /// privilege at all.
    pub(crate) fn gives_anything_to(&self, role: i64) -> bool {
        self.items.iter().any(|item| {
            (item.grantee == role || item.grantee == PUBLIC_OID) && !item.privileges.is_empty()
        })
    }

    /// Makes `new_owner` stand wherever `old_owner` stood, as grantee and as
    /// grantor, as PostgreSQL's `aclnewowner` does. Two entries that become
    /// the same pair merge into the earlier one.
    pub(crate) fn change_owner(&mut self, old_owner: i64, new_owner: i64) {
        for item in &mut self.items {
            if item.grantee == old_owner {
                item.grantee = new_owner;
            }
            if item.grantor == old_owner {
                item.grantor = new_owner;
            }
        }
        let mut merged: Vec<AclItem> = Vec::with_capacity(self.items.len());
        for item in self.items.drain(..) {
            match merged
                .iter_mut()
                .find(|earlier| earlier.grantee == item.grantee && earlier.grantor == item.grantor)
            {
                Some(earlier) => earlier.privileges |= item.privileges,
                None => merged.push(item),
            }
        }
        self.items = merged;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: i64 = 10;
    const READER: i64 = 16385;
    const STRANGER: i64 = 16386;

    fn names(oid: i64) -> String {
        match oid {
            OWNER => "postgres".to_string(),
            READER => "reader".to_string(),
            STRANGER => "my role".to_string(),
            other => other.to_string(),
        }
    }

    #[test]
    fn the_default_list_gives_the_owner_everything() {
        assert_eq!(
            Acl::default_for(RelationKind::Table, OWNER).display_text(&names),
            "{postgres=arwdDxtm/postgres}"
        );
        assert_eq!(
            Acl::default_for(RelationKind::Sequence, OWNER).stored_text(),
            "{10=rwU/10}"
        );
    }

    #[test]
    fn a_grant_adds_an_entry_once() {
        let mut acl = Acl::default_for(RelationKind::Table, OWNER);
        acl.grant(READER, Privileges::SELECT, OWNER);
        acl.grant(READER, Privileges::SELECT, OWNER);
        acl.grant(PUBLIC_OID, Privileges::SELECT, OWNER);
        acl.grant(OWNER, Privileges::SELECT, OWNER);
        assert_eq!(acl.stored_text(), "{10=arwdDxtm/10,16385=r/10,=r/10}");
        assert_eq!(
            acl.display_text(&names),
            "{postgres=arwdDxtm/postgres,reader=r/postgres,=r/postgres}"
        );
    }

    #[test]
    fn grant_gives_the_privileges_that_the_kind_of_relation_has() {
        let letters = |kind, privileges: &[ast::Privilege]| {
            Privileges::granted_on(kind, privileges).to_string()
        };
        assert_eq!(
            letters(RelationKind::Table, &[ast::Privilege::All]),
            "arwdDxtm"
        );
        assert_eq!(
            letters(RelationKind::Sequence, &[ast::Privilege::All]),
            "rwU"
        );
        assert_eq!(
            letters(
                RelationKind::Table,
                &[ast::Privilege::Update, ast::Privilege::Select]
            ),
            "rw"
        );
        assert_eq!(
            letters(
                RelationKind::Sequence,
                &[ast::Privilege::Insert, ast::Privilege::Update]
            ),
            "w"
        );
    }

    #[test]
    fn the_list_gives_a_role_its_own_privileges_and_those_of_public() {
        let mut acl = Acl::default_for(RelationKind::Table, OWNER);
        acl.grant(READER, Privileges::SELECT, OWNER);
        acl.grant(PUBLIC_OID, Privileges::INSERT, OWNER);
        assert!(acl.grants(READER, Privileges::SELECT | Privileges::INSERT));
        assert!(!acl.grants(READER, Privileges::SELECT | Privileges::UPDATE));
        assert!(acl.grants(STRANGER, Privileges::INSERT));
        assert!(!acl.grants(STRANGER, Privileges::SELECT));
    }

    #[test]
    fn the_list_says_whom_it_gives_privileges() {
        let mut acl = Acl::default_for(RelationKind::Table, OWNER);
        assert!(acl.gives_anything_to(OWNER));
        assert!(acl.grants(OWNER, Privileges::SELECT));
        assert!(!acl.gives_anything_to(READER));
        assert!(!acl.grants(READER, Privileges::SELECT));
        acl.grant(READER, Privileges::SELECT, OWNER);
        assert!(acl.gives_anything_to(READER));
        assert!(acl.grants(READER, Privileges::SELECT));
        assert!(!acl.gives_anything_to(STRANGER));
        acl.grant(PUBLIC_OID, Privileges::SELECT, OWNER);
        assert!(acl.gives_anything_to(STRANGER));
        assert!(acl.grants(STRANGER, Privileges::SELECT));
    }

    #[test]
    fn a_new_owner_takes_the_old_owners_place() {
        let mut acl = Acl::default_for(RelationKind::Table, OWNER);
        acl.grant(READER, Privileges::SELECT, OWNER);
        acl.grant(STRANGER, Privileges::SELECT, OWNER);
        acl.change_owner(OWNER, STRANGER);
        assert_eq!(acl.stored_text(), "{16386=arwdDxtm/16386,16385=r/16386}");
    }

    #[test]
    fn the_stored_form_round_trips() {
        for text in [
            "{}",
            "{10=arwdDxtm/10}",
            "{10=arwdDxtm/10,16385=r/10,=r/10}",
            "{16385=rwU/16385}",
        ] {
            let acl = Acl::parse(text).unwrap_or_else(|| panic!("{text} does not parse"));
            assert_eq!(acl.stored_text(), text);
        }
    }

    #[test]
    fn names_are_quoted_as_postgresql_quotes_them() {
        let mut acl = Acl::default_for(RelationKind::Table, STRANGER);
        acl.grant(READER, Privileges::SELECT, STRANGER);
        assert_eq!(
            acl.display_text(&names),
            "{\"\\\"my role\\\"=arwdDxtm/\\\"my role\\\"\",\"reader=r/\\\"my role\\\"\"}"
        );
        assert_eq!(acl_role_name("Bob"), "Bob");
        assert_eq!(acl_role_name("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn malformed_text_does_not_parse() {
        for text in [
            "",
            "10=r/10",
            "{10=r}",
            "{10=q/10}",
            "{reader=r/10}",
            "{10=r/postgres}",
            "{\"unterminated=r/10}",
        ] {
            assert!(Acl::parse(text).is_none(), "{text} parses");
        }
    }
}
