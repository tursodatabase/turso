//! Privileges and access control lists.
//!
//! A privilege is a right to do one kind of action on one object, such as
//! reading a table. Which privileges an object can have depends on its kind:
//! a table can be read, written and have triggers, a function can be
//! executed, a schema can be used and created in.
//!
//! An access control list (ACL) records which role holds which privileges
//! on one object, and who granted them. An object without a stored list has
//! the default list of its kind: its owner holds every privilege and may
//! grant them, and `public` holds what every role may do with such an object
//! by default, such as executing a function.

use std::fmt;

use crate::security::roles::RoleId;
use crate::{LimboError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Privilege {
    Select = 1 << 0,
    Insert = 1 << 1,
    Update = 1 << 2,
    Delete = 1 << 3,
    Truncate = 1 << 4,
    References = 1 << 5,
    Trigger = 1 << 6,
    Execute = 1 << 7,
    Usage = 1 << 8,
    Create = 1 << 9,
    Temporary = 1 << 10,
    Set = 1 << 11,
}

impl Privilege {
    const ALL: [Privilege; 12] = [
        Privilege::Select,
        Privilege::Insert,
        Privilege::Update,
        Privilege::Delete,
        Privilege::Truncate,
        Privilege::References,
        Privilege::Trigger,
        Privilege::Execute,
        Privilege::Usage,
        Privilege::Create,
        Privilege::Temporary,
        Privilege::Set,
    ];

    /// The letter that stands for the privilege in a stored list. They are
    /// the letters PostgreSQL prints in `aclitem` values.
    pub fn code(self) -> char {
        match self {
            Privilege::Select => 'r',
            Privilege::Insert => 'a',
            Privilege::Update => 'w',
            Privilege::Delete => 'd',
            Privilege::Truncate => 'D',
            Privilege::References => 'x',
            Privilege::Trigger => 't',
            Privilege::Execute => 'X',
            Privilege::Usage => 'U',
            Privilege::Create => 'C',
            Privilege::Temporary => 'T',
            Privilege::Set => 's',
        }
    }

    /// The privilege called `name` in SQL, in any case.
    pub fn from_name(name: &str) -> Option<Privilege> {
        Privilege::ALL
            .into_iter()
            .find(|privilege| privilege.name().eq_ignore_ascii_case(name))
    }

    pub fn from_code(code: char) -> Option<Privilege> {
        Privilege::ALL
            .into_iter()
            .find(|privilege| privilege.code() == code)
    }

    pub fn name(self) -> &'static str {
        match self {
            Privilege::Select => "SELECT",
            Privilege::Insert => "INSERT",
            Privilege::Update => "UPDATE",
            Privilege::Delete => "DELETE",
            Privilege::Truncate => "TRUNCATE",
            Privilege::References => "REFERENCES",
            Privilege::Trigger => "TRIGGER",
            Privilege::Execute => "EXECUTE",
            Privilege::Usage => "USAGE",
            Privilege::Create => "CREATE",
            Privilege::Temporary => "TEMPORARY",
            Privilege::Set => "SET",
        }
    }
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A set of privileges.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Privileges(u16);

impl Privileges {
    pub const NONE: Privileges = Privileges(0);

    pub fn of(privilege: Privilege) -> Privileges {
        Privileges(privilege as u16)
    }

    pub fn contains(self, privilege: Privilege) -> bool {
        self.0 & privilege as u16 != 0
    }

    pub fn contains_all(self, other: Privileges) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn with(self, privilege: Privilege) -> Privileges {
        Privileges(self.0 | privilege as u16)
    }

    pub fn union(self, other: Privileges) -> Privileges {
        Privileges(self.0 | other.0)
    }

    pub fn intersection(self, other: Privileges) -> Privileges {
        Privileges(self.0 & other.0)
    }

    pub fn difference(self, other: Privileges) -> Privileges {
        Privileges(self.0 & !other.0)
    }

    pub fn iter(self) -> impl Iterator<Item = Privilege> {
        Privilege::ALL
            .into_iter()
            .filter(move |privilege| self.contains(*privilege))
    }
}

impl FromIterator<Privilege> for Privileges {
    fn from_iter<I: IntoIterator<Item = Privilege>>(privileges: I) -> Self {
        privileges
            .into_iter()
            .fold(Privileges::NONE, Privileges::with)
    }
}

impl fmt::Debug for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

/// The kinds of object that privileges apply to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    Table,
    View,
    Sequence,
    Function,
    Schema,
    Database,
    Type,
    Setting,
}

impl ObjectKind {
    /// Every privilege an object of this kind can have.
    pub fn all_privileges(self) -> Privileges {
        let privileges: &[Privilege] = match self {
            ObjectKind::Table | ObjectKind::View => &[
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
                Privilege::Truncate,
                Privilege::References,
                Privilege::Trigger,
            ][..],
            ObjectKind::Sequence => &[Privilege::Usage, Privilege::Select, Privilege::Update],
            ObjectKind::Function => &[Privilege::Execute],
            ObjectKind::Schema => &[Privilege::Usage, Privilege::Create],
            ObjectKind::Database => &[Privilege::Create, Privilege::Temporary],
            ObjectKind::Type => &[Privilege::Usage],
            ObjectKind::Setting => &[Privilege::Select, Privilege::Set],
        };
        Privileges::from_iter(privileges.iter().copied())
    }

    /// What every role may do with a new object of this kind until its owner
    /// revokes it: execute a function, use a type, and nothing with stored
    /// data or settings.
    pub fn default_public_privileges(self) -> Privileges {
        match self {
            ObjectKind::Function => Privileges::of(Privilege::Execute),
            ObjectKind::Type => Privileges::of(Privilege::Usage),
            ObjectKind::Database => Privileges::of(Privilege::Temporary),
            ObjectKind::Table
            | ObjectKind::View
            | ObjectKind::Sequence
            | ObjectKind::Schema
            | ObjectKind::Setting => Privileges::NONE,
        }
    }
}

/// The privileges one role holds on one object, and who granted them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEntry {
    pub grantee: RoleId,
    pub privileges: Privileges,
    /// The privileges the grantee may grant to other roles.
    pub grant_option: Privileges,
    pub grantor: RoleId,
}

/// The privileges every role holds on one object.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Acl {
    entries: Vec<AclEntry>,
}

impl Acl {
    pub fn empty() -> Acl {
        Acl::default()
    }

    /// The list an object of `kind` has when nothing was granted or revoked:
    /// the owner holds every privilege and may grant them, and `public`
    /// holds the kind's default privileges.
    pub fn default_for(kind: ObjectKind, owner: RoleId) -> Acl {
        let mut acl = Acl::empty();
        let all = kind.all_privileges();
        acl.grant(owner, all, all, owner);
        acl.grant(
            RoleId::PUBLIC,
            kind.default_public_privileges(),
            Privileges::NONE,
            owner,
        );
        acl
    }

    pub fn entries(&self) -> &[AclEntry] {
        &self.entries
    }

    /// The privileges `role` holds, including those granted to `public`.
    pub fn privileges_of(&self, role: RoleId) -> Privileges {
        self.entries
            .iter()
            .filter(|entry| entry.grantee == role || entry.grantee == RoleId::PUBLIC)
            .fold(Privileges::NONE, |held, entry| held.union(entry.privileges))
    }

    /// The privileges `role` may grant to others, including those granted
    /// to `public` with grant option.
    pub fn grant_options_of(&self, role: RoleId) -> Privileges {
        self.entries
            .iter()
            .filter(|entry| entry.grantee == role || entry.grantee == RoleId::PUBLIC)
            .fold(Privileges::NONE, |held, entry| {
                held.union(entry.grant_option)
            })
    }

    /// Adds `privileges`, and the right to grant `grant_option` of them, to
    /// what `grantee` holds from `grantor`.
    pub fn grant(
        &mut self,
        grantee: RoleId,
        privileges: Privileges,
        grant_option: Privileges,
        grantor: RoleId,
    ) {
        let grant_option = grant_option.intersection(privileges);
        if privileges.is_empty() {
            return;
        }
        match self
            .entries
            .iter_mut()
            .find(|entry| entry.grantee == grantee && entry.grantor == grantor)
        {
            Some(entry) => {
                entry.privileges = entry.privileges.union(privileges);
                entry.grant_option = entry.grant_option.union(grant_option);
            }
            None => self.entries.push(AclEntry {
                grantee,
                privileges,
                grant_option,
                grantor,
            }),
        }
    }

    /// Removes `privileges` from what `grantee` holds from `grantor`. With
    /// `grant_option_only`, the grantee keeps the privileges and loses only
    /// the right to grant them.
    pub fn revoke(
        &mut self,
        grantee: RoleId,
        privileges: Privileges,
        grant_option_only: bool,
        grantor: RoleId,
    ) {
        for entry in &mut self.entries {
            if entry.grantee != grantee || entry.grantor != grantor {
                continue;
            }
            entry.grant_option = entry.grant_option.difference(privileges);
            if !grant_option_only {
                entry.privileges = entry.privileges.difference(privileges);
            }
        }
        self.entries.retain(|entry| !entry.privileges.is_empty());
    }

    /// Reads a list written by [`Acl::to_stored`].
    pub fn from_stored(text: &str) -> Result<Acl> {
        let invalid = || LimboError::Corrupt(format!("invalid access control list: {text}"));
        let inner = text
            .strip_prefix('{')
            .and_then(|rest| rest.strip_suffix('}'))
            .ok_or_else(invalid)?;
        let mut acl = Acl::empty();
        if inner.is_empty() {
            return Ok(acl);
        }
        for item in inner.split(',') {
            let (grantee, rest) = item.split_once('=').ok_or_else(invalid)?;
            let (codes, grantor) = rest.split_once('/').ok_or_else(invalid)?;
            let grantee = parse_role_id(grantee).ok_or_else(invalid)?;
            let grantor = parse_role_id(grantor).ok_or_else(invalid)?;
            let mut privileges = Privileges::NONE;
            let mut grant_option = Privileges::NONE;
            let mut last = None;
            for code in codes.chars() {
                if code == '*' {
                    let privilege = last.ok_or_else(invalid)?;
                    grant_option = grant_option.with(privilege);
                    last = None;
                } else {
                    let privilege = Privilege::from_code(code).ok_or_else(invalid)?;
                    privileges = privileges.with(privilege);
                    last = Some(privilege);
                }
            }
            acl.entries.push(AclEntry {
                grantee,
                privileges,
                grant_option,
                grantor,
            });
        }
        Ok(acl)
    }

    /// Writes the list as `{grantee=codes/grantor,...}`, with role ids for
    /// the roles and `*` after a privilege the grantee may grant, like a
    /// PostgreSQL `aclitem` array with ids instead of names.
    pub fn to_stored(&self) -> String {
        let items: Vec<String> = self
            .entries
            .iter()
            .map(|entry| {
                let mut codes = String::new();
                for privilege in entry.privileges.iter() {
                    codes.push(privilege.code());
                    if entry.grant_option.contains(privilege) {
                        codes.push('*');
                    }
                }
                format!("{}={}/{}", entry.grantee.get(), codes, entry.grantor.get())
            })
            .collect();
        format!("{{{}}}", items.join(","))
    }
}

fn parse_role_id(text: &str) -> Option<RoleId> {
    text.parse::<i64>().ok().and_then(RoleId::from_stored)
}

/// Who owns an object and what every role may do with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAccess {
    pub owner: RoleId,
    pub acl: Acl,
}

impl ObjectAccess {
    /// An object that nothing was granted or revoked on.
    pub fn default_for(kind: ObjectKind, owner: RoleId) -> ObjectAccess {
        ObjectAccess {
            owner,
            acl: Acl::default_for(kind, owner),
        }
    }

    /// A built-in object, owned by the superuser, with `privileges` granted
    /// to every role on top of the kind's defaults.
    pub fn builtin_with_public(kind: ObjectKind, privileges: Privileges) -> ObjectAccess {
        let mut access = ObjectAccess::default_for(kind, RoleId::SUPERUSER);
        access.acl.grant(
            RoleId::PUBLIC,
            privileges,
            Privileges::NONE,
            RoleId::SUPERUSER,
        );
        access
    }

    /// A built-in object that every role may read, such as a catalog table.
    pub fn readable_by_everyone(kind: ObjectKind) -> ObjectAccess {
        ObjectAccess::builtin_with_public(kind, Privileges::of(Privilege::Select))
    }

    /// A built-in object that only the superuser may use, such as a function
    /// that reads the process or the database file.
    pub fn superuser_only() -> ObjectAccess {
        ObjectAccess {
            owner: RoleId::SUPERUSER,
            acl: Acl::empty(),
        }
    }

    pub fn allows(&self, role: RoleId, privilege: Privilege) -> bool {
        self.acl.privileges_of(role).contains(privilege)
    }

    pub fn is_owner(&self, role: RoleId) -> bool {
        self.owner == role
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(id: i64) -> RoleId {
        RoleId::from_stored(id).unwrap()
    }

    #[test]
    fn every_privilege_has_a_distinct_code() {
        let mut codes: Vec<char> = Privilege::ALL.iter().map(|p| p.code()).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), Privilege::ALL.len());
        for privilege in Privilege::ALL {
            assert_eq!(Privilege::from_code(privilege.code()), Some(privilege));
        }
    }

    #[test]
    fn default_list_gives_the_owner_everything_and_public_the_kinds_default() {
        let table = Acl::default_for(ObjectKind::Table, role(5));
        assert_eq!(
            table.privileges_of(role(5)),
            ObjectKind::Table.all_privileges()
        );
        assert_eq!(table.privileges_of(role(6)), Privileges::NONE);
        assert_eq!(table.privileges_of(RoleId::PUBLIC), Privileges::NONE);

        let function = Acl::default_for(ObjectKind::Function, role(5));
        assert!(function.privileges_of(role(6)).contains(Privilege::Execute));
        assert!(!function
            .grant_options_of(role(6))
            .contains(Privilege::Execute));
        assert!(function
            .grant_options_of(role(5))
            .contains(Privilege::Execute));
    }

    #[test]
    fn grant_adds_to_what_the_grantee_holds_from_the_same_grantor() {
        let mut acl = Acl::empty();
        acl.grant(
            role(2),
            Privileges::of(Privilege::Select),
            Privileges::NONE,
            role(1),
        );
        acl.grant(
            role(2),
            Privileges::of(Privilege::Insert),
            Privileges::NONE,
            role(1),
        );
        acl.grant(
            role(2),
            Privileges::of(Privilege::Select),
            Privileges::NONE,
            role(3),
        );

        assert_eq!(acl.entries().len(), 2);
        assert_eq!(
            acl.privileges_of(role(2)),
            Privileges::from_iter([Privilege::Select, Privilege::Insert])
        );
    }

    #[test]
    fn revoke_removes_only_what_the_grantor_gave() {
        let mut acl = Acl::empty();
        acl.grant(
            role(2),
            Privileges::of(Privilege::Select),
            Privileges::NONE,
            role(1),
        );
        acl.grant(
            role(2),
            Privileges::of(Privilege::Select),
            Privileges::NONE,
            role(3),
        );

        acl.revoke(role(2), Privileges::of(Privilege::Select), false, role(1));

        assert!(acl.privileges_of(role(2)).contains(Privilege::Select));
        assert_eq!(acl.entries().len(), 1);
        assert_eq!(acl.entries()[0].grantor, role(3));
    }

    #[test]
    fn revoking_the_grant_option_keeps_the_privilege() {
        let mut acl = Acl::empty();
        let select = Privileges::of(Privilege::Select);
        acl.grant(role(2), select, select, role(1));

        acl.revoke(role(2), select, true, role(1));

        assert!(acl.privileges_of(role(2)).contains(Privilege::Select));
        assert!(!acl.grant_options_of(role(2)).contains(Privilege::Select));
    }

    #[test]
    fn public_privileges_count_for_every_role() {
        let mut acl = Acl::empty();
        acl.grant(
            RoleId::PUBLIC,
            Privileges::of(Privilege::Select),
            Privileges::NONE,
            role(1),
        );

        assert!(acl.privileges_of(role(9)).contains(Privilege::Select));
        assert!(ObjectAccess {
            owner: role(1),
            acl
        }
        .allows(role(9), Privilege::Select));
    }

    #[test]
    fn stored_form_round_trips_and_marks_grant_options() {
        let mut acl = Acl::empty();
        let read_write = Privileges::from_iter([Privilege::Select, Privilege::Update]);
        acl.grant(
            role(3),
            read_write,
            Privileges::of(Privilege::Select),
            RoleId::SUPERUSER,
        );
        acl.grant(
            RoleId::PUBLIC,
            Privileges::of(Privilege::Select),
            Privileges::NONE,
            role(3),
        );

        let stored = acl.to_stored();
        assert_eq!(stored, "{3=r*w/0,-1=r/3}");
        assert_eq!(Acl::from_stored(&stored).unwrap(), acl);
        assert_eq!(Acl::from_stored("{}").unwrap(), Acl::empty());
        assert_eq!(Acl::empty().to_stored(), "{}");
    }

    #[test]
    fn stored_form_that_is_not_a_list_is_corrupt() {
        for text in [
            "", "3=r/0", "{3=r}", "{3=q/0}", "{x=r/0}", "{3=*r/0}", "{-2=r/0}",
        ] {
            let error = Acl::from_stored(text).unwrap_err();
            assert!(matches!(error, LimboError::Corrupt(_)), "{text}: {error}");
        }
    }
}
