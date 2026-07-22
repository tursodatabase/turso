#!/usr/bin/env python3
"""Generate seed LDIF data for the LDAP CDC demo.

Creates `ou=people` and `ou=groups` under a base DN, then populates them with
inetOrgPerson users and groupOfNames groups distributed across a handful of
geographic locations (encoded via the standard `l` locality attribute rather
than a per-region OU hierarchy -- see tools/ldap-connector/README.md for why).

Usage:
    python3 gen_seed_ldif.py > seed.ldif
"""

import random
import sys

BASE_DN = "dc=example,dc=org"
NUM_USERS = 1000
NUM_GROUPS = 500
MEMBERS_PER_GROUP = 6

REGIONS = [
    ("us-east", "Ashburn", "VA", "US"),
    ("us-west", "San Jose", "CA", "US"),
    ("eu-west", "Dublin", None, "IE"),
    ("eu-central", "Frankfurt", None, "DE"),
    ("apac-sg", "Singapore", None, "SG"),
    ("apac-jp", "Tokyo", None, "JP"),
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
]

GROUP_KINDS = [
    "engineering", "sales", "support", "finance", "marketing", "product",
    "security", "operations", "legal", "hr", "design", "research",
]


def ldif_escape(dn_value: str) -> str:
    """Escape a DN attribute value per RFC 4514 (minimal, good enough for generated data)."""
    return dn_value.replace("\\", "\\\\").replace(",", "\\,").replace("+", "\\+").replace('"', '\\"')


def emit_ou(base_dn: str, ou: str) -> None:
    print(f"dn: ou={ou},{base_dn}")
    print("objectClass: organizationalUnit")
    print(f"ou: {ou}")
    print()


def user_dn(uid: str, base_dn: str) -> str:
    return f"uid={uid},ou=people,{base_dn}"


def group_dn(cn: str, base_dn: str) -> str:
    return f"cn={ldif_escape(cn)},ou=groups,{base_dn}"


def gen_users(base_dn: str, count: int, rng: random.Random):
    users_by_region = {region[0]: [] for region in REGIONS}
    for i in range(1, count + 1):
        uid = f"user{i:04d}"
        given = rng.choice(FIRST_NAMES)
        sn = rng.choice(LAST_NAMES)
        cn = f"{given} {sn}"
        region, city, state, _country = rng.choice(REGIONS)
        dn = user_dn(uid, base_dn)
        print(f"dn: {dn}")
        print("objectClass: inetOrgPerson")
        print("objectClass: organizationalPerson")
        print("objectClass: person")
        print("objectClass: top")
        print(f"uid: {uid}")
        print(f"cn: {cn}")
        print(f"sn: {sn}")
        print(f"givenName: {given}")
        print(f"mail: {uid}@example.org")
        print(f"l: {city}")
        if state:
            print(f"st: {state}")
        print("description: " + region)
        print("userPassword: {SSHA}placeholder")
        print()
        users_by_region[region].append(dn)
    return users_by_region


def gen_groups(base_dn: str, count: int, users_by_region: dict, rng: random.Random):
    for i in range(1, count + 1):
        kind = rng.choice(GROUP_KINDS)
        region, city, state, _country = rng.choice(REGIONS)
        cn = f"{kind}-{region}-{i:04d}"
        dn = group_dn(cn, base_dn)
        pool = users_by_region[region] or [u for lst in users_by_region.values() for u in lst]
        members = rng.sample(pool, k=min(MEMBERS_PER_GROUP, len(pool)))
        print(f"dn: {dn}")
        print("objectClass: groupOfNames")
        print("objectClass: extensibleObject")
        print("objectClass: top")
        print(f"cn: {cn}")
        print(f"description: {kind} team, {region}")
        print(f"l: {city}")
        if state:
            print(f"st: {state}")
        for m in members:
            print(f"member: {m}")
        print()


def main():
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
    rng = random.Random(seed)

    emit_ou(BASE_DN, "people")
    emit_ou(BASE_DN, "groups")
    users_by_region = gen_users(BASE_DN, NUM_USERS, rng)
    gen_groups(BASE_DN, NUM_GROUPS, users_by_region, rng)


if __name__ == "__main__":
    main()
