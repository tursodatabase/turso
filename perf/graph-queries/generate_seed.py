#!/usr/bin/env python3
"""Generate synthetic seed data for the graph-queries performance benchmark.

The data models a university course catalog / enrollment graph:
  - node:         courses, lectures, exams, departments, professors, etc.
  - edge:         directed relationships (requires, equivalent, offered_in, ...)
  - message:      student/advisor messages within enrollment threads
  - conversation: enrollment / advising sessions
  - journal:      semester review notes

Usage:
    python3 generate_seed.py | sqlite3 graph-queries.db
"""

import json
import random
import sys

random.seed(42)

# ── Row counts (original proportions) ────────────────────────────────────────
NUM_NODES = 2286
NUM_EDGES = 2787
NUM_MESSAGES = 6087
NUM_CONVERSATIONS = 827
NUM_JOURNALS = 44

# ── Fixed IDs referenced by benchmark queries ───────────────────────────────
CENTRAL_NODE_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
IN_LIST_IDS = [
    "6ba7b810-9dad-41d8-80b4-00c04fd430c8",
    "6ba7b811-9dad-41d8-80b4-00c04fd430c9",
    "6ba7b812-9dad-41d8-80b4-00c04fd430ca",
    "6ba7b813-9dad-41d8-80b4-00c04fd430cb",
    "6ba7b814-9dad-41d8-80b4-00c04fd430cc",
    "6ba7b815-9dad-41d8-80b4-00c04fd430cd",
    "6ba7b816-9dad-41d8-80b4-00c04fd430ce",
    "6ba7b817-9dad-41d8-80b4-00c04fd430cf",
    "6ba7b818-9dad-41d8-80b4-00c04fd430d0",
    "6ba7b819-9dad-41d8-80b4-00c04fd430d1",
]

# ── Timestamp range: Jan 2025 → Mar 2026 (epoch milliseconds) ──────────────
TS_START = 1735689600000  # 2025-01-01 00:00 UTC
TS_END = 1773532800000  # ~2026-03-13 00:00 UTC
# Query-E window (one specific day)
TS_DAY_START = 1772582400000
TS_DAY_END = 1772668799999

# ── Node type distribution (must sum to NUM_NODES) ─────────────────────────
NODE_TYPE_COUNTS = {
    "lecture": 350,
    "course": 800,
    "exam": 200,
    "assignment": 180,
    "semester": 80,
    "term": 60,
    "department": 50,
    "building": 50,
    "professor": 80,
    "instructor": 40,
    "program": 30,
    "major": 30,
    "credit": 40,
    "room": 30,
    "lab": 30,
    "tutorial": 60,
    "grade": 60,
    "enrollment": 40,
    "unknown": 36,
    "system": 40,
}
assert sum(NODE_TYPE_COUNTS.values()) == NUM_NODES

# ── Edge label distribution (must sum to NUM_EDGES) ────────────────────────
EDGE_LABEL_COUNTS = {
    "requires": 1500,
    "equivalent": 100,
    "precedes": 150,
    "follows": 150,
    "offered_in": 120,
    "taught_by": 100,
    "enrolled": 100,
    "graded": 100,
    "references": 100,
    "merges": 67,
    "covers": 100,
    "evaluates": 50,
    "fall_ref": 75,
    "spring_ref": 75,
}
assert sum(EDGE_LABEL_COUNTS.values()) == NUM_EDGES

# ── Level distribution for course nodes ────────────────────────────────────
LEVEL_COUNTS = {
    "introductory": 60,
    "intermediate": 120,
    "advanced": 200,
    "graduate": 100,
    "seminar": 200,
    "workshop": 80,
    "independent": 40,
}
assert sum(LEVEL_COUNTS.values()) == NODE_TYPE_COUNTS["course"]


# ── Helpers ─────────────────────────────────────────────────────────────────


def uuid4():
    return (
        f"{random.getrandbits(32):08x}-"
        f"{random.getrandbits(16):04x}-"
        f"4{random.getrandbits(12):03x}-"
        f"{random.choice('89ab')}{random.getrandbits(12):03x}-"
        f"{random.getrandbits(48):012x}"
    )


def ts(start=TS_START, end=TS_END):
    return random.uniform(start, end)


def sql(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


SUBJECTS = [
    "algebra",
    "biology",
    "calculus",
    "dynamics",
    "economics",
    "finance",
    "geometry",
    "history",
    "informatics",
    "jurisprudence",
    "kinetics",
    "linguistics",
    "mechanics",
    "neuroscience",
    "optics",
    "philosophy",
    "quantitative",
    "rhetoric",
    "statistics",
    "topology",
    "urban",
    "virology",
    "wavelets",
    "xenobiology",
    "yearbook",
    "zoology",
]


def display_label(ntype, idx):
    return f"{ntype.upper()[:3]}-{idx:04d}"


out = sys.stdout.write

# ── Schema (only columns referenced by benchmark queries) ────────────────

out("PRAGMA journal_mode=WAL;\n")
out("BEGIN TRANSACTION;\n\n")

out("""CREATE TABLE node (
  id varchar(36) PRIMARY KEY NOT NULL,
  displayLabel varchar NOT NULL DEFAULT '',
  createdAt real,
  type varchar DEFAULT 'node',
  entity text DEFAULT '{}'
);

CREATE TABLE edge (
  id varchar(36) PRIMARY KEY NOT NULL,
  fromId varchar(36) NOT NULL DEFAULT '',
  toId varchar(36) NOT NULL DEFAULT '',
  label varchar NOT NULL DEFAULT ''
);

CREATE TABLE message (
  id varchar NOT NULL PRIMARY KEY,
  createdAt real,
  author text
);

CREATE TABLE conversation (
  id varchar(36) PRIMARY KEY NOT NULL,
  startDate real
);

CREATE TABLE journal (
  id varchar(36) PRIMARY KEY NOT NULL,
  createdAt real NOT NULL
);

""")

# ── Generate nodes ──────────────────────────────────────────────────────────

course_levels = []
for lvl, cnt in LEVEL_COUNTS.items():
    course_levels.extend([lvl] * cnt)
random.shuffle(course_levels)
lvl_iter = iter(course_levels)

nodes_by_type: dict[str, list[str]] = {t: [] for t in NODE_TYPE_COUNTS}
all_node_ids: list[str] = []

node_counter = 0

for ntype, count in NODE_TYPE_COUNTS.items():
    for i in range(count):
        if ntype == "course" and i == 0:
            nid = CENTRAL_NODE_ID
        elif ntype == "lecture" and i < len(IN_LIST_IDS):
            nid = IN_LIST_IDS[i]
        else:
            nid = uuid4()

        nodes_by_type[ntype].append(nid)
        all_node_ids.append(nid)

        created = ts()
        dlbl = display_label(ntype, node_counter)

        if ntype == "course":
            level = next(lvl_iter)
            entity_json = json.dumps({"level": level})
        else:
            entity_json = "{}"

        out(f"INSERT INTO node VALUES({sql(nid)},{sql(dlbl)},{created},{sql(ntype)},{sql(entity_json)});\n")
        node_counter += 1

# Shift ~200 random nodes into the query-E time window
for nid in random.sample(all_node_ids, 50):
    out(f"UPDATE node SET createdAt={ts(TS_DAY_START, TS_DAY_END)} WHERE id={sql(nid)};\n")

out("\n")

# ── Generate edges ──────────────────────────────────────────────────────────

lecture_ids = nodes_by_type["lecture"]
course_ids = nodes_by_type["course"]
non_excluded_types = {"semester", "term", "lecture"}
course_and_other_ids = [nid for ntype, ids in nodes_by_type.items() if ntype not in non_excluded_types for nid in ids]

for elabel, count in EDGE_LABEL_COUNTS.items():
    for i in range(count):
        eid = uuid4()

        if elabel == "requires":
            from_id = random.choice(lecture_ids)
            to_id = random.choice(course_and_other_ids)
            # Central course gets ~30 inbound requires edges
            if i < 30:
                to_id = CENTRAL_NODE_ID
            # IN_LIST lectures each get ~5 outbound requires edges
            if i < 50:
                from_id = IN_LIST_IDS[i % len(IN_LIST_IDS)]
            # ~20 edges from central course to other courses (for query B)
            if 50 <= i < 70:
                from_id = CENTRAL_NODE_ID
                to_id = random.choice([c for c in course_ids if c != CENTRAL_NODE_ID])

        elif elabel == "equivalent":
            from_id = random.choice(course_ids)
            to_id = random.choice(course_ids)
            while to_id == from_id:
                to_id = random.choice(course_ids)
        else:
            from_id = random.choice(all_node_ids)
            to_id = random.choice(all_node_ids)
            while to_id == from_id:
                to_id = random.choice(all_node_ids)

        out(f"INSERT INTO edge VALUES({sql(eid)},{sql(from_id)},{sql(to_id)},{sql(elabel)});\n")

out("\n")

# ── Generate conversations ──────────────────────────────────────────────────

conv_ids: list[str] = []

for i in range(NUM_CONVERSATIONS):
    cid = uuid4()
    conv_ids.append(cid)
    start = ts()

    out(f"INSERT INTO conversation VALUES({sql(cid)},{start});\n")

for cid in random.sample(conv_ids, 20):
    out(f"UPDATE conversation SET startDate={ts(TS_DAY_START, TS_DAY_END)} WHERE id={sql(cid)};\n")

out("\n")

# ── Generate messages ───────────────────────────────────────────────────────

for i in range(NUM_MESSAGES):
    mid = uuid4()
    created = ts()
    author = "student" if random.random() < 0.6 else "advisor"

    out(f"INSERT INTO message VALUES({sql(mid)},{created},{sql(author)});\n")

# Extra student messages in query-E window
for i in range(30):
    mid = uuid4()
    created = ts(TS_DAY_START, TS_DAY_END)
    out(f"INSERT INTO message VALUES({sql(mid)},{created},'student');\n")

out("\n")

# ── Generate journals ───────────────────────────────────────────────────────

journal_ids_list: list[str] = []
for i in range(NUM_JOURNALS):
    jid = uuid4()
    journal_ids_list.append(jid)
    created = ts()

    out(f"INSERT INTO journal VALUES({sql(jid)},{created});\n")

for jid in random.sample(journal_ids_list, 5):
    out(f"UPDATE journal SET createdAt={ts(TS_DAY_START, TS_DAY_END)} WHERE id={sql(jid)};\n")

out("\n")

# ── Indexes ─────────────────────────────────────────────────────────────────

out("""CREATE INDEX idx_edge_label ON edge (label);
CREATE INDEX idx_edge_toId ON edge (toId);
CREATE INDEX idx_edge_fromId ON edge (fromId);
CREATE INDEX idx_conversations_start_date ON conversation (startDate DESC);
CREATE INDEX idx_journals_createdAt_date ON journal (createdAt DESC);
""")

out("\nCOMMIT;\n")
