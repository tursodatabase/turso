#!/usr/bin/env python3
"""Ask PostgreSQL 18 which function or operator it picks for each call shape.

Builds the call shapes from the catalog fixtures made by dump_catalogs.py,
runs each one on a real server, and reads the choice from the server log
(debug_print_parse). Writes postgres/frontend/resolve/testdata/oracle.json.
Run it on a scratch server only; it executes about 24 000 SELECTs of
NULL-argument calls:

    python3 postgres/conformance/tools/oracle.py \
        --host 127.0.0.1 --port 54341 --user postgres [--psql /path/to/psql] \
        [--max-cases 24000] [--cases my_cases.json]

Case shapes: for every function and operator in the fixtures, (a) the exact
argument types of each overload, (b) one or all arguments unknown, (c) each
polymorphic parameter (anyelement, anyarray, anyrange, anycompatible...) given
a type built from one element type, from two different element types, an
unknown, or a type of the wrong shape, (d) one argument replaced by each of 15
common types, and by array, range and multirange types. Typed arguments are
written as NULL::type and unknown arguments as a bare NULL (an untyped
literal). A bare NULL is used instead of a quoted literal so that the input
function of the chosen parameter type never fails and hides the choice.

oracle.json is {"postgres_version", "cases": [...]}, one case per line. A case
has "kind" ("function", "aggregate", "operator", or "cast" when the name is a
type name), "name", and "args" (a type oid or "unknown" each). A success adds
"chosen" (pg_proc oid, or pg_operator oid for an operator), "function" (the
function behind an operator), "result" (result type oid) and "coercions", one
string per argument: "none", "unknown>T" (literal read as type T), "relabel>T",
"io>T" (through input/output), "fn:F>T" (cast function F giving type T),
"array>T" (each element of an array coerced, giving array type T), or
"other:NODE>0". An error adds "error": {"sqlstate", "message", "detail", "hint"}.
"""

import argparse
import concurrent.futures
import json
import os
import random
import re
import subprocess
import sys

COMMON_TYPES = ["bool", "int2", "int4", "int8", "float4", "float8", "numeric", "text",
                "varchar", "bytea", "date", "timestamp", "timestamptz", "uuid", "oid"]
CONTAINER_TYPES = ["_int4", "_int8", "_numeric", "_text", "_varchar", "int4range", "numrange",
                   "daterange", "int4multirange", "nummultirange"]
FEW_CONTAINER_TYPES = ["_int4", "_text", "int4range", "int4multirange"]
ELEMENT_TYPES = ["int4", "numeric", "text", "date"]
MIXED_ELEMENT_TYPES = [("int4", "int8"), ("int4", "numeric"), ("numeric", "int4"),
                       ("int4", "text"), ("text", "varchar")]
ELEMENT_SHAPES = {"anyelement", "anynonarray", "anyenum", "anycompatible", "anycompatiblenonarray"}
ARRAY_SHAPES = {"anyarray", "anycompatiblearray"}
RANGE_SHAPES = {"anyrange", "anycompatiblerange"}
MULTIRANGE_SHAPES = {"anymultirange", "anycompatiblemultirange"}
MAX_CONTAINER_CASES = 3000
UNKNOWN = "unknown"
BATCH = 250
WORKERS = 4


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    testdata = os.path.join(here, "..", "..", "frontend", "resolve", "testdata")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default="5432")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--dbname", default="postgres")
    ap.add_argument("--psql", default="psql")
    ap.add_argument("--testdata", default=testdata)
    ap.add_argument("--max-cases", type=int, default=24000)
    ap.add_argument("--cases", help="JSON file with a list of {kind, name, args} instead of generated cases")
    ap.add_argument("--out", help="default: <testdata>/oracle.json")
    args = ap.parse_args()

    version = run_psql(args, "SHOW server_version").strip()
    if not version.startswith("18."):
        sys.exit(f"expected PostgreSQL 18, got {version}")

    types = load(args.testdata, "pg_type.json")
    type_names = {t["oid"]: t["typname"] for t in types}
    if args.cases:
        with open(args.cases) as f:
            cases = json.load(f)
    else:
        cases = generate_cases(args.testdata, types, args.max_cases)
    print(f"{len(cases)} cases", file=sys.stderr)

    batches = [cases[i:i + BATCH] for i in range(0, len(cases), BATCH)]
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        results = [r for batch in pool.map(lambda b: ask_batch(args, type_names, b), batches) for r in batch]

    results = [r for r in results if r.get("error", {}).get("sqlstate") != "42601"]
    out = args.out or os.path.join(args.testdata, "oracle.json")
    with open(out, "w") as f:
        f.write('{"postgres_version":%s,"cases":[\n' % json.dumps(version.split()[0]))
        f.write(",\n".join(json.dumps(r, separators=(",", ":")) for r in results))
        f.write("\n]}\n")
    print(f"wrote {len(results)} cases to {out} ({os.path.getsize(out)} bytes)", file=sys.stderr)


def load(testdata, name):
    with open(os.path.join(testdata, name)) as f:
        return json.load(f)


def generate_cases(testdata, types, max_cases):
    buckets = CallBuckets(types, Shapes(types, load(testdata, "pg_range.json")))
    for kind, name, sig in read_signatures(testdata):
        buckets.add_calls(kind, name, sig)
    chosen = choose_cases(buckets, max_cases)
    ordered = sorted(chosen, key=lambda k: (k[0], k[1], json.dumps(k[2])))
    return [{"kind": k, "name": n, "args": list(a)} for k, n, a in ordered]


def read_signatures(testdata):
    signatures = []
    for p in load(testdata, "pg_proc.json"):
        if 1 <= len(p["proargtypes"]) <= 3:
            signatures.append(("function", p["proname"], p["proargtypes"]))
    for o in load(testdata, "pg_operator.json"):
        sides = [t for t in (o["oprleft"], o["oprright"]) if t != 0]
        signatures.append(("operator", o["oprname"], sides))
    return signatures


def choose_cases(buckets, max_cases):
    chosen = {}
    rng = random.Random(18)
    limits = [(buckets.exact, max_cases), (buckets.unknown, max_cases), (buckets.polymorphic, max_cases),
              (buckets.in_container, MAX_CONTAINER_CASES), (buckets.replaced, max_cases)]
    for bucket, bucket_limit in limits:
        keys = [k for k in bucket if k not in chosen]
        limit = min(max_cases, len(chosen) + bucket_limit)
        for key in round_robin_by_name(keys, rng):
            if len(chosen) >= limit:
                break
            chosen[key] = None
    return chosen


class CallBuckets:
    """Collects the call shapes of each signature, by the kind of shape."""

    def __init__(self, types, shapes):
        type_oid = {t["typname"]: t["oid"] for t in types}
        self.pseudo = {t["oid"] for t in types if t["typtype"] == "p"}
        self.common = [type_oid[n] for n in COMMON_TYPES]
        self.containers = [type_oid[n] for n in CONTAINER_TYPES]
        self.few_containers = [type_oid[n] for n in FEW_CONTAINER_TYPES]
        self.container_oids = {t["oid"] for t in types if t["typtype"] in "rm" or t["typcategory"] == "A"}
        self.shapes = shapes
        self.exact, self.unknown, self.polymorphic, self.in_container, self.replaced = {}, {}, {}, {}, {}

    def add_calls(self, kind, name, sig):
        def add(bucket, new_args):
            bucket.setdefault((kind, name, tuple(new_args)), None)

        if not any(t in self.pseudo for t in sig):
            add(self.exact, sig)
        add(self.unknown, [UNKNOWN] * len(sig))
        for new_args in self.shapes.polymorphic_calls(sig, self.pseudo):
            add(self.polymorphic, new_args)
        takes_container = any(t in self.container_oids or t in self.shapes.polymorphic for t in sig)
        for i in range(len(sig)):
            if any(t in self.pseudo for j, t in enumerate(sig) if j != i):
                continue
            add(self.unknown, sig[:i] + [UNKNOWN] + sig[i + 1:])
            for t in self.common:
                add(self.replaced, sig[:i] + [t] + sig[i + 1:])
            for t in self.containers if takes_container else self.few_containers:
                add(self.in_container if takes_container else self.replaced, sig[:i] + [t] + sig[i + 1:])


class Shapes:
    """Builds the argument types that fill the polymorphic parameters of a signature."""

    def __init__(self, types, ranges):
        self.type_oid = {t["typname"]: t["oid"] for t in types}
        self.array_of = {t["oid"]: t["typarray"] for t in types if t["typarray"] != 0}
        self.range_of = {r["rngsubtype"]: r["rngtypid"] for r in ranges}
        self.multirange_of = {r["rngsubtype"]: r["rngmultitypid"] for r in ranges}
        names = ELEMENT_SHAPES | ARRAY_SHAPES | RANGE_SHAPES | MULTIRANGE_SHAPES
        self.polymorphic = {self.type_oid[n]: n for n in names}

    def polymorphic_calls(self, sig, pseudo):
        positions = [i for i, t in enumerate(sig) if t in self.polymorphic]
        if not positions or any(t in pseudo and t not in self.polymorphic for t in sig):
            return
        for element in ELEMENT_TYPES:
            yield from self.fill(sig, {i: element for i in positions})
        for first, rest in MIXED_ELEMENT_TYPES:
            if len(positions) > 1:
                yield from self.fill(sig, {i: first if i == positions[0] else rest for i in positions})
        for i in positions:
            for element in ("int4", "text"):
                others = {j: element for j in positions if j != i}
                for call in self.fill(sig, others):
                    yield call[:i] + [UNKNOWN] + call[i + 1:]
                    yield call[:i] + [self.wrong_shape(sig[i])] + call[i + 1:]

    def fill(self, sig, element_of_position):
        call = list(sig)
        for i, element in element_of_position.items():
            call[i] = self.shaped(sig[i], self.type_oid[element])
            if call[i] is None:
                return
        yield call

    def shaped(self, parameter, element):
        shape = self.polymorphic[parameter]
        if shape in ARRAY_SHAPES:
            return self.array_of.get(element)
        if shape in RANGE_SHAPES:
            return self.range_of.get(element)
        if shape in MULTIRANGE_SHAPES:
            return self.multirange_of.get(element)
        return element

    def wrong_shape(self, parameter):
        if self.polymorphic[parameter] in ELEMENT_SHAPES:
            return self.type_oid["_int4"]
        return self.type_oid["int4"]


def round_robin_by_name(keys, rng):
    groups = {}
    for key in keys:
        groups.setdefault((key[0], key[1]), []).append(key)
    for g in groups.values():
        rng.shuffle(g)
    queues = list(groups.values())
    rng.shuffle(queues)
    while queues:
        for q in queues:
            yield q.pop()
        queues = [q for q in queues if q]


def ask_batch(args, type_names, cases):
    script = ["SET client_min_messages = log;", "SET debug_print_parse = on;",
              "SET debug_pretty_print = off;", "SET default_transaction_read_only = on;",
              "SET statement_timeout = '2s';"]
    for i, case in enumerate(cases):
        script.append(f"\\warn @@CASE {i}")
        script.append(call_sql(case, type_names) + ";")
    proc = subprocess.run(
        [args.psql, "-h", args.host, "-p", str(args.port), "-U", args.user, "-d", args.dbname,
         "-At", "-q", "-X", "-o", os.devnull, "-v", "VERBOSITY=verbose", "-f", "-"],
        input="\n".join(script) + "\n", capture_output=True, text=True)
    segments = split_segments(proc.stderr)
    out = []
    for i, case in enumerate(cases):
        if i not in segments:
            sys.exit(f"no answer for {case}; psql said:\n{proc.stderr[-2000:]}")
        out.append(read_answer(case, segments[i]))
    return out


def call_sql(case, type_names):
    def arg(a):
        return "NULL" if a == UNKNOWN else f'NULL::pg_catalog."{type_names[a]}"'

    name = case["name"]
    rendered = [arg(a) for a in case["args"]]
    if case["kind"] == "function":
        return f'SELECT "{name.replace(chr(34), chr(34) * 2)}"({", ".join(rendered)})'
    if len(rendered) == 1:
        return f"SELECT {name} ({rendered[0]})"
    return f"SELECT ({rendered[0]}) {name} ({rendered[1]})"


def split_segments(stderr):
    segments, current, index = {}, None, None
    for line in stderr.split("\n"):
        m = re.match(r"^@@CASE (\d+)$", line)
        if m:
            index = int(m.group(1))
            current = segments[index] = []
        elif current is not None:
            current.append(re.sub(r"^psql:\S*:\d+: ", "", line))
    return {i: "\n".join(lines) for i, lines in segments.items()}


def read_answer(case, text):
    m = re.search(r"LOG:  00000: parse tree:\nDETAIL:  (.*?)\n\nLOCATION:", text, re.S)
    if m:
        return describe(case, parse_dump(" ".join(m.group(1).split("\n"))))
    m = re.search(r"ERROR:  (\w{5}): (.*?)\n", text)
    if not m:
        sys.exit(f"no parse tree and no error for {case}:\n{text}")
    error = {"sqlstate": m.group(1), "message": m.group(2)}
    for field in ("detail", "hint"):
        found = re.search(rf"\n{field.upper()}:  (.*?)\n(?:LOCATION|DETAIL|HINT|CONTEXT|STATEMENT):", text, re.S)
        if found:
            error[field] = " ".join(found.group(1).split())
    return {**case, "error": error}


def describe(case, query):
    targets = query[":targetList"]
    assert len(targets) == 1, case
    expr = targets[0][":expr"]
    top = expr["_"]
    types = case["args"]
    if top == "FUNCEXPR" and expr[":funcformat"] == "0":
        return {**case, "kind": "function", "chosen": int(expr[":funcid"]),
                "result": int(expr[":funcresulttype"]),
                "coercions": coercions(types, expr[":args"])}
    if top == "OPEXPR":
        return {**case, "kind": "operator", "chosen": int(expr[":opno"]),
                "function": int(expr[":opfuncid"]), "result": int(expr[":opresulttype"]),
                "coercions": coercions(types, expr[":args"])}
    if top == "AGGREF":
        inner = [a[":expr"] for a in expr[":args"]]
        return {**case, "kind": "aggregate", "chosen": int(expr[":aggfnoid"]),
                "result": int(expr[":aggtype"]), "coercions": coercions(types, inner)}
    return {**case, "kind": "cast", "node": top}


def coercions(types, nodes):
    if len(types) != len(nodes):
        return ["other:ARGS>0"] * len(types)
    return [coercion(t, n) for t, n in zip(types, nodes)]


def coercion(original, node):
    name = node["_"]
    if name == "CONST":
        target = int(node[":consttype"])
        return "none" if original == target else f"unknown>{target}"
    if name == "RELABELTYPE":
        return f"relabel>{node[':resulttype']}"
    if name == "COERCEVIAIO":
        return f"io>{node[':resulttype']}"
    if name == "FUNCEXPR" and node[":funcformat"] == "2":
        return f"fn:{node[':funcid']}>{node[':funcresulttype']}"
    if name == "ARRAYCOERCEEXPR":
        return f"array>{node[':resulttype']}"
    return f"other:{name}>0"


TOKEN = re.compile(r"\{|\}|\(|\)|\[|\]|(?:\\.|[^\s(){}\[\]])+")


def parse_dump(text):
    tokens = TOKEN.findall(text)
    value, end = parse_value(tokens, 0)
    assert end == len(tokens), "trailing tokens in parse dump"
    return value


def parse_value(tokens, i):
    tok = tokens[i]
    if tok == "{":
        node = {"_": tokens[i + 1]}
        i += 2
        while tokens[i] != "}":
            if tokens[i] == "[":
                while tokens[i] != "]":
                    i += 1
                i += 1
                continue
            key = tokens[i]
            assert key.startswith(":"), f"expected a field name, got {key}"
            node[key], i = parse_value(tokens, i + 1)
        return node, i + 1
    if tok == "(":
        items = []
        i += 1
        while tokens[i] != ")":
            item, i = parse_value(tokens, i)
            items.append(item)
        return items, i + 1
    return tok, i + 1


def run_psql(args, sql):
    cmd = [args.psql, "-h", args.host, "-p", str(args.port), "-U", args.user, "-d", args.dbname,
           "-At", "-q", "-X", "-v", "ON_ERROR_STOP=1", "-c", sql]
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


if __name__ == "__main__":
    main()
