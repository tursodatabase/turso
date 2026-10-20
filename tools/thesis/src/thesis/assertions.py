"""Evaluation of Antithesis SDK assertion messages.

The SDK reports each assertion as a JSON line of the form
{"antithesis_assert": {...}}. Entries with "hit": false are catalog entries
declaring that an assertion exists; entries with "hit": true are evaluations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Property:
    id: str
    message: str
    display_type: str
    must_hit: bool
    location: dict[str, Any]
    hits: int = 0
    passes: int = 0
    failures: int = 0
    first_failure: dict[str, Any] | None = None
    first_failure_step: int | None = None

    @property
    def where(self) -> str:
        file = self.location.get("file") or "?"
        line = self.location.get("begin_line")
        return f"{file}:{line}" if line else file

    def failed(self) -> bool:
        """The property was violated by some evaluation."""
        return self.failures > 0

    def unsatisfied(self) -> bool:
        """The property needed something to happen that never did."""
        if self.display_type in ("Sometimes", "Reachable"):
            return self.passes == 0
        return self.must_hit and self.hits == 0


@dataclass
class Violation:
    prop: Property
    details: Any
    step: int


@dataclass
class Assertions:
    properties: dict[str, Property] = field(default_factory=dict)

    def _property(self, a: dict[str, Any]) -> Property:
        key = a.get("id") or a.get("message", "")
        prop = self.properties.get(key)
        if prop is None:
            prop = Property(
                id=key,
                message=a.get("message", ""),
                display_type=a.get("display_type", ""),
                must_hit=bool(a.get("must_hit", False)),
                location=a.get("location") or {},
            )
            self.properties[key] = prop
        return prop

    def record(self, a: dict[str, Any], step: int) -> Violation | None:
        prop = self._property(a)
        if not a.get("hit", False):
            return None
        prop.hits += 1
        condition = bool(a.get("condition", False))
        if prop.display_type == "Unreachable":
            ok = False
        elif prop.display_type in ("Always", "AlwaysOrUnreachable"):
            ok = condition
        else:
            # Sometimes and Reachable never fail on an individual hit; they
            # fail only if nothing ever satisfies them.
            if condition or prop.display_type == "Reachable":
                prop.passes += 1
            return None
        if ok:
            prop.passes += 1
            return None
        prop.failures += 1
        if prop.first_failure is None:
            prop.first_failure = a.get("details")
            prop.first_failure_step = step
        return Violation(prop, a.get("details"), step)

    def ingest(self, path: Path, step: int) -> list[Violation]:
        """Reads one SDK output file and returns the violations it reports."""
        violations = []
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return violations
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            a = msg.get("antithesis_assert") if isinstance(msg, dict) else None
            if isinstance(a, dict):
                v = self.record(a, step)
                if v is not None:
                    violations.append(v)
        return violations

    def unsatisfied(self) -> list[Property]:
        return [p for p in self.properties.values() if p.unsatisfied()]
