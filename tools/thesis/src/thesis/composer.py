"""Discovery of Test Composer commands in a test template directory.

Test Composer recognizes executables by filename prefix. Files without a
recognized prefix (helpers, libraries, data) and non-executable files are
ignored, as they are on Antithesis.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class Kind(str, Enum):
    FIRST = "first"
    PARALLEL_DRIVER = "parallel_driver"
    SERIAL_DRIVER = "serial_driver"
    SINGLETON_DRIVER = "singleton_driver"
    ANYTIME = "anytime"
    EVENTUALLY = "eventually"
    FINALLY = "finally"


# Longest prefixes first so that e.g. "parallel_driver_" is not shadowed.
_PREFIXES = sorted(((f"{k.value}_", k) for k in Kind), key=lambda p: -len(p[0]))


@dataclass(frozen=True)
class Command:
    path: Path
    kind: Kind

    @property
    def name(self) -> str:
        return self.path.name


@dataclass(frozen=True)
class Template:
    path: Path
    commands: tuple[Command, ...]

    def of(self, *kinds: Kind) -> list[Command]:
        return [c for c in self.commands if c.kind in kinds]


def classify(name: str) -> Kind | None:
    for prefix, kind in _PREFIXES:
        if name.startswith(prefix):
            return kind
    return None


def load_template(path: Path) -> Template:
    path = path.resolve()
    if not path.is_dir():
        raise ValueError(f"{path}: not a directory")
    commands = []
    # Sorted so that the command list, and therefore every choice made from
    # it, does not depend on directory iteration order.
    for entry in sorted(path.iterdir()):
        kind = classify(entry.name)
        if kind is None or not entry.is_file():
            continue
        if not os.access(entry, os.X_OK):
            continue
        commands.append(Command(entry, kind))
    if not commands:
        raise ValueError(f"{path}: no Test Composer commands found")
    return Template(path, tuple(commands))
