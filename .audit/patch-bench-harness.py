#!/usr/bin/env python3
"""Add --label and --group-commit to the latency and throughput harnesses.

Box-local only. Do not commit. Safe to rerun.
"""
from pathlib import Path
import sys

LABEL_ARGS = """
    #[arg(long = "label", help = "CSV engine name and filename prefix")]
    label: Option<String>,

    #[arg(long = "group-commit", default_value_t = false)]
    group_commit: bool,
"""

GC_PRAGMA = """
        if config.group_commit {
            conn.execute("PRAGMA mvcc_group_commit = yes", ()).await.unwrap();
        }
"""


def once(text: str, old: str, new: str, where: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise SystemExit(f"missing needle in {where}:\n{old[:160]!r}")
    return text.replace(old, new, 1)


def patch_main(path: Path, extra_config_field: str) -> None:
    text = path.read_text()
    text = once(
        text,
        "    engine: Engine,\n",
        "    engine: Engine,\n" + LABEL_ARGS,
        path,
    )
    text = once(
        text,
        extra_config_field,
        extra_config_field.replace("\n}", "\n    pub group_commit: bool,\n}"),
        f"{path} Config",
    )
    if extra_config_field.startswith("    pub max_overrun"):
        text = once(
            text,
            "        max_overrun: args.max_overrun,\n    };\n",
            "        max_overrun: args.max_overrun,\n        group_commit: args.group_commit,\n    };\n",
            f"{path} Config init",
        )
    else:
        text = once(
            text,
            "        checkpointer: (args.checkpointer > 0).then(|| Duration::from_millis(args.checkpointer)),\n    };\n",
            "        checkpointer: (args.checkpointer > 0).then(|| Duration::from_millis(args.checkpointer)),\n"
            "        group_commit: args.group_commit,\n    };\n",
            f"{path} Config init",
        )
    text = once(
        text,
        "    let engine_label = match args.engine {\n        Engine::Sqlite => \"sqlite\",\n        Engine::Turso => \"turso\",\n    };\n",
        "    let engine_label_buf = args.label.unwrap_or_else(|| match args.engine {\n"
        "        Engine::Sqlite => \"sqlite\".to_string(),\n"
        "        Engine::Turso => \"turso\".to_string(),\n"
        "    });\n"
        "    let engine_label = engine_label_buf.as_str();\n",
        f"{path} engine_label",
    )
    path.write_text(text)
    print(f"patched {path}")


def patch_engine(path: Path) -> None:
    text = path.read_text()
    if "PRAGMA mvcc_group_commit" in text:
        print(f"already patched {path}")
        return
    # setup() is the second synchronous=FULL, after the checkpointer threshold.
    anchor = 'if config.checkpointer.is_some()'
    i = text.find(anchor)
    if i < 0:
        raise SystemExit(f"missing checkpointer setup in {path}")
    sync = '    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();\n'
    j = text.find(sync, i)
    if j < 0:
        raise SystemExit(f"missing setup synchronous pragma in {path}")
    path.write_text(text[:j] + GC_PRAGMA + text[j:])
    print(f"patched {path}")


def main() -> None:
    root = Path(sys.argv[1])
    patch_main(
        root / "perf/latency/main.rs",
        "    pub max_overrun: f64,\n}\n",
    )
    patch_main(
        root / "perf/throughput/main.rs",
        "    pub checkpointer: Option<Duration>,\n}\n",
    )
    patch_engine(root / "perf/latency/turso_engine.rs")
    patch_engine(root / "perf/throughput/turso_engine.rs")
    print("harness patched", root)


if __name__ == "__main__":
    main()
