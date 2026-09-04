#!/usr/bin/env python3
"""Pull PNG files from EC2 via chunked SSM base64."""
from __future__ import annotations

import base64
import json
import pathlib
import subprocess
import time

IID = "i-05fc9cecc91c64a99"
PROFILE = "pereman2"
CHUNK = 15000
OUTDIR = pathlib.Path(__file__).resolve().parent / "official"


def ssm(cmd: str, timeout: int = 120) -> str:
    params_path = pathlib.Path("/tmp/ssm-chunk.json")
    params_path.write_text(json.dumps({"commands": [cmd]}))
    cid = subprocess.check_output(
        [
            "aws",
            "ssm",
            "send-command",
            "--instance-ids",
            IID,
            "--document-name",
            "AWS-RunShellScript",
            "--timeout-seconds",
            str(timeout),
            "--parameters",
            f"file://{params_path}",
            "--query",
            "Command.CommandId",
            "--output",
            "text",
            "--profile",
            PROFILE,
        ],
        text=True,
    ).strip()
    for _ in range(90):
        time.sleep(2)
        raw = subprocess.check_output(
            [
                "aws",
                "ssm",
                "get-command-invocation",
                "--command-id",
                cid,
                "--instance-id",
                IID,
                "--query",
                "{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}",
                "--output",
                "json",
                "--profile",
                PROFILE,
            ],
            text=True,
        )
        data = json.loads(raw)
        if data["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            if data["Status"] != "Success":
                raise SystemExit(f"ssm failed: {data}")
            return data["Stdout"]
    raise SystemExit("ssm timeout")


def pull(remote: str, localname: str) -> pathlib.Path:
    meta = ssm(f"bash -lc 'stat -c %s {remote}'")
    size = int(meta.strip().splitlines()[-1])
    nchunks = (size + CHUNK - 1) // CHUNK
    print(f"{localname}: size={size} chunks={nchunks}")
    parts: list[bytes] = []
    for i in range(nchunks):
        start = i * CHUNK
        cmd = (
            f"bash -lc 'dd if={remote} bs=1 skip={start} count={CHUNK} status=none "
            f"| base64 -w0; echo'"
        )
        b64 = ssm(cmd).strip().splitlines()[-1]
        parts.append(base64.b64decode(b64))
        print(f"  chunk {i + 1}/{nchunks} ({len(parts[-1])} bytes)")
    data = b"".join(parts)
    if len(data) != size:
        raise SystemExit(f"size mismatch for {localname}: got {len(data)} want {size}")
    OUTDIR.mkdir(parents=True, exist_ok=True)
    path = OUTDIR / localname
    path.write_bytes(data)
    print(f"wrote {path}")
    return path


def main() -> None:
    pull(
        "/mnt/nvme/run/compare-new-20260903/latency/latency-ecdf.png",
        "latency-ecdf.png",
    )
    pull(
        "/mnt/nvme/run/compare-new-20260903/throughput/throughput.png",
        "throughput-official.png",
    )


if __name__ == "__main__":
    main()
