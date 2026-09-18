#!/usr/bin/env python3
"""
Download the RetailRocket dataset and check it is what we expect.

    pip install -r requirements-data.txt
    python scripts/fetch_dataset.py

Kaggle requires an account for downloads, so this needs an API token:
Kaggle -> Settings -> "API Tokens" tab -> under "Legacy API Credentials",
"Create Legacy API Key" downloads kaggle.json. (The newer "Generate New
Token" above it is a different format this package does not read.) Put the
file at %USERPROFILE%\\.kaggle\\kaggle.json (Windows) or ~/.kaggle/kaggle.json
(Linux/macOS), or set KAGGLE_USERNAME and KAGGLE_KEY instead.

The files land in data/raw/ (git-ignored - 1.4 GB does not belong in a
repository) with a MANIFEST.json recording sizes, checksums and row counts, so
a later run can prove it is working on the same data.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data import retailrocket as rr  # noqa: E402

DATASET = "retailrocket/ecommerce-dataset"
DEFAULT_DEST = Path(__file__).resolve().parents[1] / "data" / "raw"

# What the published dataset contains. Checked after download: a dataset that
# changed under us should fail here, not halfway through an evaluation.
class Expected(NamedTuple):
    columns: list[str]
    min_rows: int


EXPECTED = {
    "events.csv": Expected(rr.EVENT_COLUMNS, 2_500_000),
    "item_properties_part1.csv": Expected(rr.PROPERTY_COLUMNS, 1_000_000),
    "item_properties_part2.csv": Expected(rr.PROPERTY_COLUMNS, 1_000_000),
    "category_tree.csv": Expected(["categoryid", "parentid"], 1_000),
}

CREDENTIAL_HELP = """
No Kaggle credentials found.

  1. Sign in at https://www.kaggle.com (a free account is enough)
  2. Settings -> the "API Tokens" tab -> under "Legacy API Credentials",
     "Create Legacy API Key". That downloads kaggle.json. The newer
     "Generate New Token" button above it produces a different format that
     this package does not read.
  3. Move it to:
       Windows  %USERPROFILE%\\.kaggle\\kaggle.json
       Linux    ~/.kaggle/kaggle.json
     or set KAGGLE_USERNAME and KAGGLE_KEY in the environment.
  4. Accept the dataset's terms once, on
     https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

Then run this script again.
""".strip()


def have_credentials() -> bool:
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return True
    config_dir = os.getenv("KAGGLE_CONFIG_DIR")
    candidates = [Path(config_dir) / "kaggle.json"] if config_dir else []
    candidates.append(Path.home() / ".kaggle" / "kaggle.json")
    return any(path.is_file() for path in candidates)


def sha256(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while block := handle.read(chunk):
            digest.update(block)
    return digest.hexdigest()


def count_rows(path: Path) -> int:
    """Lines minus the header. Reads bytes, not CSV fields: 30x faster."""
    with open(path, "rb") as handle:
        lines = sum(block.count(b"\n") for block in iter(lambda: handle.read(1 << 20), b""))
    return max(lines - 1, 0)


def header_of(path: Path) -> list[str]:
    with open(path, encoding="utf-8") as handle:
        return [c.strip() for c in handle.readline().strip().split(",")]


def download(dest: Path) -> None:
    if not have_credentials():
        print(CREDENTIAL_HELP)
        raise SystemExit(2)
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("The kaggle package is missing. Install it with:\n"
              "    pip install -r requirements-data.txt")
        raise SystemExit(2) from None
    except OSError as exc:                      # kaggle.json present but unreadable
        print(f"Kaggle refused the credentials: {exc}\n\n{CREDENTIAL_HELP}")
        raise SystemExit(2) from None

    api = KaggleApi()
    api.authenticate()
    dest.mkdir(parents=True, exist_ok=True)
    print(f"downloading {DATASET} into {dest} (about 500 MB zipped) ...")
    started = time.time()
    api.dataset_download_files(DATASET, path=str(dest), unzip=True, quiet=False)
    print(f"downloaded in {time.time() - started:.0f}s")


def verify(dest: Path) -> dict:
    """Check every expected file, and record what we found."""
    files: dict[str, dict] = {}
    manifest: dict = {"dataset": DATASET,
                      "verified_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                      "files": files}
    problems = []
    for name, expected in EXPECTED.items():
        path = dest / name
        if not path.is_file():
            problems.append(f"{name} is missing")
            continue
        header = header_of(path)
        rows = count_rows(path)
        files[name] = {"bytes": path.stat().st_size, "rows": rows,
                                   "columns": header, "sha256": sha256(path)}
        if header != expected.columns:
            problems.append(f"{name} has columns {header}, expected "
                            f"{expected.columns}")
        if rows < expected.min_rows:
            problems.append(f"{name} has {rows:,} rows, expected at least "
                            f"{expected.min_rows:,}")
        print(f"  {name:28} {rows:>10,} rows  {path.stat().st_size / 1e6:>7.1f} MB")
    if problems:
        print("\nThe download does not look like the published dataset:")
        for problem in problems:
            print(f"  - {problem}")
        raise SystemExit(1)
    return manifest


def summarise(dest: Path) -> dict:
    """Read events.csv once and report what the pipeline will actually see."""
    first = last = None
    kinds: dict[str, int] = {}
    visitors: set[int] = set()
    items: set[int] = set()
    usable = 0
    for event in rr.read_events(str(dest / "events.csv")):
        usable += 1
        kinds[event.event_type] = kinds.get(event.event_type, 0) + 1
        visitors.add(event.visitor)
        items.add(event.item)
        first = event.at if first is None else min(first, event.at)
        last = event.at if last is None else max(last, event.at)
    days = (last - first) / 86400 if first and last else 0
    print(f"\n  usable events   {usable:,}")
    print(f"  visitors        {len(visitors):,}")
    print(f"  items           {len(items):,}")
    print("  event types     " + ", ".join(f"{k}={v:,}" for k, v in sorted(kinds.items())))
    print(f"  period          {time.strftime('%Y-%m-%d', time.gmtime(first))}"
          f" to {time.strftime('%Y-%m-%d', time.gmtime(last))} ({days:.0f} days)")
    return {"usable_events": usable, "visitors": len(visitors),
            "items": len(items), "event_types": kinds,
            "first_event": first, "last_event": last}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--force", action="store_true",
                        help="download again even if the files are already there")
    args = parser.parse_args()

    present = all((args.dest / name).is_file() for name in EXPECTED)
    if present and not args.force:
        print(f"{args.dest} already has the dataset; verifying (--force to "
              f"download again)")
    else:
        download(args.dest)

    print("\nverifying:")
    manifest = verify(args.dest)
    manifest["summary"] = summarise(args.dest)
    (args.dest / "MANIFEST.json").write_text(json.dumps(manifest, indent=2),
                                             encoding="utf-8")
    print(f"\nwrote {args.dest / 'MANIFEST.json'}")
    print("next:  python scripts/replay.py --days 7")
    return 0


if __name__ == "__main__":
    sys.exit(main())
