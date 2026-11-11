"""
MapReduce from scratch - gross revenue per product category, straight from the raw text file.

This follows Hadoop's model closely, with Python processes standing in for cluster nodes:

  INPUT SPLITS  the 125 MB raw order_items.csv is cut into byte ranges (like HDFS blocks /
                TextInputFormat). A split that starts mid-line skips to the next full line;
                a split that ends mid-line reads past its end to finish that line.
  MAP           each worker parses its lines, drops bad records (qty <= 0, missing product,
                unparseable price) and emits (category, line_amount)
  COMBINE       the worker pre-sums its own pairs so it ships one number per key, not millions
  SHUFFLE       the driver groups the partial sums by key
  REDUCE        one final sum per key

The product -> category lookup is sent to every worker once (Hadoop's distributed cache).
Only the standard library is used, so worker processes start fast.

Usage:  python -m src.mapreduce --workers 8 [--splits 32]      (prints a JSON report)
"""
import argparse
import csv
import io
import json
import os
import re
import time
from collections import defaultdict
from multiprocessing import Pool

from config import RAW

INPUT = RAW / "order_items.csv"
_CATEGORY = {}
_NUM = re.compile(r"[^0-9.]")


def _init(lookup):
    global _CATEGORY
    _CATEGORY = lookup


def load_lookup():
    with open(RAW / "products.csv", newline="", encoding="utf-8") as f:
        return {r["product_id"]: r["category"] for r in csv.DictReader(f)}


def make_splits(n_splits):
    size = INPUT.stat().st_size
    step = size // n_splits + 1
    return [(i, i * step, min((i + 1) * step, size)) for i in range(n_splits)]


def mapper(split):
    split_id, start, end = split
    t0 = time.time()
    with open(INPUT, "rb") as f:
        if start == 0:
            f.readline()                      # header
        else:
            f.seek(start - 1)
            if f.read(1) != b"\n":
                f.readline()                  # partial line belongs to the previous split
        chunk = []
        while f.tell() < end or f.tell() == start:
            line = f.readline()
            if not line:
                break
            chunk.append(line)
            if f.tell() >= end:
                break
    combined = defaultdict(float)
    records = bad = 0
    for row in csv.reader(io.StringIO(b"".join(chunk).decode("utf-8"))):
        records += 1
        try:
            _, _, pid, qty, price, disc = row
            qty = int(qty)
            price = float(_NUM.sub("", price))
            if not pid or qty <= 0 or price <= 0:
                raise ValueError
        except ValueError:
            bad += 1
            continue
        combined[_CATEGORY.get(pid, "Unknown")] += round(qty * price * (1 - int(disc) / 100), 2)
    return {"split": split_id, "bytes": f"{start:,}-{end:,}", "records": records, "bad_records": bad,
            "pid": os.getpid(), "seconds": round(time.time() - t0, 3), "emitted": dict(combined)}


def run_job(workers: int, n_splits: int = 32) -> dict:
    lookup = load_lookup()
    splits = make_splits(n_splits)
    t0 = time.time()
    if workers == 1:
        _init(lookup)
        maps = [mapper(s) for s in splits]
    else:
        with Pool(workers, initializer=_init, initargs=(lookup,)) as pool:
            maps = pool.map(mapper, splits)
    t_map = time.time()

    shuffled = defaultdict(list)
    for m in maps:
        for key, value in m["emitted"].items():
            shuffled[key].append(value)
    t_shuffle = time.time()

    reduced = {k: round(sum(v), 2) for k, v in sorted(shuffled.items())}
    t_end = time.time()

    return {
        "workers": workers, "input_file": INPUT.name, "input_mb": round(INPUT.stat().st_size / 1e6, 1),
        "input_splits": len(splits), "records": sum(m["records"] for m in maps),
        "bad_records": sum(m["bad_records"] for m in maps),
        "worker_processes": len({m["pid"] for m in maps}),
        "seconds": {"map": round(t_map - t0, 3), "shuffle": round(t_shuffle - t_map, 4),
                    "reduce": round(t_end - t_shuffle, 4), "total": round(t_end - t0, 3)},
        "map_outputs": [{k: v for k, v in m.items() if k != "emitted"} | {"keys_emitted": len(m["emitted"])}
                        for m in maps],
        "shuffle": {k: len(v) for k, v in sorted(shuffled.items())},
        "result": reduced,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--splits", type=int, default=32)
    a = ap.parse_args()
    print(json.dumps(run_job(a.workers, a.splits)))
