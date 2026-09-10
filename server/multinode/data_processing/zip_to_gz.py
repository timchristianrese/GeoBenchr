# /// script
# requires-python = ">=3.10"
# dependencies = [
# "numpy~=2.2",
# "polars~=1.35",
# "requests~=2.32",
# ]
# ///

from __future__ import annotations

import io
import gzip
import pathlib
import zipfile
import requests
import xml.etree.ElementTree as ET
from collections import defaultdict
import re
from multiprocessing import Pool, cpu_count

# ---------------- CONFIG ----------------

BASE_BUCKET = "http://aisdata.ais.dk.s3.eu-central-1.amazonaws.com"
PREFIXES = ["2024/", "2025/"]

RAW_DIR = pathlib.Path("data/raw_csv_gz")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- S3 LISTING ----------------

def list_zip_files(prefix: str) -> list[str]:
    url = f"{BASE_BUCKET}/?list-type=2&prefix={prefix}"
    r = requests.get(url)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    files = []
    for c in root.findall("s3:Contents", ns):
        key = c.find("s3:Key", ns).text
        if key.endswith(".zip"):
            files.append(f"{BASE_BUCKET}/{key}")

    return files


def month_from_key(key: str) -> str | None:
    m = re.search(r"aisdk-(\d{4}-\d{2})-\d{2}", key)
    return m.group(1) if m else None


def group_zips_by_month() -> dict[str, list[str]]:
    buckets = defaultdict(list)

    for prefix in PREFIXES:
        for url in list_zip_files(prefix):
            key = url.split("/")[-1]
            month = month_from_key(key)
            if month:
                buckets[month].append(url)

    return buckets


# ---------------- CORE WORKER ----------------

def process_zip(url: str):
    print(f"[download] {url}")

    r = requests.get(url, stream=True)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        for name in z.namelist():
            if not name.endswith(".csv"):
                continue

            safe_name = name.replace("/", "_")
            gz_path = RAW_DIR / f"{safe_name}.csv.gz"

            if gz_path.exists():
                continue

            with z.open(name) as f_in, gzip.open(gz_path, "wb") as f_out:
                f_out.write(f_in.read())


# ---------------- MONTH WORKER ----------------

def process_month(args):
    month, urls = args

    print(f"\n=== MONTH {month} | {len(urls)} files ===")

    for url in urls:
        try:
            process_zip(url)
        except Exception as e:
            print(f"[ERROR] {url}: {e}")

    return month


# ---------------- MAIN ----------------

def main():
    month_buckets = group_zips_by_month()

    tasks = list(month_buckets.items())

    workers = min(len(tasks), cpu_count())

    print(f"Launching {workers} workers for {len(tasks)} months")

    with Pool(processes=workers) as pool:
        pool.map(process_month, tasks)


if __name__ == "__main__":
    main()