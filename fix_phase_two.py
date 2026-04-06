#!/usr/bin/env python3
"""
Phase Two: Resolve gcp_cdn using Pimcore DB path metadata + GCS validation.

Algorithm:
1. Load DB export (filename → [bucket_path, ...] ordered by most recent)
2. For each CSV row: extract filename, lookup bucket_paths from DB
3. Validate each candidate against GCS (most recent first, first existing wins)
4. Write output CSVs to outputs/phase-two/
"""

import asyncio
import aiohttp
import csv
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

sys.stdout.reconfigure(line_buffering=True)

BUCKET = "mt-prod-pimcore-bucket"
GCS_API = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o"
CONCURRENCY = 200
OUTPUT_DIR = Path("outputs/phase-two")

CSV_FILES = [
    "match_cdn_CREA-DESI_Brands-Website-CDN-Links_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Manufacturers_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Product-Gallery-Images_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Service-Portal-CDN-Links_2026-03-23_Rev0_matched.csv",
]

DB_EXPORT = "pimcore_path_map_clean.tsv"


async def get_auth_token():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"}
        ) as resp:
            data = await resp.json()
            return data["access_token"]


async def check_exists_batch(paths, token):
    """Check which paths exist in GCS. Returns set of existing paths."""
    sem = asyncio.Semaphore(CONCURRENCY)
    results = {}

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)

    async def check_one(path):
        async with sem:
            encoded = quote(path, safe="")
            url = f"{GCS_API}/{encoded}?fields=name"
            headers = {"Authorization": f"Bearer {token}"}
            try:
                async with session.get(url, headers=headers) as resp:
                    results[path] = resp.status == 200
            except:
                results[path] = False

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [check_one(p) for p in paths]
        batch_size = 5000
        total = len(tasks)
        for i in range(0, total, batch_size):
            await asyncio.gather(*tasks[i:i + batch_size])
            done = min(i + batch_size, total)
            found = sum(1 for v in results.values() if v)
            print(f"  GCS check: {done}/{total} | exists={found}")

    return {p for p, v in results.items() if v}


def load_db_export(filepath):
    """Load DB export into filename → [bucket_path1, bucket_path2, ...] (ordered by most recent first)."""
    db_map = defaultdict(list)
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)  # skip header
        for row in reader:
            if len(row) >= 2:
                filename = row[0].strip()
                bucket_path = row[1].strip()
                if filename and bucket_path:
                    db_map[filename].append(bucket_path)
    return db_map


def extract_filename(gcp_cdn):
    """Extract filename from gcp_cdn value."""
    if not gcp_cdn:
        return None
    return gcp_cdn.split("/")[-1]


def main():
    start = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Load DB export
    print("Step 1: Loading Pimcore DB export...")
    db_map = load_db_export(DB_EXPORT)
    print(f"  Loaded {len(db_map)} unique filenames from DB")

    # Step 2: Collect all candidate paths from CSVs
    print("\nStep 2: Extracting filenames from CSVs and looking up in DB...")
    all_candidates = set()
    csv_stats = {}

    for csv_file in CSV_FILES:
        path = Path(csv_file)
        if not path.exists():
            print(f"  SKIP: {csv_file}")
            continue

        found_in_db = 0
        not_in_db = 0
        empty = 0

        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                gcp = row.get("gcp_cdn", "")
                if not gcp:
                    empty += 1
                    continue
                fname = extract_filename(gcp)
                if fname and fname in db_map:
                    for bp in db_map[fname]:
                        all_candidates.add(bp)
                    found_in_db += 1
                else:
                    not_in_db += 1
                # Also add original CSV value as fallback candidate
                if gcp:
                    all_candidates.add(gcp)

        short = csv_file.split("CREA-DESI_")[1].split("_2026")[0]
        csv_stats[csv_file] = {"found": found_in_db, "not_in_db": not_in_db, "empty": empty}
        print(f"  {short}: found_in_db={found_in_db} not_in_db={not_in_db} empty={empty}")

    print(f"  Total unique candidate paths to validate: {len(all_candidates)}")

    # Step 3: Validate all candidates against GCS
    print(f"\nStep 3: Validating {len(all_candidates)} paths against GCS...")
    t3 = time.time()
    token = asyncio.run(get_auth_token())
    existing = asyncio.run(check_exists_batch(list(all_candidates), token))
    print(f"  Validated in {time.time() - t3:.1f}s | {len(existing)} exist in GCS")

    # Step 4: Write output CSVs
    print("\nStep 4: Writing output CSVs...")
    total_errors = []

    for csv_file in CSV_FILES:
        path = Path(csv_file)
        if not path.exists():
            continue

        short = csv_file.split("CREA-DESI_")[1].split("_2026")[0]
        print(f"  Processing {short}...")

        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames
            rows = list(reader)

        transformed = 0
        errors = 0
        empty = 0
        kept_original = 0

        for i, row in enumerate(rows):
            gcp = row.get("gcp_cdn", "")
            if not gcp:
                empty += 1
                continue

            fname = extract_filename(gcp)

            # Try each DB candidate (most recent first), first existing wins
            resolved = None
            if fname in db_map:
                for bp in db_map[fname]:
                    if bp in existing:
                        resolved = bp
                        break

            # Fallback: try original CSV value directly
            if not resolved and gcp in existing:
                resolved = gcp

            if resolved:
                row["gcp_cdn"] = resolved
                transformed += 1
            elif fname in db_map:
                total_errors.append([csv_file, i + 2, gcp, "|".join(db_map[fname][:3]), "not_in_gcs"])
                errors += 1
            else:
                total_errors.append([csv_file, i + 2, gcp, "", "not_in_pimcore_db"])
                errors += 1

        out_path = OUTPUT_DIR / csv_file
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)

        print(f"    Rows: {len(rows)} | Transformed: {transformed} | Errors: {errors} | Empty: {empty}")

    # Write errors
    if total_errors:
        err_path = OUTPUT_DIR / "errors.csv"
        with open(err_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["file", "row", "original_gcp_cdn", "db_candidates", "reason"])
            writer.writerows(total_errors)
        print(f"\n  Errors: {len(total_errors)} rows → {err_path}")
    else:
        print("\n  No errors!")

    elapsed = time.time() - start
    print(f"\n=== DONE in {elapsed:.1f}s ===")
    print(f"  Output: {OUTPUT_DIR}/")


async def get_auth_token_sync():
    return await get_auth_token()


if __name__ == "__main__":
    main()
