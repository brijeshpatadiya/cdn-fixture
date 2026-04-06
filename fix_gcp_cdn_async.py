#!/usr/bin/env python3
"""
Fix gcp_cdn paths in CSV files and verify against GCP bucket.
Uses async HTTP to verify ~100K paths fast on GCP internal network.
"""

import asyncio
import aiohttp
import csv
import os
import re
import sys
import time
import json
from pathlib import Path
from collections import defaultdict

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

BUCKET = "mt-prod-pimcore-bucket"
OUTPUT_DIR = Path("outputs/phase-one")
CONCURRENCY = 200  # concurrent HTTP requests
GCS_API = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o"

CSV_FILES = [
    "match_cdn_CREA-DESI_Brands-Website-CDN-Links_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Manufacturers_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Product-Gallery-Images_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Service-Portal-CDN-Links_2026-03-23_Rev0_matched.csv",
]

PATTERN = re.compile(r'^asset/[^/]+/([a-f0-9]+)/original/(.+)$')


def transform_path(gcp_cdn):
    if not gcp_cdn or not gcp_cdn.startswith("asset/"):
        return gcp_cdn, False
    m = PATTERN.match(gcp_cdn)
    if m:
        return f"{m.group(1)}/{m.group(2)}", True
    return gcp_cdn, False


async def get_auth_token():
    """Get access token from GCE metadata server."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"}
        ) as resp:
            data = await resp.json()
            return data["access_token"]


async def check_object_exists(session, sem, path, token, results):
    """Check if a single object exists using GCS JSON API."""
    from urllib.parse import quote
    encoded_path = quote(path, safe="")
    url = f"{GCS_API}/{encoded_path}?fields=name"
    headers = {"Authorization": f"Bearer {token}"}

    async with sem:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    results[path] = True
                elif resp.status == 404:
                    results[path] = False
                else:
                    # Retry once on unexpected status
                    await asyncio.sleep(0.1)
                    async with session.get(url, headers=headers) as resp2:
                        results[path] = resp2.status == 200
        except Exception as e:
            results[path] = None  # mark as error


async def verify_all_paths(paths):
    """Verify all paths exist in GCP bucket using async HTTP."""
    print(f"  Getting auth token from metadata server...")
    token = await get_auth_token()

    sem = asyncio.Semaphore(CONCURRENCY)
    results = {}

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for path in paths:
            tasks.append(check_object_exists(session, sem, path, token, results))

        # Process with progress reporting
        total = len(tasks)
        done = 0
        batch_size = 5000

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch)
            done += len(batch)
            found = sum(1 for v in results.values() if v is True)
            not_found = sum(1 for v in results.values() if v is False)
            errors = sum(1 for v in results.values() if v is None)
            print(f"  Progress: {done}/{total} checked | found={found} not_found={not_found} errors={errors}")

    return results


def main():
    start = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Extract and transform
    print("Step 1: Extracting and transforming gcp_cdn paths...")
    all_transformed = {}  # original -> transformed

    for csv_file in CSV_FILES:
        path = Path(csv_file)
        if not path.exists():
            print(f"  SKIP: {csv_file} not found")
            continue
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            gcp_idx = header.index("gcp_cdn")
            for row in reader:
                if len(row) > gcp_idx:
                    val = row[gcp_idx]
                    if val and val.startswith("asset/") and val not in all_transformed:
                        new_val, changed = transform_path(val)
                        if changed:
                            all_transformed[val] = new_val

    unique_paths = list(set(all_transformed.values()))
    print(f"  Unique original paths: {len(all_transformed)}")
    print(f"  Unique transformed paths: {len(unique_paths)}")
    print(f"  Step 1 done in {time.time() - start:.1f}s")

    # Step 2: Verify
    print(f"\nStep 2: Verifying {len(unique_paths)} paths against GCP bucket...")
    t2 = time.time()
    results = asyncio.run(verify_all_paths(unique_paths))

    found = {p for p, v in results.items() if v is True}
    not_found = {p for p, v in results.items() if v is False}
    errored = {p for p, v in results.items() if v is None}

    print(f"\n  Results: found={len(found)} not_found={len(not_found)} errors={len(errored)}")
    print(f"  Step 2 done in {time.time() - t2:.1f}s")

    # Retry errors
    if errored:
        print(f"\n  Retrying {len(errored)} errored paths...")
        retry_results = asyncio.run(verify_all_paths(list(errored)))
        for p, v in retry_results.items():
            results[p] = v
            if v is True:
                found.add(p)
                errored.discard(p)
            elif v is False:
                not_found.add(p)
                errored.discard(p)
        print(f"  After retry: found={len(found)} not_found={len(not_found)} still_errored={len(errored)}")

    # Build verified map
    verified_map = {}
    for orig, transformed in all_transformed.items():
        if transformed in found:
            verified_map[orig] = transformed

    # Step 3: Write output CSVs
    print(f"\nStep 3: Writing output CSVs...")
    errors_log = []

    for csv_file in CSV_FILES:
        path = Path(csv_file)
        if not path.exists():
            continue
        print(f"  Processing {csv_file}...")

        with open(path, "r", newline="", encoding="utf-8") as fin:
            reader = csv.reader(fin)
            header = next(reader)
            gcp_idx = header.index("gcp_cdn")
            rows = list(reader)

        transformed_count = 0
        error_count = 0
        empty_count = 0

        for i, row in enumerate(rows):
            if len(row) > gcp_idx:
                val = row[gcp_idx]
                if val in verified_map:
                    row[gcp_idx] = verified_map[val]
                    transformed_count += 1
                elif val and val.startswith("asset/") and val in all_transformed:
                    errors_log.append([csv_file, i + 2, val, all_transformed[val], "not_found_in_gcp"])
                    error_count += 1
                elif not val:
                    empty_count += 1

        out_path = OUTPUT_DIR / csv_file
        with open(out_path, "w", newline="", encoding="utf-8") as fout:
            writer = csv.writer(fout)
            writer.writerow(header)
            writer.writerows(rows)

        print(f"    Rows: {len(rows)} | Transformed: {transformed_count} | Errors: {error_count} | Empty: {empty_count}")

    # Write errors
    if errors_log:
        err_path = OUTPUT_DIR / "errors.csv"
        with open(err_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["file", "row", "original_gcp_cdn", "attempted_transform", "reason"])
            writer.writerows(errors_log)
        print(f"\n  Errors: {len(errors_log)} rows -> {err_path}")
    else:
        print(f"\n  No errors! All paths verified.")

    # Summary
    elapsed = time.time() - start
    print(f"\n=== SUMMARY ===")
    print(f"  Paths verified as existing: {len(found)}")
    print(f"  Paths not found: {len(not_found)}")
    print(f"  Paths with errors: {len(errored)}")
    print(f"  Error rows logged: {len(errors_log)}")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Output: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
