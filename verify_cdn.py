#!/usr/bin/env python3
"""
Verify:
1. All transformed gcp_cdn paths exist in GCP bucket
2. Content from source CDN matches content in GCP (MD5 comparison)
"""

import asyncio
import aiohttp
import csv
import hashlib
import sys
import time
import json
from pathlib import Path
from urllib.parse import quote

sys.stdout.reconfigure(line_buffering=True)

BUCKET = "mt-prod-pimcore-bucket"
GCS_API = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o"
GCS_DOWNLOAD = f"https://storage.googleapis.com/{BUCKET}"
CONCURRENCY = 100  # be gentler on source CDNs
OUTPUT_DIR = Path("outputs/phase-one")

CSV_FILES = [
    "match_cdn_CREA-DESI_Brands-Website-CDN-Links_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Manufacturers_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Product-Gallery-Images_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Service-Portal-CDN-Links_2026-03-23_Rev0_matched.csv",
]

# Column names that contain source URLs per file
SOURCE_URL_COLS = {
    "Brands-Website": "URL",
    "Manufacturers": None,  # multiple URL columns, handle separately
    "Product-Gallery": None,  # multiple URL columns
    "Service-Portal": "cdn_link",
}


async def get_auth_token():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"}
        ) as resp:
            data = await resp.json()
            return data["access_token"]


async def stream_md5(session, url, headers=None, timeout=30):
    """Download URL and compute MD5 without storing the file."""
    try:
        to = aiohttp.ClientTimeout(total=timeout)
        async with session.get(url, headers=headers, timeout=to, allow_redirects=True) as resp:
            if resp.status != 200:
                return None, resp.status
            md5 = hashlib.md5()
            async for chunk in resp.content.iter_chunked(8192):
                md5.update(chunk)
            return md5.hexdigest(), 200
    except Exception as e:
        return None, str(e)


async def check_exists(session, sem, gcp_path, token, results):
    """Check if object exists in GCP."""
    encoded = quote(gcp_path, safe="")
    url = f"{GCS_API}/{encoded}?fields=name"
    headers = {"Authorization": f"Bearer {token}"}
    async with sem:
        try:
            async with session.get(url, headers=headers) as resp:
                results[gcp_path] = resp.status == 200
        except:
            results[gcp_path] = None


async def compare_content(session, sem, source_url, gcp_path, token, results):
    """Download from both source and GCP, compare MD5."""
    gcp_url = f"{GCS_DOWNLOAD}/{quote(gcp_path, safe='/')}"
    gcp_headers = {"Authorization": f"Bearer {token}"}

    async with sem:
        # Download both in parallel
        source_task = stream_md5(session, source_url, timeout=60)
        gcp_task = stream_md5(session, gcp_url, headers=gcp_headers, timeout=60)

        source_md5, source_status = await source_task
        gcp_md5, gcp_status = await gcp_task

        results[gcp_path] = {
            "source_url": source_url,
            "source_md5": source_md5,
            "source_status": source_status,
            "gcp_md5": gcp_md5,
            "gcp_status": gcp_status,
            "match": source_md5 == gcp_md5 if (source_md5 and gcp_md5) else None,
        }


def extract_pairs(csv_file):
    """Extract (source_url, gcp_cdn) pairs from output CSV."""
    pairs = {}
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames

        for row in reader:
            gcp_cdn = row.get("gcp_cdn", "")
            if not gcp_cdn or gcp_cdn.startswith("asset/"):
                continue  # skip untransformed/empty

            # Find source URL based on file type
            source_url = None
            if "URL" in fields:
                source_url = row.get("URL", "")
            elif "cdn_link" in fields:
                source_url = row.get("cdn_link", "")
            elif "GalleryB" in fields:
                # Product gallery - use GalleryB as source
                source_url = row.get("GalleryB", "")
            elif "LogoUrl" in fields:
                # Manufacturers - use ProductPageLogoUrl
                source_url = row.get("ProductPageLogoUrl", "") or row.get("LogoUrl", "")

            if source_url and gcp_cdn and source_url.startswith("http"):
                if gcp_cdn not in pairs:
                    pairs[gcp_cdn] = source_url

    return pairs


async def run_check1(paths):
    """Check 1: All gcp_cdn paths exist in bucket."""
    print(f"\n{'='*60}")
    print(f"CHECK 1: Verifying {len(paths)} paths exist in GCP bucket")
    print(f"{'='*60}")

    token = await get_auth_token()
    sem = asyncio.Semaphore(200)
    results = {}

    connector = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [check_exists(session, sem, p, token, results) for p in paths]
        total = len(tasks)
        batch = 5000

        for i in range(0, len(tasks), batch):
            await asyncio.gather(*tasks[i:i+batch])
            done = min(i + batch, total)
            found = sum(1 for v in results.values() if v is True)
            missing = sum(1 for v in results.values() if v is False)
            errors = sum(1 for v in results.values() if v is None)
            print(f"  Progress: {done}/{total} | found={found} missing={missing} errors={errors}")

    found = {p for p, v in results.items() if v is True}
    missing = {p for p, v in results.items() if v is False}
    errors = {p for p, v in results.items() if v is None}

    print(f"\n  RESULT: {len(found)} exist | {len(missing)} missing | {len(errors)} errors")
    return found, missing, errors


async def run_check2(pairs, valid_paths):
    """Check 2: Content comparison between source CDN and GCP."""
    # Only compare paths that exist in bucket
    valid_pairs = {k: v for k, v in pairs.items() if k in valid_paths}

    print(f"\n{'='*60}")
    print(f"CHECK 2: Comparing content for {len(valid_pairs)} source<->GCP pairs")
    print(f"{'='*60}")

    token = await get_auth_token()
    sem = asyncio.Semaphore(CONCURRENCY)
    results = {}

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        items = list(valid_pairs.items())
        batch_size = 1000
        total = len(items)

        for i in range(0, total, batch_size):
            batch = items[i:i+batch_size]
            tasks = [compare_content(session, sem, src, gcp, token, results) for gcp, src in batch]
            await asyncio.gather(*tasks)

            done = min(i + batch_size, total)
            matched = sum(1 for v in results.values() if v.get("match") is True)
            mismatched = sum(1 for v in results.values() if v.get("match") is False)
            inconclusive = sum(1 for v in results.values() if v.get("match") is None)
            print(f"  Progress: {done}/{total} | match={matched} mismatch={mismatched} inconclusive={inconclusive}")

    matched = {p: v for p, v in results.items() if v.get("match") is True}
    mismatched = {p: v for p, v in results.items() if v.get("match") is False}
    inconclusive = {p: v for p, v in results.items() if v.get("match") is None}

    print(f"\n  RESULT: {len(matched)} match | {len(mismatched)} mismatch | {len(inconclusive)} inconclusive")

    # Write detailed results
    with open(OUTPUT_DIR / "verify_check2_mismatches.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["gcp_path", "source_url", "source_md5", "gcp_md5", "source_status", "gcp_status"])
        for p, v in mismatched.items():
            writer.writerow([p, v["source_url"], v["source_md5"], v["gcp_md5"], v["source_status"], v["gcp_status"]])

    with open(OUTPUT_DIR / "verify_check2_inconclusive.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["gcp_path", "source_url", "source_md5", "gcp_md5", "source_status", "gcp_status"])
        for p, v in inconclusive.items():
            writer.writerow([p, v["source_url"], v["source_md5"], v["gcp_md5"], v["source_status"], v["gcp_status"]])

    return matched, mismatched, inconclusive


def main():
    start = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Extract all transformed gcp_cdn paths and source URL pairs
    print("Extracting transformed gcp_cdn paths from output CSVs...")
    all_gcp_paths = set()
    all_pairs = {}  # gcp_cdn -> source_url

    for csv_file in CSV_FILES:
        path = OUTPUT_DIR / csv_file
        if not path.exists():
            print(f"  SKIP: {path}")
            continue

        pairs = extract_pairs(path)
        all_pairs.update(pairs)
        all_gcp_paths.update(pairs.keys())

        # Also get paths without source URLs
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                gcp = row.get("gcp_cdn", "")
                if gcp and not gcp.startswith("asset/"):
                    all_gcp_paths.add(gcp)

    print(f"  Unique transformed paths: {len(all_gcp_paths)}")
    print(f"  Paths with source URL pairs: {len(all_pairs)}")

    # Check 1: Existence
    found, missing, errors = asyncio.run(run_check1(list(all_gcp_paths)))

    # Write missing paths
    with open(OUTPUT_DIR / "verify_check1_missing.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["gcp_path"])
        for p in sorted(missing):
            writer.writerow([p])

    # Check 2: Content comparison
    matched, mismatched, inconclusive = asyncio.run(run_check2(all_pairs, found))

    # Summary
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"  Check 1 - Existence:")
    print(f"    Exist in bucket: {len(found)}")
    print(f"    Missing from bucket: {len(missing)}")
    print(f"    Errors: {len(errors)}")
    print(f"  Check 2 - Content match:")
    print(f"    MD5 match: {len(matched)}")
    print(f"    MD5 mismatch: {len(mismatched)}")
    print(f"    Inconclusive (download failed): {len(inconclusive)}")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Reports: {OUTPUT_DIR}/verify_check*.csv")


if __name__ == "__main__":
    main()
