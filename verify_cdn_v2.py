#!/usr/bin/env python3
"""
Verify gcp_cdn content matches source CDN content.
- GCP side: metadata API for MD5 (no download)
- Source side: download with browser headers, retry, fallback URLs
"""

import asyncio
import aiohttp
import csv
import hashlib
import sys
import time
import base64
from pathlib import Path
from urllib.parse import quote

sys.stdout.reconfigure(line_buffering=True)

BUCKET = "mt-prod-pimcore-bucket"
GCS_API = f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o"
CONCURRENCY = 150
OUTPUT_DIR = Path("outputs/phase-one")
MAX_RETRIES = 3

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

CSV_FILES = [
    "match_cdn_CREA-DESI_Brands-Website-CDN-Links_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Manufacturers_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Product-Gallery-Images_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Service-Portal-CDN-Links_2026-03-23_Rev0_matched.csv",
]


async def get_auth_token():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"}
        ) as resp:
            data = await resp.json()
            return data["access_token"]


async def get_gcp_md5(session, gcp_path, token):
    """Get MD5 from GCS metadata API — no download needed."""
    encoded = quote(gcp_path, safe="")
    url = f"{GCS_API}/{encoded}?fields=md5Hash,size"
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    md5_b64 = data.get("md5Hash", "")
                    if md5_b64:
                        md5_hex = base64.b64decode(md5_b64).hex()
                        return md5_hex, int(data.get("size", 0))
                    return None, 0
                elif resp.status == 404:
                    return "NOT_FOUND", 0
                # Retry on 429, 500, 503
                if resp.status in (429, 500, 503):
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                return None, 0
        except Exception:
            await asyncio.sleep(0.5 * (attempt + 1))
    return None, 0


async def get_source_md5(session, urls):
    """Download from source URL(s) and compute MD5. Try each URL with retries."""
    for url in urls:
        if not url or not url.startswith("http"):
            continue
        for attempt in range(MAX_RETRIES):
            try:
                to = aiohttp.ClientTimeout(total=60)
                async with session.get(url, headers=BROWSER_HEADERS, timeout=to, allow_redirects=True) as resp:
                    if resp.status == 200:
                        md5 = hashlib.md5()
                        async for chunk in resp.content.iter_chunked(8192):
                            md5.update(chunk)
                        return md5.hexdigest(), resp.status, url
                    elif resp.status == 403:
                        # Try next URL, don't retry 403
                        break
                    elif resp.status in (429, 500, 502, 503):
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    else:
                        break  # 404, etc — try next URL
            except asyncio.TimeoutError:
                await asyncio.sleep(1 * (attempt + 1))
            except Exception:
                break  # try next URL

    return None, "ALL_FAILED", None


async def compare_one(session, sem, gcp_path, source_urls, token, results):
    """Compare one GCP path against source URLs."""
    async with sem:
        gcp_md5, gcp_size = await get_gcp_md5(session, gcp_path, token)
        src_md5, src_status, used_url = await get_source_md5(session, source_urls)

        if gcp_md5 == "NOT_FOUND":
            status = "gcp_missing"
        elif gcp_md5 is None:
            status = "gcp_error"
        elif src_md5 is None:
            status = "source_inaccessible"
        elif gcp_md5 == src_md5:
            status = "match"
        else:
            status = "mismatch"

        results[gcp_path] = {
            "gcp_md5": gcp_md5,
            "gcp_size": gcp_size,
            "source_md5": src_md5,
            "source_status": src_status,
            "source_url": used_url or source_urls[0] if source_urls else "",
            "status": status,
        }


def extract_pairs(csv_file):
    """Extract (gcp_cdn, [source_urls]) from output CSV."""
    pairs = {}
    with open(csv_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames

        for row in reader:
            gcp_cdn = row.get("gcp_cdn", "")
            if not gcp_cdn or gcp_cdn.startswith("asset/"):
                continue

            # Collect ALL possible source URLs for this row
            urls = []
            for col in ["URL", "cdn_link", "GalleryB", "GalleryXL", "GalleryDThumb",
                        "ProductPageLogoUrl", "CartAndOrderPageLogoUrl", "LogoUrl",
                        "media_original", "media_small"]:
                if col in fields:
                    val = row.get(col, "")
                    if val and val.startswith("http"):
                        urls.append(val)

            if urls and gcp_cdn not in pairs:
                pairs[gcp_cdn] = urls

    return pairs


async def run_verify(all_pairs):
    print(f"\nVerifying {len(all_pairs)} GCP paths against source CDNs...")
    print(f"Concurrency: {CONCURRENCY} | Retries: {MAX_RETRIES}")
    print(f"GCP: metadata API (no download) | Source: download with browser headers\n")

    token = await get_auth_token()
    sem = asyncio.Semaphore(CONCURRENCY)
    results = {}

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        items = list(all_pairs.items())
        batch_size = 2000
        total = len(items)

        for i in range(0, total, batch_size):
            batch = items[i:i + batch_size]
            tasks = [compare_one(session, sem, gcp, srcs, token, results) for gcp, srcs in batch]
            await asyncio.gather(*tasks)

            done = min(i + batch_size, total)
            counts = {}
            for v in results.values():
                s = v["status"]
                counts[s] = counts.get(s, 0) + 1
            parts = " | ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            print(f"  {done}/{total} | {parts}")

    return results


def main():
    start = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Extracting source<->GCP pairs from output CSVs...")
    all_pairs = {}
    for csv_file in CSV_FILES:
        path = OUTPUT_DIR / csv_file
        if not path.exists():
            print(f"  SKIP: {path}")
            continue
        pairs = extract_pairs(path)
        all_pairs.update(pairs)
        print(f"  {csv_file.split('CREA-DESI_')[1].split('_2026')[0]}: {len(pairs)} pairs")

    print(f"  Total unique pairs: {len(all_pairs)}")

    results = asyncio.run(run_verify(all_pairs))

    # Categorize
    by_status = {}
    for gcp, info in results.items():
        s = info["status"]
        by_status.setdefault(s, []).append((gcp, info))

    # Write reports
    for status, items in by_status.items():
        if status == "match":
            continue  # don't need a file for matches
        out_file = OUTPUT_DIR / f"verify_v2_{status}.csv"
        with open(out_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["gcp_path", "gcp_md5", "gcp_size", "source_md5", "source_status", "source_url"])
            for gcp, info in items:
                writer.writerow([gcp, info["gcp_md5"], info["gcp_size"],
                                info["source_md5"], info["source_status"], info["source_url"]])
        print(f"  Written: {out_file} ({len(items)} rows)")

    # Summary
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"RESULTS ({elapsed:.1f}s)")
    print(f"{'='*60}")
    for status in ["match", "mismatch", "source_inaccessible", "gcp_missing", "gcp_error"]:
        count = len(by_status.get(status, []))
        pct = count / len(results) * 100 if results else 0
        print(f"  {status:25s}: {count:6d} ({pct:.1f}%)")
    print(f"  {'TOTAL':25s}: {len(results):6d}")


if __name__ == "__main__":
    main()
