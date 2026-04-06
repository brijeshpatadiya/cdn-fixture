import pandas as pd
import re
import os
import subprocess
import sys
from pathlib import Path

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

BUCKET = "mt-prod-pimcore-bucket"
INPUT_DIR = Path(".")
OUTPUT_DIR = Path("outputs/phase-one")
GSUTIL = "/opt/homebrew/share/google-cloud-sdk/bin/gsutil"
BATCH_SIZE = 500  # paths per gsutil -m stat call

CSV_FILES = [
    "match_cdn_CREA-DESI_Brands-Website-CDN-Links_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Manufacturers_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Product-Gallery-Images_2026-03-23_Rev0_matched.csv",
    "match_cdn_CREA-DESI_Service-Portal-CDN-Links_2026-03-23_Rev0_matched.csv",
]

PATTERN = re.compile(r'^asset/[^/]+/([a-f0-9]+)/original/(.+)$')


def transform_path(gcp_cdn):
    """Transform asset/{dept}/{hash}/original/{filename} -> {hash}/{filename}"""
    if not gcp_cdn or pd.isna(gcp_cdn) or not gcp_cdn.startswith("asset/"):
        return gcp_cdn, False
    m = PATTERN.match(gcp_cdn)
    if m:
        return f"{m.group(1)}/{m.group(2)}", True
    return gcp_cdn, False


def verify_paths_batch(paths, batch_num, total_batches):
    """Verify a batch of paths exist in GCP using gsutil -m stat."""
    gs_paths = [f"gs://{BUCKET}/{p}" for p in paths]
    try:
        result = subprocess.run(
            [GSUTIL, "-m", "stat"] + gs_paths,
            capture_output=True, text=True, timeout=300
        )
        # gsutil stat outputs info for found objects, errors for missing ones
        found = set()
        for line in result.stdout.split("\n"):
            if line.strip().startswith(f"gs://{BUCKET}/"):
                obj_path = line.strip().rstrip(":")[len(f"gs://{BUCKET}/"):]
                found.add(obj_path)
        # Also check stderr for "No URLs matched" to find missing paths
        missing = set()
        for line in result.stderr.split("\n"):
            if "No URLs matched" in line:
                # Extract the path from the error message
                for p in paths:
                    gs_p = f"gs://{BUCKET}/{p}"
                    if gs_p in line:
                        missing.add(p)
        return found, missing
    except subprocess.TimeoutExpired:
        print(f"  Batch {batch_num} timed out!")
        return set(), set(paths)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Extract and transform unique paths
    print("Step 1: Extracting and transforming gcp_cdn paths...")
    all_original = set()
    all_transformed = {}  # original -> transformed

    for csv_file in CSV_FILES:
        df = pd.read_csv(INPUT_DIR / csv_file, dtype=str, keep_default_na=False)
        for val in df["gcp_cdn"]:
            if val and val.startswith("asset/"):
                if val not in all_transformed:
                    new_val, changed = transform_path(val)
                    if changed:
                        all_transformed[val] = new_val
                        all_original.add(val)

    unique_paths = list(set(all_transformed.values()))
    print(f"  Total unique original paths: {len(all_original)}")
    print(f"  Total unique transformed paths: {len(unique_paths)}")

    # Step 2: Verify all transformed paths exist in GCP
    print(f"\nStep 2: Verifying {len(unique_paths)} paths in GCP (batch size={BATCH_SIZE})...")
    verified = set()
    not_found = set()
    total_batches = (len(unique_paths) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(unique_paths), BATCH_SIZE):
        batch = unique_paths[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        if batch_num % 10 == 1 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches} ({len(verified)} verified so far)...")
        found, missing = verify_paths_batch(batch, batch_num, total_batches)
        verified.update(found)
        not_found.update(missing)

    # Paths not explicitly found or missing need recheck
    unchecked = set(unique_paths) - verified - not_found
    if unchecked:
        print(f"  Rechecking {len(unchecked)} ambiguous paths individually...")
        for p in unchecked:
            result = subprocess.run(
                [GSUTIL, "stat", f"gs://{BUCKET}/{p}"],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode == 0:
                verified.add(p)
            else:
                not_found.add(p)

    print(f"\n  Verified: {len(verified)}")
    print(f"  Not found: {len(not_found)}")

    # Build lookup: original -> verified transformed (or None if not found)
    verified_map = {}
    for orig, transformed in all_transformed.items():
        if transformed in verified:
            verified_map[orig] = transformed
        # else: keep original (will be logged as error)

    # Step 3: Write output CSVs
    print("\nStep 3: Writing output CSVs...")
    errors = []

    for csv_file in CSV_FILES:
        print(f"  Processing {csv_file}...")
        df = pd.read_csv(INPUT_DIR / csv_file, dtype=str, keep_default_na=False)
        original_count = len(df)
        transformed_count = 0
        error_count = 0

        new_gcp_cdn = []
        for idx, val in enumerate(df["gcp_cdn"]):
            if val in verified_map:
                new_gcp_cdn.append(verified_map[val])
                transformed_count += 1
            elif val and val.startswith("asset/") and val in all_transformed:
                # Transform failed verification — keep original, log error
                new_gcp_cdn.append(val)
                errors.append({
                    "file": csv_file,
                    "row": idx + 2,
                    "original_gcp_cdn": val,
                    "attempted_transform": all_transformed[val],
                    "reason": "not_found_in_gcp"
                })
                error_count += 1
            else:
                new_gcp_cdn.append(val)

        df["gcp_cdn"] = new_gcp_cdn
        df.to_csv(OUTPUT_DIR / csv_file, index=False)
        print(f"    Rows: {original_count} | Transformed: {transformed_count} | Errors: {error_count} | Empty/skipped: {original_count - transformed_count - error_count}")

    # Write errors file
    if errors:
        errors_df = pd.DataFrame(errors)
        errors_df.to_csv(OUTPUT_DIR / "errors.csv", index=False)
        print(f"\n  Errors written to {OUTPUT_DIR / 'errors.csv'} ({len(errors)} rows)")
    else:
        print("\n  No errors! All paths verified.")

    # Step 4: Summary
    print("\n=== SUMMARY ===")
    print(f"  Unique paths transformed: {len(verified_map)}")
    print(f"  Paths not found in GCP: {len(not_found)}")
    print(f"  Error rows logged: {len(errors)}")
    print(f"  Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
