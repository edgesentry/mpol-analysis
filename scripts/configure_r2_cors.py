"""Configure CORS on the arktrace-public R2 bucket for browser app access.

The Vite SPA (app/) fetches Parquet files and ducklake_manifest.json directly
from arktrace-public.edgesentry.io.  Without CORS, browsers block these
cross-origin requests.

This script applies the CORS policy via the S3 API.  Run it once after
creating the bucket or whenever the allowed origins change.

Usage:
    uv run python scripts/configure_r2_cors.py [--dry-run]

Required env vars (same credentials used for sync_r2.py push):
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
"""

from __future__ import annotations

import argparse
import json
import os
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_DEFAULT_ENDPOINT = "https://b8a0b09feb89390fb6e8cf4ef9294f48.r2.cloudflarestorage.com"
_DEFAULT_BUCKET = "arktrace-public"

# CORS policy: allow GET/HEAD from the Cloudflare Pages origin and localhost dev
CORS_RULES = [
    {
        "AllowedOrigins": [
            "https://arktrace.pages.dev",
            "https://*.arktrace.pages.dev",
            "https://demo.arktrace.edgesentry.io",
            "http://localhost:5173",
            "http://localhost:4173",
        ],
        "AllowedMethods": ["GET", "HEAD"],
        "AllowedHeaders": ["*"],
        "ExposeHeaders": ["Content-Length", "Content-Type", "ETag"],
        "MaxAgeSeconds": 3600,
    }
]


def configure_cors(bucket: str, dry_run: bool = False) -> int:
    import boto3  # type: ignore[import]
    from botocore.config import Config  # type: ignore[import]

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        print(
            "Error: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set.",
            file=sys.stderr,
        )
        return 1

    endpoint = os.getenv("S3_ENDPOINT", _DEFAULT_ENDPOINT)

    cors_config = {"CORSRules": CORS_RULES}

    print(f"Bucket  : {bucket}")
    print(f"Endpoint: {endpoint}")
    print(f"CORS policy:\n{json.dumps(cors_config, indent=2)}")
    print()

    if dry_run:
        print("[dry-run] No changes applied.")
        return 0

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )

    try:
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)
        print(f"✓ CORS configured on {bucket}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    # Verify
    try:
        result = s3.get_bucket_cors(Bucket=bucket)
        rules = result.get("CORSRules", [])
        print(f"✓ Verified: {len(rules)} CORS rule(s) active.")
    except Exception as exc:
        print(f"[warn] Could not verify CORS: {exc}", file=sys.stderr)

    print()
    print("Allowed origins:")
    for origin in CORS_RULES[0]["AllowedOrigins"]:
        print(f"  {origin}")
    print()
    print("Done. The browser app can now fetch Parquet files from:")
    print("  https://arktrace-public.edgesentry.io/ducklake_manifest.json")
    print("  https://arktrace-public.edgesentry.io/data/...")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Configure CORS on arktrace-public R2 for browser app access",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--bucket",
        default=_DEFAULT_BUCKET,
        help=f"R2 bucket name (default: {_DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the CORS policy without applying it",
    )
    args = parser.parse_args()
    return configure_cors(args.bucket, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
