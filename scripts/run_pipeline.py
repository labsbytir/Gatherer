#!/usr/bin/env python3
"""
Run the full 8-stage pipeline for a job run.

Usage:
    python scripts/run_pipeline.py --urls https://example.com https://docs.example.com
    python scripts/run_pipeline.py --urls https://example.com --job-id 10 --name "My Harvest"
    python scripts/run_pipeline.py --urls https://example.com --api-url http://localhost:8000
"""

import argparse
import sys

import httpx


def main():
    parser = argparse.ArgumentParser(description="Run the Harvester 8-stage pipeline")
    parser.add_argument("--urls", "-u", nargs="+", required=True, help="Seed URLs")
    parser.add_argument("--job-id", type=int, default=10, help="Job run ID (default: 10)")
    parser.add_argument("--name", "-n", default="Harvest Run", help="Job run name")
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--api-url", default="http://localhost:8000")

    args = parser.parse_args()
    base = args.api_url.rstrip("/")

    print(f"Starting 8-stage pipeline")
    print(f"  Job Run ID: {args.job_id}")
    print(f"  Name: {args.name}")
    print(f"  Seed URLs: {args.urls}")
    print(f"  Max Depth: {args.max_depth}")
    print(f"  Max Pages: {args.max_pages}")
    print()

    payload = {
        "job_run_id": args.job_id,
        "name": args.name,
        "seed_urls": args.urls,
        "max_depth": args.max_depth,
        "max_pages": args.max_pages,
    }

    print("Submitting pipeline...")
    resp = httpx.post(f"{base}/api/v1/jobs/{args.job_id}/run", json=payload, timeout=600)
    resp.raise_for_status()
    result = resp.json()

    print(f"\nPipeline complete!")
    print(f"  Status:       {result['status']}")
    print(f"  Discovered:   {result['discovered']}")
    print(f"  Indexed:      {result['indexed']}")
    print(f"  Extracted:    {result['extracted']}")
    print(f"  OCR:          {result['ocr']}")
    print(f"  Transformed:  {result['transformed']}")
    print(f"  Stored:       {result['stored']}")
    print(f"  Validation:   {result['validation']}")

    print(f"\nView results:")
    print(f"  curl {base}/api/v1/jobs/{args.job_id}")
    print(f"  curl {base}/api/v1/jobs/{args.job_id}/items")

    return 0


if __name__ == "__main__":
    sys.exit(main())
