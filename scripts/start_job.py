#!/usr/bin/env python3
"""
CLI script to create and start a Harvester job run.

Usage:
    python scripts/start_job.py --name "My Harvest" --urls https://example.com https://example.org
    python scripts/start_job.py --name "My Harvest" --urls https://example.com --job-id 10 --max-depth 3 --max-pages 500
"""

import argparse
import json
import sys

import httpx


def main():
    parser = argparse.ArgumentParser(description="Start a Harvester job run")
    parser.add_argument("--name", "-n", required=True, help="Job run name")
    parser.add_argument("--urls", "-u", nargs="+", required=True, help="Seed URLs")
    parser.add_argument("--job-id", type=int, default=None, help="Custom job run ID")
    parser.add_argument("--max-depth", type=int, default=3, help="Maximum crawl depth")
    parser.add_argument("--max-pages", type=int, default=1000, help="Maximum pages to crawl")
    parser.add_argument("--api-url", default="http://localhost:8000", help="Harvester API URL")
    parser.add_argument("--start", action="store_true", help="Start the pipeline immediately after creation")
    parser.add_argument("--config", type=str, default="{}", help="Additional config as JSON string")

    args = parser.parse_args()

    base_url = args.api_url.rstrip("/")

    health = httpx.get(f"{base_url}/api/v1/health", timeout=10)
    health.raise_for_status()
    print(f"API health: {health.json()}")

    payload = {
        "name": args.name,
        "seed_urls": args.urls,
        "max_depth": args.max_depth,
        "max_pages": args.max_pages,
        "config": json.loads(args.config),
    }
    if args.job_id is not None:
        payload["job_run_id"] = args.job_id

    print(f"\nCreating job run: {args.name}")
    print(f"  Seed URLs: {args.urls}")
    print(f"  Max depth: {args.max_depth}")
    print(f"  Max pages: {args.max_pages}")

    response = httpx.post(f"{base_url}/api/v1/jobs", json=payload, timeout=30)
    response.raise_for_status()
    job = response.json()

    print(f"\nJob created successfully!")
    print(f"  Job Run ID: {job['job_run_id']}")
    print(f"  Status: {job['status']}")
    print(f"  ID: {job['id']}")

    if args.start:
        print(f"\nStarting pipeline...")
        start_response = httpx.post(
            f"{base_url}/api/v1/jobs/{job['job_run_id']}/start", timeout=30
        )
        start_response.raise_for_status()
        result = start_response.json()
        print(f"  Pipeline dispatched!")
        print(f"  Celery Task ID: {result['celery_task_id']}")
        print(f"  Status: {result['status']}")

    print(f"\nMonitor progress:")
    print(f"  curl {base_url}/api/v1/jobs/{job['job_run_id']}")
    print(f"  curl {base_url}/api/v1/jobs/{job['job_run_id']}/documents")

    return 0


if __name__ == "__main__":
    sys.exit(main())
