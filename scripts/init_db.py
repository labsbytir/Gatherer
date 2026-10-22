#!/usr/bin/env python3
from harvester.db import init_db
from harvester.utils.logging import configure_logging

configure_logging("INFO")

if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("Done!")
