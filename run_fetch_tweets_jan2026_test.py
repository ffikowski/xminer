#!/usr/bin/env python3
"""
Runner script to fetch new tweets from 2026-01-04 onwards into test table.

Usage:
    python run_fetch_tweets_jan2026_test.py
"""
from src.xminer.tasks.fetch_tweets_jan2026_test import main

if __name__ == "__main__":
    print("=" * 80)
    print("FETCHING NEW TWEETS FROM 2026-01-04 INTO TEST TABLE")
    print("=" * 80)
    print()
    main()
