#!/usr/bin/env python3
"""
Test script to fetch tweets from 2026-01-04 for a small sample of politicians.
This is a safe test before running the full fetch.
"""
import sys
import os

# Temporarily override the sample limit for testing
os.environ['TWEETS_SAMPLE_LIMIT'] = '5'  # Test with only 5 politicians

from src.xminer.tasks.fetch_tweets_jan2026_test import main, logger

if __name__ == "__main__":
    print("=" * 80)
    print("TEST RUN: Fetching tweets from 2026-01-04 for 5 politicians")
    print("=" * 80)
    print()
    print("This will:")
    print("  1. Create test table: tweets_test_jan2026")
    print("  2. Fetch tweets for 5 random politicians")
    print("  3. Show summary of what was fetched")
    print()
    print("Press Ctrl+C to cancel, or wait 5 seconds to continue...")
    print()

    import time
    try:
        for i in range(5, 0, -1):
            print(f"Starting in {i}...", end='\r')
            time.sleep(1)
        print()

        # Override the Params class to use our test sample limit
        from src.xminer.config.params import Params
        Params.tweets_sample_limit = 5

        main()

        print()
        print("=" * 80)
        print("TEST COMPLETED!")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Check the log file: logs/fetch_tweets_jan2026_test.log")
        print("  2. Verify test table:")
        print("     python -m src.xminer.tasks.merge_test_tweets_to_main verify")
        print()
        print("If everything looks good, run the full fetch:")
        print("     python run_fetch_tweets_jan2026_test.py")
        print()

    except KeyboardInterrupt:
        print("\n\nTest cancelled by user.")
        sys.exit(0)
