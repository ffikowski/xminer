# Guide: Fetching New Tweets from 2026-01-04

This guide explains how to fetch new tweets from January 4, 2026 onwards into a test table, verify them, and merge them into the main tweets table.

## Overview

You currently have 220 tweets in the main `tweets` table with `created_at >= 2026-01-04`. This process will:
1. Fetch any NEW tweets from the API that were posted after 2026-01-04
2. Store them in a test table `tweets_test_jan2026`
3. Allow you to verify the data before merging
4. Merge the data into the main `tweets` table

## Step 1: Fetch New Tweets into Test Table

```bash
python run_fetch_tweets_jan2026_test.py
```

**What this does:**
- Creates a test table `tweets_test_jan2026` (if it doesn't exist)
- For each politician, fetches tweets posted after 2026-01-04
- Uses the latest tweet ID from the main table as a starting point
- Stores results in the test table
- Creates a log file: `logs/fetch_tweets_jan2026_test.log`

**Configuration:**
The script uses your existing `parameters.yml` settings for:
- `tweets_sample_limit`: Limit number of profiles to fetch (set to -1 for all)
- `sample_seed`: Random seed for sampling
- `tweet_fields`: Fields to fetch from API

## Step 2: Verify Test Table Data

After fetching, verify the data before merging:

```bash
python -m src.xminer.tasks.merge_test_tweets_to_main verify
```

**This shows:**
- Total tweets in test table
- Date range of tweets
- Number of unique authors
- Top 10 authors by tweet count
- How many tweets are duplicates vs new
- Breakdown of what will be merged

## Step 3: Merge to Main Table (Dry Run)

Test the merge without making changes:

```bash
python -m src.xminer.tasks.merge_test_tweets_to_main merge --dry-run
```

**This shows:**
- What data would be merged
- No actual changes made
- Safe to run multiple times

## Step 4: Merge to Main Table (Live)

Once you're satisfied, do the actual merge:

```bash
python -m src.xminer.tasks.merge_test_tweets_to_main merge --live
```

**This will:**
- Insert all tweets from test table into main `tweets` table
- Use ON CONFLICT to update metrics if tweet already exists
- Show final tweet counts

## Step 5: Cleanup (Optional)

After successful merge, drop the test table:

```bash
python -m src.xminer.tasks.merge_test_tweets_to_main cleanup
```

## File Structure

```
xminer/
├── run_fetch_tweets_jan2026_test.py          # Main runner script
├── src/xminer/tasks/
│   ├── fetch_tweets_jan2026_test.py          # Fetching logic
│   └── merge_test_tweets_to_main.py          # Merge/verify logic
└── logs/
    └── fetch_tweets_jan2026_test.log         # Fetch log file
```

## Database Tables

- **Main table:** `tweets` - Production tweets table
- **Test table:** `tweets_test_jan2026` - Temporary test table for new tweets
- **Schema:** Both tables have identical structure

## Important Notes

1. **API Rate Limits:** The script handles Twitter API rate limits automatically
2. **Upsert Logic:** If a tweet already exists (by tweet_id), it updates the metrics
3. **Test First:** Always use the test table workflow before touching production data
4. **Logs:** Check `logs/fetch_tweets_jan2026_test.log` for detailed progress

## Example Workflow

```bash
# 1. Fetch new tweets
python run_fetch_tweets_jan2026_test.py

# 2. Check what was fetched
python -m src.xminer.tasks.merge_test_tweets_to_main verify

# 3. Test merge (dry run)
python -m src.xminer.tasks.merge_test_tweets_to_main merge --dry-run

# 4. Actual merge
python -m src.xminer.tasks.merge_test_tweets_to_main merge --live

# 5. Cleanup test table
python -m src.xminer.tasks.merge_test_tweets_to_main cleanup
```

## Troubleshooting

**Error: "No tweets found in main table for [username]"**
- This is normal for politicians with no tweets yet
- The script skips them and continues

**Error: Rate limit hit**
- Script automatically waits and retries
- Check logs for details

**Error: Table already exists**
- Test table persists between runs
- You can drop it with the cleanup command or continue using it

## Current Status

Based on your database:
- **Total tweets in main table:** 44,201
- **Tweets after 2026-01-04:** 220
- **Date range:** 2025-02-15 to 2026-01-04

After running this process, you should have additional tweets from 2026-01-04 onwards that weren't captured in the original fetch.
