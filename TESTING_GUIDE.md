# Quick Testing Guide

## Step 1: Check API Credentials

Your API credentials should already be in the `.env` file. To verify:

```bash
python check_api_credentials.py
```

This will check:
- ✅ `.env` file exists
- ✅ `DATABASE_URL` is set
- ✅ `X_BEARER_TOKEN` is set
- ✅ Twitter API connection works
- ✅ Database connection works

### If credentials are missing:

Your `.env` file should look like:

```env
# Database connection
DATABASE_URL=postgresql+psycopg2://user:password@host:port/database

# X API credentials
X_BEARER_TOKEN=your_twitter_bearer_token_here

# Environment
ENV=dev
```

**To get your Twitter Bearer Token:**
1. Go to https://developer.twitter.com/
2. Create an app or use existing app
3. Go to "Keys and tokens"
4. Generate/copy the "Bearer Token"
5. Paste it in your `.env` file

## Step 2: Test with Small Sample (RECOMMENDED)

Test with just 5 politicians first:

```bash
python test_fetch_jan2026.py
```

This will:
- Create the test table
- Fetch tweets for 5 random politicians
- Show you what was fetched
- Create log file: `logs/fetch_tweets_jan2026_test.log`

## Step 3: Verify Test Results

```bash
python -m src.xminer.tasks.merge_test_tweets_to_main verify
```

This shows:
- How many tweets were fetched
- Which politicians were included
- Date ranges
- What's new vs duplicates

## Step 4: Run Full Fetch (if test passed)

If the test looks good, fetch for all politicians:

```bash
python run_fetch_tweets_jan2026_test.py
```

## Step 5: Merge to Main Table

After fetching all tweets:

```bash
# Verify what will be merged
python -m src.xminer.tasks.merge_test_tweets_to_main verify

# Test merge (no changes)
python -m src.xminer.tasks.merge_test_tweets_to_main merge --dry-run

# Actually merge
python -m src.xminer.tasks.merge_test_tweets_to_main merge --live

# Cleanup test table
python -m src.xminer.tasks.merge_test_tweets_to_main cleanup
```

## Troubleshooting

### "X_BEARER_TOKEN not set"
- Check your `.env` file has `X_BEARER_TOKEN=...`
- Make sure there are no spaces around the `=`
- Make sure the token is on one line

### "API connection failed"
- Verify your bearer token is valid
- Try generating a new token from Twitter Developer Portal
- Check if your app has the right permissions

### "Database connection failed"
- Verify `DATABASE_URL` in `.env` is correct
- Make sure PostgreSQL is running
- Check your database credentials

### "Rate limit hit"
- The script automatically waits and retries
- This is normal for large fetches
- Check the log file for details

## Files Created

```
xminer/
├── .env                                      # Your credentials (DO NOT COMMIT!)
├── check_api_credentials.py                 # Credential checker
├── test_fetch_jan2026.py                    # Small test run
├── run_fetch_tweets_jan2026_test.py         # Full fetch run
└── logs/
    └── fetch_tweets_jan2026_test.log        # Detailed logs
```

## Current Configuration

From your `parameters.yml`:
- **Year/Month:** 2025-12
- **Sample limit:** -1 (all politicians)
- **Sample seed:** 42 (for reproducible random selection)
- **Start date for NEW tweets:** 2026-01-04

## Quick Reference

| Command | Purpose |
|---------|---------|
| `python check_api_credentials.py` | Verify credentials |
| `python test_fetch_jan2026.py` | Test with 5 politicians |
| `python run_fetch_tweets_jan2026_test.py` | Fetch all |
| `python -m src.xminer.tasks.merge_test_tweets_to_main verify` | Check results |
| `python -m src.xminer.tasks.merge_test_tweets_to_main merge --live` | Merge to main |
