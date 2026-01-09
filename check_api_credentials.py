#!/usr/bin/env python3
"""
Check if API credentials are properly configured.
"""
import os
from dotenv import load_dotenv

print("=" * 80)
print("CHECKING API CREDENTIALS")
print("=" * 80)
print()

# Load .env file
load_dotenv()

# Check for required environment variables
print("1. Checking .env file...")
env_file = ".env"
if os.path.exists(env_file):
    print(f"   ✅ .env file found: {env_file}")
else:
    print(f"   ❌ .env file NOT found!")
    print()
    print("   Create a .env file with:")
    print("   DATABASE_URL=postgresql+psycopg2://user:password@host:port/database")
    print("   X_BEARER_TOKEN=your_twitter_bearer_token_here")
    exit(1)

print()
print("2. Checking environment variables...")

# Check DATABASE_URL
db_url = os.getenv("DATABASE_URL")
if db_url:
    # Hide password in output
    safe_url = db_url.split('@')[0].rsplit(':', 1)[0] + ':****@' + db_url.split('@')[1] if '@' in db_url else "****"
    print(f"   ✅ DATABASE_URL: {safe_url}")
else:
    print("   ❌ DATABASE_URL not set!")

# Check X_BEARER_TOKEN
bearer_token = os.getenv("X_BEARER_TOKEN")
if bearer_token:
    # Show only first and last few characters
    safe_token = bearer_token[:10] + "..." + bearer_token[-10:] if len(bearer_token) > 20 else "****"
    print(f"   ✅ X_BEARER_TOKEN: {safe_token}")
else:
    print("   ❌ X_BEARER_TOKEN not set!")

print()
print("3. Testing Twitter API connection...")

# Check which API mode is configured
api_mode = os.getenv("X_API_MODE", "official")
print(f"   API Mode: {api_mode}")

try:
    from src.xminer.io.x_api_dual import client

    # Try a simple API call
    me = client.get_me()

    if me and me.data:
        print(f"   ✅ API connection successful!")
        print(f"   Mode: {client.mode}")
        if hasattr(me.data, 'username'):
            print(f"   Authenticated as: @{me.data.username}")
    else:
        print("   ⚠️  API connected but no user data returned")

except Exception as e:
    print(f"   ❌ API connection failed: {str(e)}")
    print()
    if api_mode == "twitterapiio":
        print("   Check your twitterapiio_API_KEY is valid.")
    else:
        print("   Make sure your X_BEARER_TOKEN is valid.")
        print("   Get your bearer token from: https://developer.twitter.com/")

print()
print("4. Testing database connection...")

try:
    from src.xminer.io.db import engine
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM tweets"))
        count = result.scalar()
        print(f"   ✅ Database connection successful!")
        print(f"   Current tweets in database: {count:,}")

except Exception as e:
    print(f"   ❌ Database connection failed: {str(e)}")

print()
print("=" * 80)
print("CREDENTIAL CHECK COMPLETE")
print("=" * 80)
print()
print("If all checks passed, you can proceed with:")
print("  python test_fetch_jan2026.py")
print()
