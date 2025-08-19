import os
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ENV = os.getenv("ENV", "dev")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")
