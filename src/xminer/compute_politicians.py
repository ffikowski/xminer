import pandas as pd
from sqlalchemy import text
from .db import engine

def main():
    df = pd.read_sql_query(text("""
        SELECT id, vorname, nachname, partei_kurz, geburtsdatum, geschlecht
        FROM politicians
    """), engine)

    gb = pd.to_datetime(df["geburtsdatum"], errors="coerce", utc=True)
    now = pd.Timestamp.now(tz="UTC")
    df["age_years"] = ((now - gb).dt.days / 365.25).round(1)

    outdir = "outputs"
    os.makedirs(outdir, exist_ok=True)
    df.to_csv(f"{outdir}/politicians_with_age.csv", index=False)
    print("Saved outputs/politicians_with_age.csv")

if __name__ == "__main__":
    import os
    main()
