from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

TABLES = [
    "dim_creator",
    "dim_show",
    "dim_episode",
    "dim_listener",
    "fct_events",
    "fct_ads",
]

DDL = {
    "dim_creator": """
        CREATE TABLE IF NOT EXISTS dim_creator (
            creator_id  INT PRIMARY KEY,
            country     TEXT,
            tier        TEXT,
            join_ts     TIMESTAMPTZ
        );
    """,
    "dim_show": """
        CREATE TABLE IF NOT EXISTS dim_show (
            show_id     INT PRIMARY KEY,
            creator_id  INT REFERENCES dim_creator(creator_id),
            category    TEXT,
            language    TEXT
        );
    """,
    "dim_episode": """
        CREATE TABLE IF NOT EXISTS dim_episode (
            episode_id  INT PRIMARY KEY,
            show_id     INT REFERENCES dim_show(show_id),
            creator_id  INT REFERENCES dim_creator(creator_id),
            publish_ts  TIMESTAMPTZ,
            duration_s  INT
        );
    """,
    "dim_listener": """
        CREATE TABLE IF NOT EXISTS dim_listener (
            listener_id INT PRIMARY KEY,
            region      TEXT,
            signup_ts   TIMESTAMPTZ
        );
    """,
    "fct_events": """
        CREATE TABLE IF NOT EXISTS fct_events (
            event_ts    TIMESTAMPTZ,
            event_type  TEXT,
            listener_id INT NULL,
            creator_id  INT NULL,
            show_id     INT NULL,
            episode_id  INT NULL
        );
    """,
    "fct_ads": """
        CREATE TABLE IF NOT EXISTS fct_ads (
            ad_ts              TIMESTAMPTZ,
            creator_id         INT,
            show_id            INT,
            episode_id         INT,
            impressions        INT,
            filled_impressions INT,
            cpm_usd            DOUBLE PRECISION,
            revenue_usd        DOUBLE PRECISION
        );
    """,
}


def main() -> None:
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not found. Put it in .env")

    # Prefer psycopg3 driver when a generic PostgreSQL URL is provided
    if db_url.startswith("postgresql://") and "+psycopg" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    engine = create_engine(db_url)

    base = Path("data/generated")
    if not base.exists():
        raise FileNotFoundError("data/generated not found. Run generator first.")

    with engine.begin() as conn:
        # Create tables
        for ddl in DDL.values():
            conn.execute(text(ddl))

        # Truncate for idempotent loads
        for t in reversed(TABLES):
            conn.execute(text(f"TRUNCATE TABLE {t} CASCADE;"))

    # Load CSVs
    for t in TABLES:
        path = base / f"{t}.csv"
        df = pd.read_csv(path)

        # ensure timestamps parse
        for col in df.columns:
            if col.endswith("_ts"):
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        df.to_sql(t, engine, if_exists="append", index=False, method="multi", chunksize=5000)
        print(f"Loaded {t}: {len(df):,} rows")

    print("✅ Done")


if __name__ == "__main__":
    main()