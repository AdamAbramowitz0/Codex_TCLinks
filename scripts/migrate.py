#!/usr/bin/env python3
"""Apply SQL migrations for SQLite or PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_migration_files(engine: str):
    folder = ROOT / "migrations" / engine
    if not folder.exists():
        raise RuntimeError(f"Migration folder does not exist: {folder}")
    return sorted(folder.glob("*.sql"))


def apply_sqlite(db_path: str, files):
    conn = sqlite3.connect(db_path)
    try:
        for file in files:
            sql = file.read_text(encoding="utf-8")
            conn.executescript(sql)
        conn.commit()
    finally:
        conn.close()


def apply_postgres(database_url: str, files):
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for postgres migrations") from exc

    conn = psycopg.connect(database_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for file in files:
                    sql = file.read_text(encoding="utf-8")
                    cur.execute(sql)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply DB migrations")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="PostgreSQL URL (if set, postgres migrations are used)",
    )
    parser.add_argument(
        "--database-path",
        default=os.getenv("DATABASE_PATH", "market.db"),
        help="SQLite DB path",
    )
    args = parser.parse_args()

    if args.database_url:
        files = load_migration_files("postgres")
        apply_postgres(args.database_url, files)
        print(f"Applied {len(files)} postgres migration files")
    else:
        files = load_migration_files("sqlite")
        apply_sqlite(args.database_path, files)
        print(f"Applied {len(files)} sqlite migration files to {args.database_path}")


if __name__ == "__main__":
    main()
