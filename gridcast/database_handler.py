import sqlite3
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from loguru import logger

from gridcast.config import BASE_DATE, DB_DIR
from gridcast.scraping import scrape_ren_datahub


def get_db_path() -> Path:
    """Return the path to the SQLite database file."""
    return DB_DIR / "grid_data.db"


def get_db_connection() -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db() -> None:
    """Initialize the database schema."""
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS grid_data (
                date_time DATETIME PRIMARY KEY,
                hydro REAL,
                wind REAL,
                solar REAL,
                biomass REAL,
                waves REAL,
                gas_combined REAL,
                gas_cogeneration REAL,
                coal REAL,
                other_thermal REAL,
                import REAL,
                export REAL,
                pumped_storage REAL,
                battery_injection REAL,
                battery_consumption REAL,
                consumption REAL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def get_latest_timestamp() -> Optional[dt]:
    """Get the latest timestamp from the database."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT MAX(date_time) FROM grid_data")
        result = cursor.fetchone()[0]
        return dt.strptime(result, "%Y-%m-%d %H:%M:%S") if result else None
    finally:
        conn.close()


def insert_data(conn: sqlite3.Connection, data: pd.DataFrame) -> None:
    """Insert data into the database."""
    try:
        columns = ", ".join(data.columns)
        placeholders = ", ".join(["?"] * len(data.columns))
        sql = f"INSERT OR REPLACE INTO grid_data ({columns}) VALUES ({placeholders})"
        cursor = conn.cursor()
        cursor.executemany(sql, data.to_numpy().tolist())
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error inserting data into grid_data: {e}")


def update() -> None:
    """ Update the data with the latest data from the REN datahub. """
    init_db()
    yesterday = dt.now() - timedelta(days=1)
    final_date = yesterday.strftime("%Y-%m-%d")
    max_db_date = get_latest_timestamp()
    start_date = (
        (max_db_date - timedelta(days=1)).strftime("%Y-%m-%d")
        if max_db_date else BASE_DATE
    )
    logger.info(f"Scraping data from {start_date} to {final_date}")
    new_data = scrape_ren_datahub(start_date, final_date)
    if not new_data.empty:
        conn = get_db_connection()
        try:
            insert_data(conn, new_data)
            conn.commit()
            logger.info(f"Successfully updated {len(new_data)} records")
        finally:
            conn.close()
    return


def load() -> pd.DataFrame:
    """
    Load the data from the SQLite database.

    Returns
    -------
    pd.DataFrame
        The complete dataset.
    """
    init_db()
    conn = get_db_connection()
    try:
        query = "SELECT * FROM grid_data ORDER BY date_time"
        df = pd.read_sql_query(query, conn, parse_dates=["date_time"], index_col="date_time")
        return df
    finally:
        conn.close()
