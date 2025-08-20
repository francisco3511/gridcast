from datetime import datetime as dt
from datetime import timedelta
from io import StringIO

import pandas as pd
import requests
from loguru import logger

from gridcast.config import BASE_DATE, RAW_DATA_DIR


def scrape_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Scrape data from the REN datahub and optionally save it locally.

    Parameters
    ----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format

    Returns
    -------
    pd.DataFrame
        The scraped data.
    """

    url = (
        "https://datahub.ren.pt/service/download/csv/"
        f"1354?startDateString={start_date}&endDateString={end_date}&culture=pt-PT"
    )

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Request failed: {e}")
        raise Exception(f"Request failed: {e}") from e

    df = pd.read_csv(StringIO(response.text), encoding="utf-8", sep=';', skiprows=2, header=0)
    df = df.sort_values(by=["Data e Hora"]).reset_index(drop=True)
    return df


def update_data() -> pd.DataFrame:
    """
    Update the data with the latest data from the REN datahub.

    Returns
    -------
    pd.DataFrame
        The updated data.
    """

    today = dt.now() - timedelta(days=1)
    last_date = today.strftime("%Y-%m-%d")
    filepath = RAW_DATA_DIR / "production_breakdown.csv"

    try:
        df = pd.read_csv(RAW_DATA_DIR / "production_breakdown.csv")
        first_date = (
            dt.strptime(df.iloc[-1]["Data e Hora"], "%Y-%m-%d %H:%M:%S")
            - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        new_data = scrape_data(first_date, last_date)
        df = pd.concat([df, new_data])
        df = df.drop_duplicates(subset=["Data e Hora"], keep="last").reset_index(drop=True)
        df.to_csv(filepath, index=False)
    except FileNotFoundError:
        logger.info("No data found, scraping new data")
        df = scrape_data(BASE_DATE, dt.strftime(last_date, "%Y-%m-%d"))
        df.to_csv(filepath, index=False)

    return df


def load_data() -> pd.DataFrame:
    """
    Load the data from the CSV file.
    """
    df = pd.read_csv(RAW_DATA_DIR / "production_breakdown.csv", parse_dates=["Data e Hora"])
    df["Data e Hora"] = pd.to_datetime(df["Data e Hora"])
    return df
