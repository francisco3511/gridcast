from datetime import datetime as dt
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
    return df


def update_data() -> pd.DataFrame:
    """
    Update the data with the latest data from the REN datahub.

    Returns
    -------
    pd.DataFrame
        The updated data.
    """

    today = dt.now().strftime("%Y-%m-%d")
    filepath = RAW_DATA_DIR / "production_breakdown.csv"

    try:
        df = pd.read_csv(RAW_DATA_DIR / "production_breakdown.csv")
        last_date = dt.strptime(df.iloc[-1]["Date"], "%Y-%m-%d") + dt.timedelta(days=1)
        new_data = scrape_data(last_date, today)
        df = pd.concat([df, new_data])
        df = df.drop_duplicates(subset=["Date"])
        df.to_csv(filepath, index=False)
    except FileNotFoundError:
        logger.info("No data found, scraping new data")
        df = scrape_data(BASE_DATE, dt.strptime(today, "%Y-%m-%d") )
        df.to_csv(filepath, index=False)

    return df
