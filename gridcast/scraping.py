import pandas as pd
import requests
from io import StringIO
from loguru import logger

from gridcast.config import GRID_COLUMNS


def scrape_ren_datahub(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Scrape data from the REN datahub.

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

    df = pd.read_csv(
        StringIO(response.text),
        encoding="utf-8",
        sep=';',
        skiprows=2,
        header=0,
        parse_dates=["Data e Hora"]
    )
    df.columns = GRID_COLUMNS
    df = df.sort_values(by=["date_time"]).reset_index(drop=True)
    df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df
