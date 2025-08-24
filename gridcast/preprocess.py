from gridcast.database_handler import load
from gridcast.config import PROCESSED_DATA_DIR


def save_hourly_data() -> None:
    """Resample the data to hourly means."""
    df = load()
    df = df.resample("h").mean()
    df.to_parquet(PROCESSED_DATA_DIR / "grid_data_hourly.parquet")
    return
