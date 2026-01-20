import pandas as pd


def prepare_data(file_path: str) -> pd.DataFrame:
    """Prepare data in correct order to produce

    Args:
        file_path (str): Path to the file

    Returns:
        pd.DataFrame: The sorted DataFrame
    """
    df = pd.read_parquet(file_path, engine="pyarrow")
    df = df.sort_values(
        by="event_time",
        key=lambda s: pd.to_datetime(s, utc=True, errors="raise"),
    ).reset_index(drop=True)

    return df
