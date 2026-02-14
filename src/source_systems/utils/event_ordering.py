import pandas as pd


def order_event_by_time(file_path: str, by: str) -> pd.DataFrame:
    """Prepare data in correct order to produce

    Args:
        file_path (str): Path to the file

    Returns:
        pd.DataFrame: The sorted DataFrame
    """
    df = pd.read_parquet(file_path, engine="pyarrow")
    df = df.sort_values(
        by=by,
        key=lambda s: pd.to_datetime(s, utc=True, errors="raise"),
    ).reset_index(drop=True)

    return df
