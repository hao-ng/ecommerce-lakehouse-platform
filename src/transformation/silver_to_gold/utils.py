from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col
from pyspark.sql import Column

NULL_SENTINEL = "_spark_surrogate_key_null_"


def generate_surrogate_key(columns: list[str], sep: str = "||") -> Column:
    """
    Generate deterministic surrogate key
    Args:
        columns (list[str]): List of column names to be used for generating the surrogate key
        sep (str, optional): Seperator. Defaults to "||".

    Returns:
        Column: A spark Column containing the surrogate key
    Example:
        df.withColumn("surrogate_key", generate_surrogate_key(["col1", "col2"]))
    """
    if not columns:
        raise ValueError("columns must not be empty")

    normalized_cols = [
        concat_ws(
            "=",
            lit(c),
            coalesce(col(c).cast("string"), lit(NULL_SENTINEL)),
        )
        for c in columns
    ]

    return sha2(concat_ws(sep, *normalized_cols), 256)
