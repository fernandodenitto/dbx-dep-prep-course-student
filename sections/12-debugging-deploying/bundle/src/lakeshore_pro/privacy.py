"""The S10 masking move, packaged."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr


def pseudonymize_email(df: DataFrame, salt: str,
                       source_col: str = "email") -> DataFrame:
    """A salted hash replaces the raw address."""
    return (df.withColumn("email_hash",
                          expr(f"sha2(concat({source_col}, '{salt}'), 256)"))
            .drop(source_col))
