"""
PySpark utility module providing data cleaning, null handling, 
and date extraction transformation functions for dataframe columns.
"""

from pyspark.sql import functions as F
from pyspark.sql.column import Column

def clean_text(column: str, default: str = "999999") -> Column:
    """
    Cleans a text column by removing periods and commas, trimming whitespace, 
    converting text to uppercase, and replacing null or empty values with a default string.

    :param column: Name of the string column to clean.
    :param default: Default fallback value if the column is null or empty. Default is "999999".
    :return: A PySpark Column transformation.
    """
    tfm = F.upper(
        F.trim(
            F.regexp_replace(F.col(column), "[.,]", "")
        )
    )

    return F.when(
        (tfm == "") | (tfm.isNull()),
        default
    ).otherwise(
        tfm
    ).alias(column)

def fill_null_integers(column: str, default: int = 999999) -> Column:
    """
    Replaces null values in an integer column with a specified default value.

    :param column: Name of the integer column to process.
    :param default: Default integer value to substitute for nulls. Default is 999999.
    :return: A PySpark Column transformation.
    """
    return F.coalesce(F.col(column), F.lit(default)).alias(column)

def fill_null_integers_and_impute(column: str, default: int, impute: int) -> Column:
    """
    Replaces null values with a default and additionally replaces 
    a specific imputed or placeholder integer value with the same default.

    :param column: Name of the integer column to process.
    :param default: Default integer value to substitute.
    :param impute: Specific value to treat as missing/invalid and replace with the default.
    :return: A PySpark Column transformation.
    """
    exp = F.coalesce(F.col(column), F.lit(default))
    return F.when(exp == F.lit(impute), F.lit(default))\
        .otherwise(exp)\
        .alias(column)

def extract_year(source_column: str, new_column: str) -> Column:
    """
    Attempts to parse a date string from a source column using multiple 
    common date formats, coerces it to a date, and extracts the year into a new column.

    :param source_column: Name of the source column containing date strings.
    :param new_column: Name of the resulting column where the extracted year will be stored.
    :return: A PySpark Column transformation.
    """
    clean_str = F.trim(F.col(source_column))
    parsed_date = F.coalesce(
        F.try_to_timestamp(clean_str, F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(clean_str, F.lit("dd-M-yyyy")),
        F.try_to_timestamp(clean_str, F.lit("dd-MM-yyyy")),
        F.try_to_timestamp(clean_str, F.lit("M/d/yyyy")),
        F.try_to_timestamp(clean_str, F.lit("MM/dd/yyyy"))
    )
    return F.year(parsed_date).alias(new_column)