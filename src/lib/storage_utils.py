from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

STORAGE_ACCOUNT = "storagesa01"
STORAGE_CONTAINER = "biostatistics"
    
def download_raw_years(schema: StructType, years: list) -> DataFrame:
    """
    Downloads and reads raw CSV discharge files from Azure Blob Storage 
    for a specified list of years using a given Spark schema.

    :param spark: Active SparkSession instance.
    :param schema: PySpark StructType defining the schema for the target years.
    :param years: List of year integers corresponding to the files to download.
    :return: A DataFrame containing the loaded raw data.
    """
    spark = SparkSession.getActiveSession()
    file_paths = [
        f"abfss://{STORAGE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/discharges/{year}.csv" 
        for year in years
    ]

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .schema(schema) \
        .load(file_paths)

def download_dimension(dim_name: str) -> DataFrame:
    """
    Downloads a single dimension CSV file from Azure Blob Storage.
    """
    spark = SparkSession.getActiveSession()
    file_path = f"abfss://{STORAGE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/dimensions/{dim_name}.csv"

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .load(file_path)