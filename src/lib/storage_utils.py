from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

_config = {
    "storage_account": "storagesa01",
    "storage_container": "biostatistics"
}

def set_storage_config(storage_account: str, storage_container: str):
    """Sets storage configuration in module memory to avoid Spark Connect spark.conf restrictions."""
    _config["storage_account"] = storage_account
    _config["storage_container"] = storage_container

def download_raw_years(schema: StructType, years: list) -> DataFrame:
    """
    Downloads and reads raw CSV discharge files from Azure Blob Storage 
    for a specified list of years using a given Spark schema.
    """
    spark = SparkSession.getActiveSession()
    acc = _config["storage_account"]
    cont = _config["storage_container"]
    file_paths = [
        f"abfss://{cont}@{acc}.dfs.core.windows.net/raw/discharges/{year}.csv" 
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
    acc = _config["storage_account"]
    cont = _config["storage_container"]
    file_path = f"abfss://{cont}@{acc}.dfs.core.windows.net/raw/dimensions/{dim_name}.csv"

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .load(file_path)