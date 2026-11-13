"""
Task for transforming bronze health discharge data into the silver tier,
including date parsing, null filling, data imputation, data quality expectations, and persisting 
as a managed Delta table in Unity Catalog.
"""

import sys
import argparse
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pathlib import Path

CURRENT_DIR = str(Path().resolve())
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from lib import discharges_silver_loader as silver_loader

def process(skip_validation: str = "false") -> DataFrame:
    """
    Reads the bronze `uniformed_discharges` table from the DLT pipeline, applies cleaning 
    and imputation functions from `silver_loader`, adjusts 2018 health insurance codes, 
    and returns the cleaned DataFrame.
    """
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Silver Loading step as requested.")
        return None

    cols_to_load = [
        "date", "gender", "age", "health_insurance", "city", 
        "healthcare_service", "facility", "cause_of_death_1", "cause_of_death_2"
    ]

    raw_df = spark.read.table("lab.biostatistics.discharges_raw").select(*cols_to_load)
    raw_df = raw_df.filter(F.col("date").isNotNull() & (F.trim(F.col("date")) != ""))

    transformed_df = raw_df.select(
        silver_loader.extract_year("date", "year"),
        silver_loader.fill_null_integers("gender"),
        silver_loader.fill_null_integers("age"),
        silver_loader.fill_null_integers_and_impute("health_insurance", default=9, impute=0),
        silver_loader.fill_null_integers_and_impute("city", default=999999, impute=99999),
        silver_loader.fill_null_integers("healthcare_service"),
        silver_loader.clean_text("cause_of_death_1", default="N/E"),
        silver_loader.clean_text("cause_of_death_2", default="N/E")
    )\
    .filter(
        (F.col("year").isNotNull()) & 
        (F.col("year") > F.lit(2000))
    )\
    .withColumn(
        # Ultima condicion
        "health_insurance",
        F.when(
            (F.col("year") == F.lit(2018)) &
            (F.col("health_insurance") == F.lit(9)),
            F.lit(99)
        ).otherwise(F.col("health_insurance"))
    )

    transformed_df.write.mode("overwrite").saveAsTable("lab.biostatistics.discharges_silver")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Loading for hospital discharges")
    parser.add_argument("--skip-validation", default="false", help="Set to true to skip this step")
    args = parser.parse_args()

    try:
        process(args.skip_validation)
    except AssertionError as e:
        print(str(e))
        sys.exit(1) # Explicitly fails the Databricks Job
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)