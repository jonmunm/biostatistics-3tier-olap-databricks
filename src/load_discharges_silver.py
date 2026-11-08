"""
Delta Live Tables (DLT) module for transforming bronze health discharge data into the silver tier,
including date parsing, null filling, data imputation, data quality expectations, and persisting 
as a managed Delta table in Unity Catalog.
"""

import sys
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from lib import discharges_silver_loader as silver_loader

def process() -> DataFrame:
    """
    Reads the bronze `uniformed_discharges` table from the DLT pipeline, applies cleaning 
    and imputation functions from `silver_loader`, adjusts 2018 health insurance codes, 
    and returns the cleaned DataFrame.
    """

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
    )

    final_df = transformed_df.withColumn(
        "health_insurance",
        F.when(
            (F.col("year") == F.lit(2018)) &
            (F.col("health_insurance") == F.lit(9)),
            F.lit(99)
        ).otherwise(F.col("health_insurance"))
    )

    final_df.write.mode("overwrite").saveAsTable("lab.biostatistics.discharges_silver")

if __name__ == "__main__":
    process()