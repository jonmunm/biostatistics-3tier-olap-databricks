"""
Delta Live Tables (DLT) module for transforming silver health discharge data into the gold tier,
performing dimension lookups from ADLS Gen2 (via Unity Catalog External Location), recoding 
causes of death, validating city CUTs, and materializing the final Gold fact table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

STORAGE_ACCOUNT = "storagesa01"
STORAGE_CONTAINER = "biostatistics"
DIMENSIONS_BASE_PATH = f"wasbs://{STORAGE_CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/raw/dimensions/"

def read_dimension_csv(filename: str) -> DataFrame:
    """
    Helper function to read dimension and lookup CSV files from the External Location
    using the active SparkSession provided by the DLT runtime.
    """
    spark = SparkSession.getActiveSession()
    return (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .csv(f"{DIMENSIONS_BASE_PATH}/{filename}")
    )

def process() -> DataFrame:
    """
    Reads the `silver_discharges` DLT table and joins it with dimension lookup CSVs 
    to produce the final star-schema fact table.
    """

    dim_gender_df = read_dimension_csv("dim_gender.csv").select(
        F.col("id").cast(IntegerType()).alias("id")
    )

    dim_age_df = read_dimension_csv("dim_age.csv").select(
        (F.col("age_id").cast(IntegerType()) + 1).alias("real_age"),
        F.col("id").cast(IntegerType()).alias("age_id")
    )

    dim_year_df = read_dimension_csv("dim_year.csv").select(
        (F.col("id").cast(IntegerType()) + 1989).alias("real_year"),
        F.col("id").cast(IntegerType()).alias("year_id")
    )

    dim_health_insurance_df = read_dimension_csv("dim_health_insurance.csv").select(
        F.col("id").cast(IntegerType()).alias("health_insurance_id"),
        F.col("health_insurance_code").cast(IntegerType()).alias("health_insurance_code"),
        F.col("valid_from").cast(IntegerType()).alias("valid_from"),
        F.col("valid_to").cast(IntegerType()).alias("valid_to")
    )

    dim_healthcare_service_df = read_dimension_csv("dim_healthcare_service.csv").select(
        F.col("id").cast(IntegerType()).alias("healthcare_service_id"),
        F.col("healthcare_service_cut").cast(IntegerType()).alias("healthcare_service_cut"),
        F.col("valid_from").cast(IntegerType()).alias("valid_from"),
        F.col("valid_to").cast(IntegerType()).alias("valid_to")
    )

    dim_city_df = read_dimension_csv("dim_city.csv").select(
        F.col("id").cast(IntegerType()).alias("city_id"),
        F.col("city_cut").cast(IntegerType()).alias("city_cut"),
        F.col("valid_from").cast(IntegerType()).alias("valid_from"),
        F.col("valid_to").cast(IntegerType()).alias("valid_to")
    )

    cod_recod_df = read_dimension_csv("cause_of_death.recoded.lookup.csv")

    dim_cause_of_death_df = read_dimension_csv("dim_cause_of_death.csv").select(
        F.col("id").cast(IntegerType()).alias("id"),
        F.col("subcategory_code")
    )

    invalid_cuts_df = read_dimension_csv("city.invalid_cuts.lookup.csv").select(
        F.col("CUT").cast(IntegerType()).alias("CUT")
    )

    discharges_df = (
        spark.read.table("lab.biostatistics.discharges_silver")
        .join(F.broadcast(dim_gender_df), F.col("gender") == F.col("id"), how="left")
        .join(F.broadcast(dim_year_df), F.col("year") == F.col("real_year"), how="left")
        .join(F.broadcast(dim_age_df), F.col("age") == F.col("real_age"), how="left")
        .join(
            F.broadcast(dim_health_insurance_df.alias("hi")),
            (F.col("year") >= F.col("hi.valid_from")) & 
            (F.col("year") <= F.col("hi.valid_to")) &
            (F.col("health_insurance") == F.col("hi.health_insurance_code")),
            how="left"
        )
        .join(
            F.broadcast(dim_healthcare_service_df.alias("hcs")),
            (F.col("year") >= F.col("hcs.valid_from")) & 
            (F.col("year") <= F.col("hcs.valid_to")) &
            (F.col("healthcare_service") == F.col("hcs.healthcare_service_cut")),
            how="left"
        )
        .select(
            F.col("year"),
            F.col("hi.health_insurance_id").cast(IntegerType()),
            F.col("year_id").cast(IntegerType()),
            F.coalesce(F.col("id"), F.lit(999999)).cast(IntegerType()).alias("gender_id"),
            F.coalesce(F.col("age_id"), F.lit(999999)).cast(IntegerType()).alias("age_id"),
            F.col("city"),
            F.col("hcs.healthcare_service_id").cast(IntegerType()),
            F.col("hcs.healthcare_service_cut"),
            F.col("cause_of_death_1"),
            F.col("cause_of_death_2")
        )
    )

    # Cause of death recoding and lookup
    discharges_df = (
        discharges_df.alias("e")
        .join(
            F.broadcast(cod_recod_df.alias("cod_1")),
            F.translate(F.col("e.cause_of_death_1"), ",.", "") == F.col("cod_1.code"),
            how="left"
        )
        .join(
            F.broadcast(cod_recod_df.alias("cod_2")),
            F.translate(F.col("e.cause_of_death_2"), ",.", "") == F.col("cod_2.code"),
            how="left"
        )
        .select(
            "*",
            F.when(
                F.coalesce(
                    F.translate(F.col("cod_1.new_code"), ",.", ""),
                    F.translate(F.col("e.cause_of_death_1"), ",.", "")
                ).rlike("^[VWXY]"), 
                F.lit("N/E")
            ).otherwise(
                F.coalesce(
                    F.translate(F.col("cod_1.new_code"), ",.", ""),
                    F.translate(F.col("e.cause_of_death_1"), ",.", "")
                )
            ).alias("cause_of_death_1_final"),
            F.coalesce(
                F.translate(F.col("cod_2.new_code"), ",.", ""),
                F.translate(F.col("e.cause_of_death_2"), ",.", "")
            ).alias("cause_of_death_2_final")
        )
        .alias("e")
        .join(
            F.broadcast(dim_cause_of_death_df.alias("c1")),
            F.col("e.cause_of_death_1_final") == F.col("c1.subcategory_code"),
            how="left"
        )
        .join(
            F.broadcast(dim_cause_of_death_df.alias("c2")),
            F.col("e.cause_of_death_2_final") == F.col("c2.subcategory_code"),
            how="left"
        )
        .select(
            F.col("e.year"),
            F.col("e.health_insurance_id"),
            F.col("e.year_id"),
            F.col("e.gender_id"),
            F.col("e.age_id"),
            F.col("e.city"),
            F.col("e.healthcare_service_id"),
            F.coalesce(F.col("c1.id"), F.lit(999999)).cast(IntegerType()).alias("main_cause_of_death_id"),
            F.coalesce(F.col("c2.id"), F.lit(999999)).cast(IntegerType()).alias("external_cause_of_death_id")
        )
    )

    # City validation and lookup
    discharges_df = (
        discharges_df
        .join(
            F.broadcast(invalid_cuts_df),
            F.col("city") == F.col("CUT"),
            how="left"
        )
        .join(
            F.broadcast(dim_city_df),
            (F.col("year") >= F.col("valid_from")) & 
            (F.col("year") <= F.col("valid_to")) &
            (F.coalesce(F.when(F.col("CUT").isNotNull(), F.lit(999999)), F.col("city")) == F.col("city_cut")),
            how="left"
        )
        .select(
            F.col("health_insurance_id"),
            F.col("year_id"),
            F.col("gender_id"),
            F.col("age_id"),
            F.col("city_id").cast(IntegerType()),
            F.col("healthcare_service_id"),
            F.col("main_cause_of_death_id"),
            F.col("external_cause_of_death_id")
        )
    )

    discharges_df.write.mode("overwrite").saveAsTable("lab.biostatistics.discharges_gold")

if __name__ == "__main__":
    process()