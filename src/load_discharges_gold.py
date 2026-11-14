import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from lib.storage_utils import download_dimension, set_storage_config

def process(storage_account: str, storage_container: str, silver_table_fqdn: str, gold_table_fqdn: str, skip_validation: str = "false") -> DataFrame:
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Gold transformation step as requested.")
        return None

    set_storage_config(storage_account, storage_container)

    dim_gender_df = download_dimension("dim_gender").select(
        F.col("id").cast(IntegerType()).alias("id")
    )

    dim_age_df = download_dimension("dim_age").select(
        (F.col("age_id").cast(IntegerType()) + 1).alias("real_age"),
        F.col("id").cast(IntegerType()).alias("age_id")
    )

    dim_year_df = download_dimension("dim_year").select(
        (F.col("id").cast(IntegerType()) + 1989).alias("real_year"),
        F.col("id").cast(IntegerType()).alias("year_id")
    )

    dim_health_insurance_df = download_dimension("dim_health_insurance").select(
        F.col("id").cast(IntegerType()).alias("health_insurance_id"),
        F.col("health_insurance_code").cast(IntegerType()).alias("health_insurance_code"),
        F.col("valid_from").cast(IntegerType()).alias("valid_from"),
        F.col("valid_to").cast(IntegerType()).alias("valid_to")
    )

    dim_healthcare_service_df = download_dimension("dim_healthcare_service").select(
        F.col("id").cast(IntegerType()).alias("healthcare_service_id"),
        F.col("healthcare_service_cut").cast(IntegerType()).alias("healthcare_service_cut"),
        F.col("valid_from").cast(IntegerType()).alias("valid_from"),
        F.col("valid_to").cast(IntegerType()).alias("valid_to")
    )

    dim_city_df = download_dimension("dim_city").select(
        F.col("id").cast(IntegerType()).alias("city_id"),
        F.col("city_cut").cast(IntegerType()).alias("city_cut"),
        F.col("valid_from").cast(IntegerType()).alias("valid_from"),
        F.col("valid_to").cast(IntegerType()).alias("valid_to")
    )

    cod_recod_df = download_dimension("cause_of_death.recoded.lookup")

    dim_cause_of_death_df = download_dimension("dim_cause_of_death").select(
        F.col("id").cast(IntegerType()).alias("id"),
        F.col("subcategory_code")
    )

    invalid_cuts_df = download_dimension("city.invalid_cuts.lookup").select(
        F.col("CUT").cast(IntegerType()).alias("CUT")
    )

    spark = SparkSession.getActiveSession()
    discharges_df = (
        spark.read.table(silver_table_fqdn)
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

    discharges_df.write.mode("overwrite").saveAsTable(gold_table_fqdn)
    return discharges_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Transformation for hospital discharges")
    parser.add_argument("--storage-account", default="storagesa01")
    parser.add_argument("--storage-container", default="biostatistics")
    parser.add_argument("--silver-table-fqdn", default="lab.biostatistics.discharges_silver_dlt")
    parser.add_argument("--gold-table-fqdn", default="lab.biostatistics.discharges_gold_dlt")
    parser.add_argument("--skip-validation", default="false")
    args = parser.parse_args()

    try:
        process(
            storage_account=args.storage_account,
            storage_container=args.storage_container,
            silver_table_fqdn=args.silver_table_fqdn,
            gold_table_fqdn=args.gold_table_fqdn,
            skip_validation=args.skip_validation
        )
        print("✅ Success! Gold transformation completed.")
    except Exception as e:
        print(f"Unexpected error in the Gold transformation pipeline: {str(e)}")
        sys.exit(1)