import sys
import argparse
import great_expectations as gx
from pathlib import Path
from pyspark.sql import SparkSession

CWD = Path().resolve()
PARENT = CWD.parent

for path_dir in [str(CWD), str(PARENT)]:
    if path_dir not in sys.path:
        sys.path.insert(0, path_dir)

from src.lib.storage_utils import download_dimension, set_storage_config

def get_ids(dim_name: str) -> list[int]:
    dim_df = download_dimension(dim_name)
    return [row[0] for row in 
                dim_df.select("id")
                      .distinct()
                      .collect()
            ]

def validate_gold_discharges(input_source: str, storage_account: str, storage_container: str, skip_validation: str = "false"):
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Gold validation step as requested.")
        return None
   
    set_storage_config(storage_account, storage_container)
    context = gx.get_context() 
    spark = SparkSession.getActiveSession()
    
    if input_source.startswith("/Volumes/"):
        df = spark.read.parquet(input_source)
    else:
        df = spark.table(input_source)

    year_ids = get_ids("dim_year")
    age_ids = get_ids("dim_age")
    hi_ids = get_ids("dim_health_insurance")
    city_ids = get_ids("dim_city")
    hcs_ids = get_ids("dim_healthcare_service")
    cd_ids = get_ids("dim_cause_of_death")

    datasource = context.data_sources.add_or_update_spark(
        name="spark_datasource",
        persist=False
    )
    
    try:
        asset = datasource.add_dataframe_asset(name="discharges_gold_asset")
    except Exception:
        asset = datasource.get_asset("discharges_gold_asset")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name="discharges_gold_quality_suite"))
    
    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )

    columnas = ["health_insurance_id", "year_id", "gender_id", "age_id", "city_id", "healthcare_service_id", "main_cause_of_death_id", "external_cause_of_death_id"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    for col in columnas:
        validator.expect_column_values_to_not_be_null(col)
        validator.expect_column_values_to_be_of_type(col, "IntegerType")

    validator.expect_column_values_to_be_in_set("year_id", value_set=year_ids)
    validator.expect_column_values_to_be_in_set("gender_id", value_set=[1, 2, 999999])
    validator.expect_column_values_to_be_in_set("age_id", value_set=age_ids)
    validator.expect_column_values_to_be_in_set("health_insurance_id", value_set=hi_ids)
    validator.expect_column_values_to_be_in_set("city_id", value_set=city_ids)
    validator.expect_column_values_to_be_in_set("healthcare_service_id", value_set=hcs_ids)
    validator.expect_column_values_to_be_in_set("main_cause_of_death_id", value_set=cd_ids)
    validator.expect_column_values_to_be_in_set("external_cause_of_death_id", value_set=cd_ids)

    results = validator.validate()
    
    if not results.success:
        for result in results.results:
            if not result.success:
                kwargs = result.expectation_config.kwargs
                column = kwargs.get("column", "N/A (Table Level)")
                print(f"::: Column: {column}")
                print(f"--> Expectation {result.expectation_config.type}")
                print(result.result)
                print("\n")

    assert results.success, "❌ Suite failed: The Gold tier data did not pass the referential integrity checks."
    print("✅ Success! All Gold checks passed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DQ for Gold hospital discharges")
    parser.add_argument("--input-source", default="lab.biostatistics.discharges_gold_dlt")
    parser.add_argument("--storage-account", default="storagesa01")
    parser.add_argument("--storage-container", default="biostatistics")
    parser.add_argument("--skip-validation", default="false")
    args = parser.parse_args()

    try:
        validate_gold_discharges(
            input_source=args.input_source,
            storage_account=args.storage_account,
            storage_container=args.storage_container,
            skip_validation=args.skip_validation
        )
    except AssertionError as e:
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)