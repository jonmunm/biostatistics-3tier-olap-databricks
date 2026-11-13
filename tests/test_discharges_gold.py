"""
Great Expectations data quality module for validating gold tier health discharge fact tables,
including referential integrity checks against dimension IDs, schema matching, null checks, and data types.
"""

import sys
import argparse
import great_expectations as gx
from pyspark.sql import SparkSession
from pathlib import Path

CWD = Path().resolve()
PARENT = CWD.parent

# Add both to sys.path so Python can find the "src" folder without issues
for path_dir in [str(CWD), str(PARENT)]:
    if path_dir not in sys.path:
        sys.path.insert(0, path_dir)

from src.lib.storage_utils import download_dimension

from src.lib.storage_utils import download_dimension

def get_ids(dim_name: str) -> list[int]:
    """
    Reads a dimension CSV file from Azure Blob Storage using the utility module
    and extracts a distinct list of integer IDs to be used for referential integrity validation sets.
    """
    dim_df = download_dimension(dim_name)
    return [row[0] for row in 
                dim_df.select("id")
                      .distinct()
                      .collect()
            ]

def validate_gold_discharges(input_source: str, skip_validation: str = "false"):
    """
    Initializes an ephemeral Great Expectations context in Databricks,
    loads the gold tier fact dataset and reference dimensions,
    executes referential and schema quality checks, and handles errors.
    """
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Gold validation step as requested.")
        return None

    # 1. Retrieve the active Spark session in Databricks
    spark = SparkSession.builder.getOrCreate()
    
    # 2. Use an ephemeral (in-memory) context
    context = gx.get_context() 
    
    # 3. Read the Fact Table Data
    if input_source.startswith("/Volumes/"):
        df = spark.read.parquet(input_source)
    else:
        df = spark.table(input_source)

    # 4. Load Reference Dimension IDs
    year_ids = get_ids("dim_year")
    age_ids = get_ids("dim_age")
    hi_ids = get_ids("dim_health_insurance")
    city_ids = get_ids("dim_city")
    hcs_ids = get_ids("dim_healthcare_service")
    cd_ids = get_ids("dim_cause_of_death")

    # 5. Create the Datasource with persist=False to fix Serverless Compatibility
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

    # Schema Checks
    columnas = ["health_insurance_id", "year_id", "gender_id", "age_id", "city_id", "healthcare_service_id", "main_cause_of_death_id", "external_cause_of_death_id"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    # Null and Type Checks
    for col in columnas:
        validator.expect_column_values_to_not_be_null(col)
        validator.expect_column_values_to_be_of_type(col, "IntegerType")

    # Referential Integrity Checks (Foreign Keys)
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
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DQ for Gold hospital discharges")
    parser.add_argument("--input-source", required=True, help="UC table name or Volume path")
    parser.add_argument("--skip-validation", default="false", help="Set to true to skip this step")
    args = parser.parse_args()

    try:
        validate_gold_discharges(args.input_source, args.skip_validation)
    except AssertionError as e:
        print(str(e))
        sys.exit(1) # Explicitly fails the Databricks Job
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)