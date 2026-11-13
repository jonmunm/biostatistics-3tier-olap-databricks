import sys
import argparse
import great_expectations as gx
from pyspark.sql import SparkSession

def validate_uniformed_discharges(input_source: str, skip_validation: str = "false"):
    """
    Initializes an ephemeral Great Expectations context in Databricks,
    loads the data (from Unity Catalog or Volumes), executes quality validations
    and handles errors by raising exceptions to fail the Databricks Job.
    """
    # 1. Retrieve the active Spark session in Databricks
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Raw validation step as requested.")
        return None

    spark = SparkSession.builder.getOrCreate()
    
    # 2. Use an ephemeral (in-memory) context. Prevents disk errors on the cluster.
    context = gx.get_context() 
    
    # 3. Read the data.
    if input_source.startswith("/Volumes/"):
        df = spark.read.parquet(input_source)
    else:
        df = spark.table(input_source)

    # 4. Create the Datasource with persist=False to fix Serverless Compatibility
    # This prevents the [CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC] error
    datasource = context.data_sources.add_or_update_spark(
        name="spark_datasource",
        persist=False  # <--- THE FIX IS HERE
    )
    
    try:
        asset = datasource.add_dataframe_asset(name="discharges_raw_uniformed_asset")
    except Exception:
        asset = datasource.get_asset("discharges_raw_uniformed_asset")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name="discharges_raw_uniformed_quality_suite"))

    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )

    # Quality Rules
    columnas = ["date", "healthcare_service", "facility", "gender", "age", "city", "cause_of_death_1", "cause_of_death_2", "health_insurance"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    validator.expect_column_values_to_be_between("age", min_value=0, max_value=130)
    validator.expect_column_values_to_be_in_set("gender", [1, 2, 3, 9])

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

    # If it fails, the assert interrupts the script and marks the Job task as "Failed"
    assert results.success, "❌ Suite failed: The data did not pass the quality checks."
    
    print("✅ Success! All checks passed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DQ for hospital discharges")
    parser.add_argument("--input-source", required=True, help="UC table name or Volume path")
    parser.add_argument("--skip-validation", default="false", help="Set to true to skip this step")
    args = parser.parse_args()

    try:
        # AQUÍ ESTABA EL ERROR: Le faltaba el segundo parámetro (args.skip_validation)
        validate_uniformed_discharges(args.input_source, args.skip_validation)
    except AssertionError as e:
        print(str(e))
        sys.exit(1) # Explicitly fails the Databricks Job
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)