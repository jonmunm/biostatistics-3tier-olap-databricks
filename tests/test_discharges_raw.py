import sys
import argparse
import great_expectations as gx
from pyspark.sql import SparkSession

def validate_raw_discharges(input_source: str, skip_validation: str = "false"):
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Raw validation step as requested.")
        return None
  
    context = gx.get_context() 
    spark = SparkSession.getActiveSession()
    
    if input_source.startswith("/Volumes/"):
        df = spark.read.parquet(input_source)
    else:
        df = spark.table(input_source)

    datasource = context.data_sources.add_or_update_spark(
        name="spark_datasource",
        persist=False
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

    assert results.success, "❌ Suite failed: The data did not pass the quality checks."
    print("✅ Success! All checks passed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DQ for hospital discharges")
    parser.add_argument("--input-source", default="lab.biostatistics.discharges_raw_dlt")
    parser.add_argument("--skip-validation", default="false")
    args = parser.parse_args()

    try:
        validate_raw_discharges(input_source=args.input_source, skip_validation=args.skip_validation)
    except AssertionError as e:
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)