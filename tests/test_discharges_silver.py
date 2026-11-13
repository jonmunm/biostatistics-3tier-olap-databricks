import sys
import argparse
import great_expectations as gx
from pyspark.sql import SparkSession

def validate_silver_discharges(input_source: str, skip_validation: str = "false"):
    """
    Initializes an ephemeral Great Expectations context in Databricks,
    loads the silver tier discharge dataset (from Unity Catalog or Volumes),
    executes comprehensive data quality validations (nulls, types, ranges, regex),
    and handles errors by raising exceptions to fail the Databricks Job.
    """
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Silver validation step as requested.")
        return None
        
    # 1. Retrieve the active Spark session in Databricks
    spark = SparkSession.builder.getOrCreate()
    
    # 2. Use an ephemeral (in-memory) context. Prevents disk errors on the cluster.
    context = gx.get_context() 
    
    # 3. Read the data.
    if input_source.startswith("/Volumes/"):
        df = spark.read.parquet(input_source)
    else:
        df = spark.table(input_source)

    # 4. Create the Datasource with persist=False to fix Serverless Compatibility
    datasource = context.data_sources.add_or_update_spark(
        name="spark_datasource",
        persist=False
    )
    
    try:
        asset = datasource.add_dataframe_asset(name="discharges_silver_asset")
    except Exception:
        asset = datasource.get_asset("discharges_silver_asset")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name="discharges_silver_quality_suite"))

    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )

    # Schema
    columnas = ["year", "gender", "age", "health_insurance", "city", "healthcare_service", "cause_of_death_1", "cause_of_death_2"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    # Nulls
    validator.expect_column_values_to_not_be_null("year")
    validator.expect_column_values_to_not_be_null("gender")
    validator.expect_column_values_to_not_be_null("age")
    validator.expect_column_values_to_not_be_null("health_insurance")
    validator.expect_column_values_to_not_be_null("city")
    validator.expect_column_values_to_not_be_null("healthcare_service")
    validator.expect_column_values_to_not_be_null("cause_of_death_1")
    validator.expect_column_values_to_not_be_null("cause_of_death_2")    

    # Types
    validator.expect_column_values_to_be_of_type("year", "IntegerType")
    validator.expect_column_values_to_be_of_type("gender", "IntegerType")
    validator.expect_column_values_to_be_of_type("age", "IntegerType")
    validator.expect_column_values_to_be_of_type("health_insurance", "IntegerType")
    validator.expect_column_values_to_be_of_type("city", "IntegerType")
    validator.expect_column_values_to_be_of_type("healthcare_service", "IntegerType")
    validator.expect_column_values_to_be_of_type("cause_of_death_1", "StringType")
    validator.expect_column_values_to_be_of_type("cause_of_death_2", "StringType")    

    # Value Ranges
    validator.expect_column_values_to_be_between("year", min_value=2001, max_value=2018)
    validator.expect_column_values_to_be_in_set("gender", [1, 2, 3, 9])
    validator.expect_column_values_to_be_between("age", min_value=0, max_value=130)
    validator.expect_column_values_to_be_between("health_insurance", min_value=0, max_value=99)
    validator.expect_column_values_to_be_between("city", min_value=0, max_value=999999)
    validator.expect_column_values_to_be_between("healthcare_service", min_value=0, max_value=999999)

    # Regex for causes of death (ICD-10 patterns)
    partes = [
        r"[A-Z][0-9]{3}", # Ejemplo: A123
        r"[A-Z][0-9]{2}X", # Ejemplo: A12X
        r"[A-Z][0-9]{2,4}", # Ejemplo: A12, A1234
        r"[A-Z][0-9]{3}\+", # Ejemplo: A123+
        r"N/E"
    ]

    regex = r"^(" + "|".join(partes) + r")$"
    validator.expect_column_values_to_match_regex("cause_of_death_1", regex=regex, mostly=0.99)
    validator.expect_column_values_to_match_regex("cause_of_death_2", regex=regex, mostly=0.74)

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

    assert results.success, "❌ Suite failed: The Silver tier data did not pass the quality checks."
    
    print("✅ Success! All Silver checks passed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DQ for Silver hospital discharges")
    parser.add_argument("--input-source", required=True, help="UC table name or Volume path")
    parser.add_argument("--skip-validation", default="false", help="Set to true to skip this step")
    args = parser.parse_args()

    try:
        validate_silver_discharges(args.input_source, args.skip_validation)
    except AssertionError as e:
        print(str(e))
        sys.exit(1) # Explicitly fails the Databricks Job
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)