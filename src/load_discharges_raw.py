import argparse
import sys
from functools import reduce
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pathlib import Path

CURRENT_DIR = str(Path().resolve())
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from lib import discharges_raw_loader as raw_loader
from lib.storage_utils import set_storage_config

def process(storage_account: str, storage_container: str, raw_table_fqdn: str, skip_validation: str = "false") -> DataFrame:
    if skip_validation.lower() == "true":
        print("⏭️ Skipping Raw Loading step as requested.")
        return None

    # Configuramos el storage en memoria para evitar restricciones de Spark Connect
    set_storage_config(storage_account, storage_container)

    group_1_df = raw_loader.load_group_1().select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_2_df = raw_loader.load_group_2().select(
        F.col("FECHA_EGR").alias("date"),
        F.col("SER_SALUD").alias("healthcare_service"),
        F.col("EST").alias("facility"),
        F.col("SEXO").alias("gender"),
        F.col("EDAD").alias("age"),
        F.col("COMUNA").alias("city"),
        F.col("DIAG1").alias("cause_of_death_1"),
        F.col("DIAG2").alias("cause_of_death_2"),
        F.col("PREVI").alias("health_insurance"),
    )

    group_3_df = raw_loader.load_group_3().select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_4_df = raw_loader.load_group_4().select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_5_df = raw_loader.load_group_5().select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_6_df = raw_loader.load_group_6().select(
        F.col("FECHA_EGR").alias("date"),
        F.col("SER_SALUD").alias("healthcare_service"),
        F.col("ESTAB").alias("facility"),
        F.col("SEXO").alias("gender"),
        F.col("EDAD").alias("age"),
        F.col("COMUNA").alias("city"),
        F.col("DIAG1").alias("cause_of_death_1"),
        F.col("DIAG2").alias("cause_of_death_2"),
        F.col("PREVI").alias("health_insurance"),
    )

    group_7_df = raw_loader.load_group_7().select(
        F.col("FECHA_EGR").alias("date"),
        F.col("ServicioSalud").alias("healthcare_service"),
        F.col("ESTAB").alias("facility"),
        F.col("SEXO").alias("gender"),
        F.col("EDAD").alias("age"),
        F.col("COMUNA").alias("city"),
        F.col("DIAG1").alias("cause_of_death_1"),
        F.col("DIAG2").alias("cause_of_death_2"),
        F.col("PREVI").alias("health_insurance"),
    )

    dfs = [
        group_1_df, group_2_df, group_3_df, group_4_df, 
        group_5_df, group_6_df, group_7_df
    ]

    complete_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
    complete_df.write.mode("overwrite").saveAsTable(raw_table_fqdn)
    return complete_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raw Loading for hospital discharges")
    parser.add_argument("--storage-account", default="storagesa01")
    parser.add_argument("--storage-container", default="biostatistics")
    parser.add_argument("--raw-table-fqdn", default="lab.biostatistics.discharges_raw_dlt")
    parser.add_argument("--skip-validation", default="false")
    args = parser.parse_args()

    try:
        process(
            storage_account=args.storage_account,
            storage_container=args.storage_container,
            raw_table_fqdn=args.raw_table_fqdn,
            skip_validation=args.skip_validation
        )
        print("✅ Success! Raw loading completed.")
    except Exception as e:
        print(f"Unexpected error in the pipeline: {str(e)}")
        sys.exit(1)