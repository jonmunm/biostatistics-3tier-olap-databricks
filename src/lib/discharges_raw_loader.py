"""
Spark module for loading raw health discharge datasets from cloud storage 
across various chronological schemas and year groupings.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from lib.storage_utils import download_raw_years
    

# 2001, 2003 -> "M/d/yyyy"
# 2002, 2004 -> "dd-M-yyyy"
# 2006 -> ""
def load_group_1() -> DataFrame:
    """
    Loads raw discharge data for Group 1 years (2001, 2002, 2003, 2004, 2006) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("D_R", IntegerType(), True),
        StructField("ser_salud", IntegerType(), True),
        StructField("estab", IntegerType(), True),
        StructField("sexo", IntegerType(), True),
        StructField("edad", IntegerType(), True),
        StructField("previ", IntegerType(), True),
        StructField("benef", IntegerType(), True),
        StructField("comuna", IntegerType(), True),
        StructField("fecha_egre", StringType(), True),
        StructField("sercle", StringType(), True),
        StructField("dias_estad", IntegerType(), True),
        StructField("diag1", StringType(), True),
        StructField("diag2", StringType(), True),
        StructField("cond_egr", IntegerType(), True),
        StructField("intervq", IntegerType(), True),
        StructField("region", IntegerType(), True),
        StructField("serv_res", StringType(), True),
    ])
    
    years = [2001, 2002, 2003, 2004, 2006]
    return download_raw_years(schema, years)


# 2005 -> "yyyy-MM-dd"
def load_group_2() -> DataFrame:
    """
    Loads raw discharge data for Group 2 years (2005) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("SER_SALUD", IntegerType(), True),
        StructField("EDAD", IntegerType(), True),
        StructField("SEXO", IntegerType(), True),
        StructField("COMUNA", IntegerType(), True),
        StructField("SER_RES", IntegerType(), True),
        StructField("REGION", IntegerType(), True),
        StructField("PREVI", IntegerType(), True),
        StructField("BENEF", IntegerType(), True),
        StructField("MOD", IntegerType(), True),
        StructField("SERC_EGR", StringType(), True),
        StructField("FECHA_EGR", StringType(), True),
        StructField("DIAS_ESTAD", IntegerType(), True),
        StructField("COND_EGR", IntegerType(), True),
        StructField("DIAG1", StringType(), True),
        StructField("DIAG2", StringType(), True),
        StructField("INTERV_Q", IntegerType(), True),
        StructField("EST", StringType(), True)
    ])

    years = [2005]
    return download_raw_years(schema, years)

# 2007, 2009, 2011, 2012
def load_group_3() -> DataFrame:
    """
    Loads raw discharge data for Group 3 years (2007, 2009, 2011, 2012) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("D_R", IntegerType(), True),
        StructField("ser_salud", IntegerType(), True),
        StructField("estab", IntegerType(), True),
        StructField("sexo", IntegerType(), True),
        StructField("edad", IntegerType(), True),
        StructField("previ", IntegerType(), True),
        StructField("benef", IntegerType(), True),
        StructField("mod", IntegerType(), True),
        StructField("comuna", IntegerType(), True),
        StructField("fecha_egre", StringType(), True),
        StructField("sercle", StringType(), True),
        StructField("dias_estad", IntegerType(), True),
        StructField("diag1", StringType(), True),
        StructField("diag2", StringType(), True),
        StructField("cond_egr", IntegerType(), True),
        StructField("intervq", IntegerType(), True),
        StructField("region", IntegerType(), True),
        StructField("serv_res", StringType(), True),
    ])

    years = [2007, 2009, 2011, 2012]
    return download_raw_years(schema, years)


# 2008
def load_group_4() -> DataFrame:
    """
    Loads raw discharge data for Group 4 years (2008) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("D_R", IntegerType(), True),
        StructField("ser_salud", IntegerType(), True),
        StructField("estab", IntegerType(), True),
        StructField("sexo", IntegerType(), True),
        StructField("edad", IntegerType(), True),
        StructField("previ", IntegerType(), True),
        StructField("benef", IntegerType(), True),
        StructField("mod", IntegerType(), True),
        StructField("comuna", IntegerType(), True),
        StructField("region", IntegerType(), True),
        StructField("serv_res", StringType(), True),
        StructField("fecha_egre", StringType(), True),
        StructField("sercle", StringType(), True),
        StructField("dias_estad", IntegerType(), True),
        StructField("diag1", StringType(), True),
        StructField("diag2", StringType(), True),
        StructField("cond_egr", IntegerType(), True),
        StructField("intervq", IntegerType(), True),
    ])

    years = [2008]
    return download_raw_years(schema, years)


# 2010
def load_group_5() -> DataFrame:
    """
    Loads raw discharge data for Group 5 years (2010) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("D_R", IntegerType(), True),
        StructField("estab", IntegerType(), True),
        StructField("ser_salud", IntegerType(), True),
        StructField("sexo", IntegerType(), True),
        StructField("edad", IntegerType(), True),
        StructField("previ", IntegerType(), True),
        StructField("benef", IntegerType(), True),
        StructField("mod", IntegerType(), True),
        StructField("comuna", IntegerType(), True),
        StructField("fecha_egre", StringType(), True),
        StructField("sercle", StringType(), True),
        StructField("dias_estad", IntegerType(), True),
        StructField("diag1", StringType(), True),
        StructField("diag2", StringType(), True),
        StructField("cond_egr", IntegerType(), True),
        StructField("intervq", IntegerType(), True),
        StructField("region", IntegerType(), True),
        StructField("serv_res", StringType(), True),
    ])

    years = [2010]
    return download_raw_years(schema, years)


# 2013
def load_group_6() -> DataFrame:
    """
    Loads raw discharge data for Group 6 years (2013) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("ESTAB", IntegerType(), True),
        StructField("SER_SALUD", IntegerType(), True),
        StructField("SEXO", IntegerType(), True),
        StructField("EDAD", IntegerType(), True),
        StructField("PREVI", IntegerType(), True),
        StructField("BENEF", IntegerType(), True),
        StructField("MOD", IntegerType(), True),
        StructField("COMUNA", IntegerType(), True),
        StructField("REGION", IntegerType(), True),
        StructField("SERV_RES", StringType(), True),
        StructField("FECHA_EGR", StringType(), True),
        StructField("SERC_EGR", StringType(), True),
        StructField("DIAG1", StringType(), True),
        StructField("DIAG2", StringType(), True),
        StructField("DIAS_ESTAD", IntegerType(), True),
        StructField("COND_EGR", IntegerType(), True),
        StructField("INTERV_Q", IntegerType(), True)
    ])

    years = [2013]
    return download_raw_years(schema, years)


# 2014, 2015, 2016, 2017, 2018
def load_group_7() -> DataFrame:
    """
    Loads raw discharge data for Group 7 years (2014, 2015, 2016, 2017, 2018) 
    using their specific schema definition.
    """
    schema = StructType([
        StructField("ESTAB", IntegerType(), True),
        StructField("Seremi", IntegerType(), True),
        StructField("ServicioSalud", IntegerType(), True),
        StructField("SEXO", IntegerType(), True),
        StructField("EDAD", IntegerType(), True),
        StructField("PREVI", IntegerType(), True),
        StructField("BENEF", IntegerType(), True),
        StructField("MOD", IntegerType(), True),
        StructField("COMUNA", IntegerType(), True),
        StructField("REGION", IntegerType(), True),
        StructField("SERV_RES", StringType(), True),
        StructField("FECHA_EGR", StringType(), True),
        StructField("SERC_EGR", StringType(), True),
        StructField("DIAG1", StringType(), True),
        StructField("DIAG2", StringType(), True),
        StructField("DIAS_ESTAD", IntegerType(), True),
        StructField("COND_EGR", IntegerType(), True),
        StructField("INTERV_Q", IntegerType(), True)
    ])

    years = [2014, 2015, 2016, 2017, 2018]
    return download_raw_years(schema, years)