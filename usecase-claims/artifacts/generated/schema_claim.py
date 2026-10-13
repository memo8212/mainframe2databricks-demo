"""COPYBOOKs -> PySpark StructTypes (claims use case)."""
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType, DoubleType, DecimalType)

CLAIM_SCHEMA = StructType([
    StructField("claim_id", StringType()  # X(10), True),
    StructField("member_id", StringType()  # X(08), True),
    StructField("service_code", StringType()  # X(03), True),
    StructField("claim_amount", DecimalType(13, 2)  # S9(9)V99, True),
    StructField("service_date", IntegerType()  # 9(08), True),
])

MEMBER_SCHEMA = StructType([
    StructField("member_id", StringType()  # X(08), True),
    StructField("member_name", StringType()  # X(30), True),
    StructField("plan_type", StringType()  # X(06), True),
    StructField("deductible_rem", DecimalType(13, 2)  # S9(7)V99, True),
    StructField("plan_year", IntegerType()  # 9(04), True),
])
