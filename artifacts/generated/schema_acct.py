"""COPYBOOK ACCTREC.cpy -> PySpark StructType."""
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType, DoubleType, DecimalType)

ACCT_SCHEMA = StructType([
    StructField("acct_no", StringType()  # X(10), True),
    StructField("acct_name", StringType()  # X(30), True),
    StructField("acct_type", StringType()  # X(02), True),
    StructField("acct_balance", DecimalType(13, 2)  # S9(11)V99, True),
    StructField("acct_status", StringType()  # X(01), True),
    StructField("acct_last_date", IntegerType()  # 9(08), True),
])
