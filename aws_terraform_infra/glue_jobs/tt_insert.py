from pytz import timezone
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as pf
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)

schema_order_detail = StructType([ \
    StructField("order_detail_id", IntegerType(), True), \
    StructField("order_id", IntegerType(), True), \
    StructField("menu_item_id", IntegerType(), True), \
    StructField("discount_id", IntegerType(), True), \
    StructField("line_number", IntegerType(), True), \
    StructField("quantity", IntegerType(), True), \
    StructField("unit_price", DoubleType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("order_item_discount_amount", DoubleType(), True)
])

schema_order_header = StructType([ \
    StructField("order_id", IntegerType(), True), \
    StructField("truck_id", IntegerType(), True), \
    StructField("location_id", IntegerType(), True), \
    StructField("customer_id", IntegerType(), True), \
    StructField("discount_id", IntegerType(), True), \
    StructField("shift_id", IntegerType(), True), \
    StructField("shift_start_time", StringType(), True), \
    StructField("shift_end_time", StringType(), True), \
    StructField("order_channel", StringType(), True), \
    StructField("order_ts", StringType(), True), \
    StructField("served_ts", StringType(), True), \
    StructField("order_currency", StringType(), True), \
    StructField("order_amount", DoubleType(), True), \
    StructField("order_tax_amount", StringType(), True), \
    StructField("order_discount_amount", StringType(), True), \
    StructField("order_total", DoubleType(), True), \
])

schema_truck = StructType([ \
    StructField("truck_id", IntegerType(), True), \
    StructField("menu_type_id", IntegerType(), True), \
    StructField("primary_city", StringType(), True), \
    StructField("region", StringType(), True), \
    StructField("iso_region", StringType(), True), \
    StructField("country", StringType(), True), \
    StructField("iso_country_code", StringType(), True), \
    StructField("franchise_flag", StringType(), True), \
    StructField("year", IntegerType(), True), \
    StructField("make", StringType(), True), \
    StructField("model", StringType(), True), \
    StructField("ev_flag", IntegerType(), True), \
    StructField("franchise_id", IntegerType(), True), \
    StructField("truck_opening_date", StringType(), True), \
])

schema_location = StructType([ \
    StructField("location_id", IntegerType(), True), \
    StructField("placekey", StringType(), True), \
    StructField("location", StringType(), True), \
    StructField("city", StringType(), True), \
    StructField("region", StringType(), True), \
    StructField("iso_country_code", StringType(), True), \
    StructField("country", StringType(), True)
])

schema_customer = StructType([ \
    StructField("customer_id", StringType(), True), \
    StructField("first_name", StringType(), True), \
    StructField("last_name", StringType(), True), \
    StructField("city", StringType(), True), \
    StructField("country", StringType(), True), \
    StructField("postal_code", StringType(), True), \
    StructField("preferred_language", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("favourite_brand", StringType(), True), \
    StructField("marital_status", StringType(), True), \
    StructField("children_count", StringType(), True), \
    StructField("sign_up_date", StringType(), True), \
    StructField("birthday_date", StringType(), True), \
    StructField("e_mail", StringType(), True), \
    StructField("phone_number", StringType(), True)
])

if __name__ == "__main__":
    tables = [
        "order_detail",
        "order_header",
        "truck",
        "location",
        "customer"
    ]

    schemas = [
        schema_order_detail,
        schema_order_header,
        schema_truck,
        schema_location,
        schema_customer
    ]
    
    for table, schema in zip(tables, schemas):
        print(f"{datetime.now(timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')} | {table}")

        url = f"s3://sfquickstarts/frostbyte_tastybytes/raw_pos/{table}/*.csv.gz"

        if table == "customer":
            url = "s3://sfquickstarts/frostbyte_tastybytes/raw_customer/customer_loyalty/*.csv.gz"

        df = spark.read.format("csv").schema(schema).load(url)
        print(f"Number of Partitions ({table}) - {df.rdd.getNumPartitions()}")

        df.write.format("parquet").mode("overwrite").save(f"s3a://grc-raw/tt/{table}")