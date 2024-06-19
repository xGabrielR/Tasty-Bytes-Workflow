from pytz import timezone
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as pf
from pyspark.sql.dataframe import DataFrame

from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)


def transform_truck_datafarame(
    df_truck: DataFrame
) -> DataFrame:
    df_truck = df_truck.select("truck_id", "primary_city", "region", "year", "make")
    df_truck = df_truck.withColumn("make", pf.regexp_replace(pf.col("make"), "_", ""))
    df_truck = df_truck.withColumn(
        "truck_class",
        pf.when(df_truck.year < 1990, "bronze").when(
            (df_truck.year > 1990) & (df_truck.year <= 2005), "silver"
        ).when(
            df_truck.year > 2005, "gold"
        ).otherwise("unclass")
    ).drop("year")

    return df_truck.select(
        pf.col("truck_id"),
        pf.col("primary_city"),
        pf.col("region").alias("truck_region"),
        pf.col("make").alias("truck_make"),
        pf.col("truck_class")
    )

def transform_customer_dataframe(
    df_customer: DataFrame
) -> DataFrame:
    df_customer = df_customer.select(
        "customer_id", "first_name", "last_name", "gender", "e_mail", "phone_number",
        "birthday_date", "sign_up_date", "favourite_brand", "postal_code", "country"
    ).distinct()

    df_customer = df_customer.withColumn(
        "name", pf.concat_ws(" ", pf.col("first_name"), pf.col("last_name"))
    ).drop("first_name").drop("last_name")

    return df_customer.select(
        pf.col("customer_id"),
        pf.col("name").alias("customer_full_name"),
        pf.col("gender"),
        pf.col("e_mail"),
        pf.col("phone_number"),
        pf.col("birthday_date"),
        pf.col("sign_up_date").alias("customer_sign_date"),
        pf.col("favourite_brand").alias("customer_favorite_brand"),
        pf.col("postal_code").alias("customer_postal_code"),
        pf.col("country").alias("customer_country")
    )
    
def transform_location_datafarame(
    df_location: DataFrame
) -> DataFrame:
    df_location = df_location.select("location_id", "location", "city", "country").distinct()
    df_location = df_location.withColumn("location", pf.regexp_replace(pf.col("location"), r"\s+", " "))

    return df_location.select(
        pf.col("location_id"),
        pf.col("location"),
        pf.col("city").alias("location_city"),
        pf.col("country").alias("location_country")
    )

def transform_order_header_datafarame(
    df_order_header: DataFrame
) -> DataFrame:
    df_order_header = df_order_header.select(
        "order_id", "truck_id",
        "location_id", "customer_id",
        "order_ts", "order_total"
    ).filter(
        pf.col("customer_id").isNotNull()
    )

    return df_order_header.select(
        pf.col("order_id"),
        pf.col("truck_id"),
        pf.col("location_id"),
        pf.col("customer_id"),
        pf.to_timestamp(pf.col("order_ts")).alias("order_date"),
        pf.col("order_total")
    )

if __name__ == "__main__":
    # Load Datafarames, predicate pushdowns and Business Logic.
    df_truck = spark.read.format("parquet").load("s3a://grc-raw/tt/truck/")
    df_truck = transform_truck_datafarame(df_truck=df_truck)
    df_truck_b = pf.broadcast(df_truck)

    df_location = spark.read.format("parquet").load("s3a://grc-raw/tt/location/")
    df_location = transform_location_datafarame(df_location=df_location)

    df_customer = spark.read.format("parquet").load("s3a://grc-raw/tt/customer/")
    df_customer = transform_customer_dataframe(df_customer=df_customer)

    df_order_header = spark.read.format("parquet").load("s3a://grc-raw/tt/order_header/")
    df_order_header = transform_order_header_datafarame(df_order_header=df_order_header)
    
    # Broadcast Distinct Orders only with Available Customers Only
    df_order_header_orders = df_order_header.select("order_id").distinct()
    df_order_header_orders_b = pf.broadcast(df_order_header_orders)

    # Load Order Details DataFrame, filter columns and Inner Join for filtering available Customers
    df_order_detail = spark.read.format("parquet").load("s3a://grc-raw/tt/order_detail/")
    df_order_detail = df_order_detail.select("order_id", "menu_item_id", "quantity", "unit_price")
    df_order_detail = df_order_detail.join(df_order_header_orders_b, how="inner", on="order_id")

    # Calculate Metrics
    df_order_detail = df_order_detail.groupBy("order_id").agg(
        pf.sum("quantity").alias("total_quantity"),
        pf.avg("quantity").alias("avg_quantity"),
        pf.avg("unit_price").alias("avg_unit_price"),
        pf.count_distinct("menu_item_id").alias("total_quantity_unique_items") 
    )

    df_view = df_order_detail.join(
        df_order_header, how="inner", on="order_id"
    ).join(
        df_truck_b, how="left", on="truck_id"
    ).join(
        df_location, how="left", on="location_id"
    ).join(
        df_customer, how="left", on="customer_id"
    )

    df_view.printSchema()

    df_view.coalesce(1).write.format("parquet").save("s3a://grc-curated/tt/order_view/")
