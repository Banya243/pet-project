"""Minimal S3/JDBC check; the orders aggregation exercise is left to the learner."""

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("lab-environment-check").getOrCreate()
    try:
        orders = (
            spark.read.option("header", True)
            .schema(
                "order_id INT, user_id INT, order_date DATE, "
                "amount DECIMAL(12,2), status STRING"
            )
            .csv("s3a://raw/orders/2026-10-26/orders.csv")
        )
        result = spark.createDataFrame(
            [(spark.version, orders.count())],
            "spark_version STRING, input_rows LONG",
        )
        result.write.mode("overwrite").parquet("s3a://processed/environment-check")
        result.write.format("jdbc").option(
            "url", "jdbc:postgresql://postgres:5432/warehouse"
        ).option("dbtable", "public.environment_check").option("user", "lab").option(
            "password", "lab"
        ).option("driver", "org.postgresql.Driver").mode("overwrite").save()
        result.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
