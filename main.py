import os
import sys
import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col, split


def build_spark_session() -> SparkSession:
    """
    Function sets basic configuration of spark object and relatives
    """

    # Windows-specific
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder.master("local[*]").appName("PySpark_de_ta_5").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    conf = SparkConf()
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    return spark


def calc_median(df: DataFrame) -> DataFrame:
    """
    Function calcs median value for supplied df grouped by day and input sensors
    """
    return (
        df.groupby("date", "input")
        .agg(f.median("value").alias("median_value"))
        .sort("date", split(df.input, "_").getItem(1).cast("integer"))
    )


def main():
    spark = build_spark_session()

    df = spark.read.json("data.jsonl")

    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")).drop("time")

    df_median = calc_median(df)

    df_median.write.mode("overwrite").json("result")


if __name__ == "__main__":
    main()
