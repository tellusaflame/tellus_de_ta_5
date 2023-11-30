import os
import sys
import glob
import shutil
import pyspark.sql.functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import to_date, to_timestamp, col, concat, split


def conf_spark():
    """
    Function sets basic configuration of spark object and relatives
    :return: spark
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


def read_jsonl(spark, file_name: str):
    """
    Functions reads jsonl file, and organize it in ready to calc median view
    :param spark: spark
    :param file_name: target file, ex. "data.jsonl"
    :return: df
    """
    schema = StructType(
        [
            StructField("date", StringType()),
            StructField("input", StringType()),
            StructField("time", StringType()),
            StructField("value", DoubleType()),
        ]
    )

    df = spark.read.json(file_name, schema)
    df = (
        df.withColumn(
            "timestamp", concat(f.concat(f.col("date"), f.lit(" "), f.col("time")))
        )
        .withColumn("timestamp", to_timestamp("timestamp"))
        .drop("time")
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .sort("date", "input", ascending=True)
    )
    return df


def calc_median(df):
    """
    Function calcs median value for supplied df grouped by day and input sensors
    :param df:
    :return: dataframe with calculated median value
    """
    df = (
        df.groupby("date", "input")
        .agg(f.median("value").alias("median_value"))
        .sort("date", split(df.input, "_").getItem(1).cast("integer"))
    )
    return df


def write_json(df):
    """
    Function to write supplied df as "result.json"
    :param df: df
    """
    (df.coalesce(1).write.mode("overwrite").json("result"))

    for source_name in glob.glob("result/*.json"):
        path, fullname = os.path.split(source_name)
        basename, ext = os.path.splitext(fullname)
        target_name = os.path.join("./", "{}{}".format("result", ext))
        os.replace(source_name, target_name)
        shutil.rmtree("result", ignore_errors=True)
