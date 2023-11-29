from models.spark_df import conf_spark, calc_median

spark, schema = conf_spark()


def are_dfs_equal(df1, df2):
    return (df1.schema == df2.schema) and (df1.collect() == df2.collect())


def test_median_01():
    input_df = spark.createDataFrame(
        [
            ("2020-01-01", "input_01", 1.5),
            ("2020-01-01", "input_01", 17.0),
            ("2020-01-01", "input_01", 7.0),
            ("2020-01-01", "input_01", 3.0),
            ("2020-01-01", "input_01", 4.0),
            ("2020-01-01", "input_01", 5.0),
        ],
        "date string, input string, value double",
    )
    result_df = spark.createDataFrame(
        [
            ("2020-01-01", "input_01", 4.5),
        ],
        "date string, input string, median_value double",
    )
    script_df = calc_median(input_df)
    assert are_dfs_equal(script_df, result_df) == True


def test_median_02():
    input_df = spark.createDataFrame(
        [
            ("2020-01-01", "input_01", 1.5),
            ("2020-01-01", "input_01", 17.0),
            ("2020-01-01", "input_01", 7.0),
            ("2020-01-01", "input_01", 8.8),
            ("2020-01-02", "input_01", 5.5),
            ("2020-01-02", "input_01", 3.3),
            ("2020-01-02", "input_01", 4.4),
        ],
        "date string, input string, value double",
    )
    result_df = spark.createDataFrame(
        [
            ("2020-01-01", "input_01", 7.9),
            ("2020-01-02", "input_01", 4.4),
        ],
        "date string, input string, median_value double",
    )
    script_df = calc_median(input_df)
    assert are_dfs_equal(script_df, result_df) == True
