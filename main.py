from models.spark_df import conf_spark, read_jsonl, calc_median, write_json


def main():
    spark = conf_spark()
    df = read_jsonl(spark=spark, file_name="data.jsonl")
    df_median = calc_median(df)
    write_json(df_median)

if __name__ == "__main__":
    main()
