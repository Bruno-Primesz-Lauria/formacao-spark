from pyspark.sql import SparkSession


def main() -> None:
    # Create a local Spark session and run a tiny job
    spark = (
        SparkSession.builder
        .appName("venv-pyspark-test")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        count = spark.range(1, 10).count()
        print(count)  # expected: 9
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

