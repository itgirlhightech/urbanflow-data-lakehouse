from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_PATH = PROJECT_ROOT / "lakehouse" / "bronze"

INPUT_FILE = RAW_DATA_PATH / "yellow_tripdata_2026-07.parquet"
OUTPUT_PATH = BRONZE_PATH / "yellow_taxi"


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("UrbanFlow-Bronze-Ingestion")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {INPUT_FILE}")

    spark = create_spark_session()

    try:
        print("Lendo arquivo Parquet...")
        df = spark.read.parquet(str(INPUT_FILE))

        print("Esquema dos dados:")
        df.printSchema()

        print("Gravando dados na camada Bronze...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(str(OUTPUT_PATH))
        )

        print(f"Dados gravados em: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
