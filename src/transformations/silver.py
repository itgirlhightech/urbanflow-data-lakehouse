from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "lakehouse" / "bronze" / "yellow_taxi"
SILVER_PATH = PROJECT_ROOT / "lakehouse" / "silver" / "yellow_taxi"

def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("UrbanFlow-Silver-Transformation")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()

#Transformação da camada Silver


def main():
    spark = create_spark_session()

    try:
        # 1. Leitura da Bronze
        df = spark.read.format("delta").load(str(BRONZE_PATH))

        # 2. Cálculo da duração da viagem
        df_silver = df.withColumn(
            "duracao_segundos",
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )

        # 3. Identificação da população de mobilidade
        criterio_mobilidade = (
            (F.col("trip_distance") > 0) &
            (F.col("trip_distance") <= 19.64) &
            (F.col("duracao_segundos") > 0) &
            (F.col("duracao_segundos") <= 3730) &
            (F.col("passenger_count") >= 1)
        )

        df_silver = df_silver.withColumn(
            "is_mobility_valid",
            F.coalesce(criterio_mobilidade, F.lit(False))
        )

        # 4. População principal de mobilidade
        df_mobilidade_final = df_silver.filter(
            F.col("is_mobility_valid") == True
        )

        print("\nPopulação principal de mobilidade:")
        print(f"Total de registros: {df_mobilidade_final.count()}")

        # 5. Escrita da Silver
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .save(str(SILVER_PATH))

        # 6. Validação da camada Silver
        df_silver_check = spark.read.format("delta").load(str(SILVER_PATH))

        resultado = df_silver_check.agg(
            F.count("*").alias("total_registros"),
            F.sum(
                F.when(F.col("is_mobility_valid"), 1).otherwise(0)
            ).alias("registros_validos"),
            F.sum(
                F.when(~F.col("is_mobility_valid"), 1).otherwise(0)
            ).alias("registros_invalidos")
        ).first()

        print("\nValidação da camada Silver:")
        print(f"Total de registros: {resultado['total_registros']}")
        print(f"Registros válidos: {resultado['registros_validos']}")
        print(f"Registros inválidos: {resultado['registros_invalidos']}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
