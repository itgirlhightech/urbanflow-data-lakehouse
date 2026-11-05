from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "lakehouse" / "silver" / "yellow_taxi"
GOLD_PATH = PROJECT_ROOT / "lakehouse" / "gold" / "yellow_taxi"

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

def main():
    spark = create_spark_session()

    try: 
        #1. Leitura de Silver
        df_silver = (spark.read.format("delta").
                     load(str(SILVER_PATH))
                     )

        #2. Definição da população analítica de julho
        df_gold = df_silver.filter(
            (F.col("tpep_pickup_datetime") >= F.to_timestamp(
                F.lit("2026-07-01 00:00:00")
            )) &
            (F.col("tpep_pickup_datetime") < F.to_timestamp(
                F.lit("2026-08-01 00:00:00")
            )) &
            (F.col("is_mobility_valid") == True)
        )

        #3. Validação da população analítica
        print("\nPopulação analítica de julho de 2026:")
        print(f"Total de viagens: {df_gold.count()}")

        #4. Métricas gerais
        df_metrics = df_gold.agg(
            F.count("*").alias("total_viagens"),
            F.round(F.avg("trip_distance"), 2).alias("distancia_media"),
            F.round(F.avg("duracao_segundos"), 2).alias("duracao_media_segundos"),
            F.round(F.max("trip_distance"), 2).alias("distancia_maxima"),
            F.max("duracao_segundos").alias("duracao_maxima")
        )

        print("\nResumo geral de julho de 2026:")
        df_metrics.show(truncate=False)

        df_metrics.write.format("delta").mode("overwrite").save(
            str(GOLD_PATH / "resumo_geral")
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    main()