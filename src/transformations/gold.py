from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "lakehouse" / "silver" / "yellow_taxi"
GOLD_PATH = PROJECT_ROOT / "lakehouse" / "gold" / "yellow_taxi"
REFERENCE_PATH = PROJECT_ROOT / "data" / "reference" / "taxi_zone_lookup.csv"

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

        #5. Extraindo data para criar tabela "viagens_por_dia"
        df_gold = df_gold.withColumn(
            "data_viagem",
            F.to_date("tpep_pickup_datetime")
        )

        #6. Métricas por dia
        viagens_por_dia = (
            df_gold
            .groupBy("data_viagem")
            .count()
            .orderBy("data_viagem")
        )

        #6. Salvando a tabela "viagens_por_dia"
        viagens_por_dia.write.format("delta").mode("overwrite").save(
            str(GOLD_PATH / "viagens_por_dia")
        )

        #7. Criando a coluna de hora
        df_gold = df_gold.withColumn(
            "hora_viagem",
            F.hour("tpep_pickup_datetime")
        )

        viagens_por_hora = (
            df_gold
            .groupBy("hora_viagem")
            .count()
            .orderBy("hora_viagem")
        )

        viagens_por_hora.show(24)

        #8. Cruzando dia e hora
        demanda_dia_hora = (
            df_gold
            .withColumn("data_viagem", F.to_date("tpep_pickup_datetime"))
            .withColumn("hora_viagem", F.hour("tpep_pickup_datetime"))
            .groupBy("data_viagem", "hora_viagem")
            .count()
            .orderBy("data_viagem", "hora_viagem")
        )
        demanda_dia_hora.show(744)

        #9. Picos diários
        janela = Window.partitionBy("data_viagem")

        picos_diarios = (
            demanda_dia_hora
            .withColumn("maior_demanda", F.max("count").over(janela))
            .filter(F.col("count") == F.col("maior_demanda"))
            .select("data_viagem", "hora_viagem", "count")
            .orderBy("data_viagem")
        )

        picos_diarios.show(31)

        #10. Análise por dia da semana
        df_gold = df_gold.withColumn(
            "dia_semana",
            F.dayofweek("data_viagem")
        )

        viagens_por_dia_semana = (
            df_gold
            .groupBy("dia_semana")
            .count()
            .orderBy("dia_semana")
        )

        viagens_por_dia_semana.show(7)

        viagens_por_dia_semana = (
            df_gold
            .groupBy("dia_semana")
            .agg(
                F.count("*").alias("total_viagens"),
                F.countDistinct("data_viagem").alias("quantidade_dias")
            )
            .withColumn(
                "media_diaria",
                F.round(
                    F.col("total_viagens") / F.col("quantidade_dias"),
                    2
                )
            )
                .orderBy("dia_semana")
)

        #11. Cruzando dia da semana e hora
        demanda_semana_hora = (
            df_gold
            .groupBy("dia_semana", "hora_viagem")
            .count()
            .orderBy("dia_semana", "hora_viagem")
        )

        demanda_semana_hora.show(168)

        demanda_semana_hora.write.format("delta").mode("overwrite").save(
            str(GOLD_PATH / "demanda_semana_hora")
        )

        #12. Análise geográfica 
        print("Zonas de origem: ", df_gold.select("PULocationID").distinct().count())
        print("Zonas de destino: ",df_gold.select("DOLocationID").distinct().count())

        df_zones = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(REFERENCE_PATH))
        )

        df_zones.printSchema()

        print("Total de zonas: ", df_zones.count())

        df_zones.show(10, truncate=False)

        origens = (
            df_gold
            .groupBy("PULocationID")
            .agg(F.count("*").alias("total_viagens"))
        )

        viagens_por_origem = (
                origens.alias("o")
                .join(
                    df_zones.alias("z"),
                    F.col("o.PULocationID") == F.col("z.LocationID"),
                    "left"
            )
            .select(
                F.col("o.PULocationID"),
                F.col("z.Borough").alias("borough"),
                F.col("z.Zone").alias("zona"),
                F.col("z.service_zone"),
                F.col("o.total_viagens")
            )
            .orderBy(F.desc("total_viagens"))
        )

        viagens_por_origem.show(20, truncate=False)

        print(
        "Origens sem correspondência:",
        viagens_por_origem.filter(F.col("zona").isNull()).count()
    )

        destinos = (
        df_gold
        .groupBy("DOLocationID")
        .agg(F.count("*").alias("total_viagens"))
    )

        viagens_por_destino = (
            destinos.alias("d")
            .join(
                df_zones.alias("z"),
                F.col("d.DOLocationID") == F.col("z.LocationID"),
                "left"
            )
            .select(
                F.col("d.DOLocationID"),
                F.col("z.Borough").alias("borough"),
                F.col("z.Zone").alias("zona"),
                F.col("z.service_zone"),
                F.col("d.total_viagens")
            )
            .orderBy(F.desc("total_viagens"))
        )

        viagens_por_destino.show(20, truncate=False)
        print(
        "Destinos sem correspondência:",
        viagens_por_destino.filter(F.col("zona").isNull()).count()
    )

        #13. Cruzando origem e destino

        rotas = (
            df_gold
            .groupBy("PULocationID", "DOLocationID")
            .agg(F.count("*").alias("total_viagens"))
            .orderBy(F.desc("total_viagens"))
        )

        rotas_identificadas = (
            rotas.alias("r")
            .join(
                df_zones.alias("o"),
                F.col("r.PULocationID") == F.col("o.LocationID"),
                "left"
            )
            .join(
                df_zones.alias("d"),
                F.col("r.DOLocationID") == F.col("d.LocationID"),
                "left"
            )
            .select(
                F.col("r.PULocationID"),
                F.col("o.Zone").alias("zona_origem"),
                F.col("r.DOLocationID"),
                F.col("d.Zone").alias("zona_destino"),
                F.col("r.total_viagens")
            )
            .orderBy(F.desc("total_viagens"))
        )

        rotas_identificadas.show(20, truncate=False)

        rotas_identificadas.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "rotas_identificadas"))


    finally:
        spark.stop()

if __name__ == "__main__":
    main()