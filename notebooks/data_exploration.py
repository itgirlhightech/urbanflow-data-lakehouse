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
#teste e investigação dos dados

def main():
    spark = create_spark_session()

    try:
        #1. Estrutura e qualidade geral
        print("Lendo dados da Bronze...")
        df = spark.read.format("delta").load(str(BRONZE_PATH))

        total = df.count()
        print(f"\nTotal de registros: {total}")

        print("\nEsquema dos dados:")
        df.printSchema()

        print("\nQuantidade de valores nulos por coluna:")
        df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in df.columns
        ]).show(truncate=False)

        print("\nExemplos de registros:")
        df.show(5, truncate=False)

        print("\nPadrões de valores nulos:")
        df.groupBy(
            F.col("passenger_count").isNull().alias("passenger_count_nulo"),
            F.col("RatecodeID").isNull().alias("RatecodeID_nulo"),
            F.col("store_and_fwd_flag").isNull().alias("store_and_fwd_flag_nulo"),
            F.col("congestion_surcharge").isNull().alias("congestion_surcharge_nulo"),
            F.col("Airport_fee").isNull().alias("Airport_fee_nulo"),
            F.col("request_source").isNull().alias("request_source_nulo"),
        ).count().orderBy("count", ascending=False).show(truncate=False) 
        

        print("\nDistribuição por request_source:")
        df.groupBy("request_source").count().orderBy(
            F.desc("count")
        ).show(truncate=False)

        print("\nAnálise por request_source:") 

        df.groupBy("request_source").agg(
            F.count("*").alias("total_viagens"),
            F.round(F.avg("trip_distance"), 2).alias("distancia_media"),
            F.round(F.avg("total_amount"), 2).alias("valor_medio"),
        ).orderBy(
            F.desc("total_viagens")
        ).show(truncate=False)

        #2. Análise de mobilidade
        print("\nQualidade da distância das viagens:")

        df.select(
            F.count("*").alias("total_registros"),
            F.sum(
                F.when(F.col("trip_distance") < 0, 1).otherwise(0)
            ).alias("distancia_negativa"),
            F.sum(
                F.when(F.col("trip_distance") == 0, 1).otherwise(0)
            ).alias("distancia_zero"),
            F.sum(
                F.when(F.col("trip_distance").isNull(), 1).otherwise(0)
            ).alias("distancia_nula"),
        ).show()

        print("\nAnálise das viagens com distância zero:")

        df.filter(
            F.col("trip_distance") == 0
        ).select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount"
        ).show(10, truncate=False)

        #3. Análise financeira
        print("\nAnálise de valores negativos:")

        df.select(
            F.sum(
                F.when(F.col("fare_amount") < 0, 1).otherwise(0)
            ).alias("tarifas_negativas"),
            F.sum(
                F.when(F.col("total_amount") < 0, 1).otherwise(0)
            ).alias("totais_negativos"),
            F.sum(
                F.when(
                    (F.col("trip_distance") == 0) &
                    (F.col("total_amount") < 0),
                    1
                ).otherwise(0)
            ).alias("distancia_zero_e_total_negativos")
        ).show()

        
        print("\nDistância zero com valores positivos:")

        df.filter(
            (F.col("trip_distance") == 0) &
            (F.col("fare_amount") > 0) &
            (F.col("total_amount") > 0)
        ).agg(
            F.count("*").alias("registros")
        ).show()

        
        print("\nDuração das viagens com distância zero:")

        df.filter(
            F.col("trip_distance") == 0
        ).select(
            (
                F.unix_timestamp("tpep_dropoff_datetime") -
                F.unix_timestamp("tpep_pickup_datetime")
            ).alias("duracao_segundos")
        ).agg(
            F.min("duracao_segundos").alias("duracao_minima"),
            F.avg("duracao_segundos").alias("duracao_media"),
            F.max("duracao_segundos").alias("duracao_maxima")
        ).show()
                
        print("\nDistribuição das durações com distância zero:")

        duracao = F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")

        df.filter(
            F.col("trip_distance") == 0
        ).withColumn(
            "duracao_segundos", duracao
        ).withColumn(
            "faixa_duracao",
            F.when(F.col("duracao_segundos") <= 60, "Até 1 minuto")
            .when(F.col("duracao_segundos") <= 600, "1 a 10 minutos")
            .when(F.col("duracao_segundos") <= 3600, "10 a 60 minutos")
            .otherwise("Mais de 60 minutos")
        ).groupBy(
            "faixa_duracao"
        ).count().orderBy(
            "faixa_duracao"
        ).show(truncate=False)

        print("\nTipos de pagamento nas viagens com distância zero:")

        df.filter(
            F.col("trip_distance") == 0
        ).groupBy(
            "payment_type"
        ).count().orderBy(
            F.col("count").desc()
        ).show()

        print("\nDistribuição de payment_type na base completa: ")

        df.groupBy("payment_type").count().orderBy(
            F.col("count").desc()
        ).show()

        print("\nAnálise de payment_type = 0 com distância zero:")

        df.filter(
            (F.col("trip_distance") == 0) &
            (F.col("payment_type") == 0)
        ).agg(
            F.count("*").alias("total_registros"),
            F.sum(
                F.when(F.col("fare_amount") < 0, 1).otherwise(0)
            ).alias("tarifas_negativas"),
            F.sum(
                F.when(F.col("total_amount") < 0, 1).otherwise(0)
            ).alias("totais_negativos"),
            F.avg("fare_amount").alias("tarifa_media"),
            F.avg("total_amount").alias("valor_total_medio")
        ).show()

        print("\nFornecedores das viagens com distância zero:")

        df.filter(
            F.col("trip_distance") == 0
        ).groupBy(
            "VendorID"
        ).count().orderBy(
            F.col("count").desc()
        ).show()

        print("\nProporção de viagens com distância zero por fornecedor:")

        df.groupBy("VendorID").agg(
            F.count("*").alias("total_viagens"),
            F.sum(
                F.when(F.col("trip_distance") == 0, 1).otherwise(0)
            ).alias("distancia_zero"),
            F.round(
                F.avg(
                    F.when(F.col("trip_distance") == 0, 1).otherwise(0)
                ) * 100,
                2
            ).alias("percentual_distancia_zero")
        ).orderBy(
            F.col("percentual_distancia_zero").desc()
        ).show()

        print("\nDuração das viagens com distância zero por fornecedor: ")

        df.filter(
            F.col("trip_distance") == 0
        ).withColumn(
            "duração_minutos",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60
        ).groupBy(
            "VendorID"
        ).agg(
            F.count("*").alias("total_viagens"),
            F.round(F.avg("duração_minutos"), 2).alias("duração_media_min"),
            F.round(F.min("duração_minutos"), 2).alias("duração_min_min"),
            F.round(F.max("duração_minutos"), 2).alias("duracao_max_min")
        ).orderBy(
            F.col("total_viagens").desc()
        ).show()

        print("\nQualidade dos registros do fornecedor 7 com distância zero:")

        df.filter(
            (F.col("VendorID") == 7) &
            (F.col("trip_distance") == 0)
        ).agg(
            F.count("*").alias("total_viagens"),
            F.sum(
                F.when(
                    F.col("fare_amount") > 0, 1
                ).otherwise(0)
            ).alias("tarifas_positivas"),
            F.sum(
                F.when(
                    F.col("total_amount") > 0, 1
                ).otherwise(0)
            ).alias("totais_positivos"),
            F.sum(
                F.when(
                    F.col("tpep_pickup_datetime") ==
                    F.col("tpep_dropoff_datetime"), 1
                ).otherwise(0)
            ).alias("horarios_identicos")
        ).show()

        print("\nValidação das viagens para análises de mobilidade:")

        df.withColumn(
            "duracao_segundos",
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ).agg(
            F.count("*").alias("total_registros"),

            F.sum(
                F.when(F.col('trip_distance') <= 0, 1).otherwise(0)
            ).alias("distancia_nao_positiva"),

            F.sum(
                F.when(F.col("duracao_segundos") <= 0, 1).otherwise(0)
            ).alias("duracao_nao_positiva"),

            F.sum(
                F.when(
                    (F.col("trip_distance") > 0) &
                    (F.col("duracao_segundos") > 0),
                    1
                ).otherwise(0)
            ).alias("candidatas_mobilidade")
           ).show()

        print("\nAplicando critérios iniciais de mobilidade:")

        df_mobilidade = df.withColumn(
                "duracao_segundos",
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ).filter(
                (F.col("trip_distance") > 0) &
                (F.col("duracao_segundos") > 0)
            )

        print(f"Registros originais: {df.count()}")
        print(f"Registros candidatos: {df_mobilidade.count()}")

        print("\nResumo dos dados selecionados:")

        df_mobilidade.select(
                F.min("trip_distance").alias("distancia_minima"),
                F.max("trip_distance").alias("distancia_maxima"),
                F.min("duracao_segundos").alias("duracao_minima_seg"),
                F.max("duracao_segundos").alias("duracao_maxima_seg")
        ).show()

        print("\nViagens com maiores distâncias: ")

        df_mobilidade.orderBy(
            F.col("trip_distance").desc()
        ).select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount"
        ).show(10, truncate=False)

        print("\nDistribuição das distâncias das viagens: ")

        df_mobilidade.select(
            F.expr("""
            percentile_approx(
            trip_distance, 
            array( 0.95, 0.99, 0.999)
            )
            """).alias("percentis")
        ).show(truncate=False)

        print("\nDistribuição das durações das viagens:")

        df_mobilidade.select(
            F.expr("""
            percentile_approx(
            duracao_segundos,
            array(0.95, 0.99, 0.999),
            1000)
            """).alias("percentis")
        ).show(truncate=False)

        print("\nRegistros acima dos limites candidatos: ")

        df_mobilidade.agg(
            F.sum(
                F.when(F.col("trip_distance") > 19.64, 1).otherwise(0)
            ).alias("distancia_acima_p99"),

            F.sum(
                F.when(F.col("duracao_segundos") > 3730, 1).otherwise(0)
            ).alias("duracao_acima_p99"),

            F.sum(
                F.when(
                    (F.col("trip_distance") > 19.64) |
                    (F.col("duracao_segundos") > 3730),
                    1
                ).otherwise(0)
            ).alias("registros_fora_dos_limites")
        ).show()

        print("\nViagens acima dos dois limites:")

        quantidade = df_mobilidade.filter(
            (F.col("trip_distance") > 19.64) &
            (F.col("duracao_segundos") > 3730)
        ).count()

        print(f"Total: {quantidade}")

        df_tempos = df.withColumn(
            "duracao_segundos",
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )

        print("\nConsistência dos horários: ")

        df_tempos.select(
            F.sum(
                F.when(F.col("duracao_segundos") < 0, 1).otherwise(0)
            ).alias("duracao_negativa"),

            F.sum(
                F.when(F.col("duracao_segundos") == 0, 1).otherwise(0)
            ).alias("duracao_zero")
        ).show()

        df_tempos.filter(
        F.col("duracao_segundos") < 0
        ).select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount"
        ).show(truncate=False)

        print("\nExtremos dos valores financeiros")

        df.select(
            F.min("fare_amount").alias("menor_fare"),
            F.max("fare_amount").alias("maior_fare"),
            F.min("total_amount").alias("menor_total"),
            F.max("total_amount").alias("maior_total")
        ).show()

        print("\nDistribuição dos valores de tarifa:")

        df.select(
            F.sum(
                F.when(F.col("fare_amount") > 500, 1).otherwise(0)
            ).alias("fare_acima_500"),

            F.sum(
                F.when(F.col("total_amount") > 500, 1).otherwise(0)
            ).alias("total_acima_500")
        ).show()


        df.orderBy(
        F.col("total_amount").desc()
        ).select(
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "total_amount"
        ).show(10, truncate=False)


        print("\nIntegridade dos identificadores de localização:")

        df.select(
            F.min("PULocationID").alias("PU_min"),
            F.max("PULocationID").alias("PU_max"),
            F.count(F.when(F.col("PULocationID").isNull(), 1)).alias("PU_nulos"),

            F.min("DOLocationID").alias("DO_min"),
            F.max("DOLocationID").alias("DO_max"),
            F.count(F.when(F.col("DOLocationID").isNull(), 1)).alias("DO_nulos")
        ).show()

        print("\nValores de store_and_fwd_flag:")

        df.groupBy("store_and_fwd_flag").count().show()

        print("\nValores de request_source:")

        df.groupBy("request_source").count().orderBy(
            F.col("count").desc()
        ).show()

        print("\nDistribuição de passenger_count:")

        df.groupBy("passenger_count").count().orderBy(
            "passenger_count"
        ).show()

        print("\nDistribuição de RatecodeID:")

        df.groupBy("RatecodeID").count().orderBy(
            "RatecodeID"
        ).show()

        print("\nViagens com passenger_count igual a zero:")

        df.filter(
            F.col("passenger_count") == 0
        ).select(
            F.count("*").alias("total"),
            F.sum(
                F.when(F.col("trip_distance") == 0, 1).otherwise(0)
            ).alias("distancia_zero"),
            F.sum(
                F.when(
                    F.col("tpep_pickup_datetime") ==
                    F.col("tpep_dropoff_datetime"), 1
                ).otherwise(0)
            ).alias("duracao_zero")
        ).show()

        print("\nRegistros duplicados:")

        quantidade_duplicados = (
            df.count() - df.dropDuplicates().count()
        )

        print(f"Total: {quantidade_duplicados}")

        print("\nRatecodeID 99:")

        df.filter(
            F.col("RatecodeID") == 99
        ).select(
            F.count("*").alias("total"),
            F.sum(
                F.when(F.col("trip_distance") > 0, 1).otherwise(0)
            ).alias("distancia_positiva"),
            F.sum(
                F.when(
                    F.col("tpep_pickup_datetime") <
                    F.col("tpep_dropoff_datetime"), 1
                ).otherwise(0)
            ).alias("duracao_positiva")
        ).show()


    finally:
        spark.stop()


if __name__ == "__main__":
    main()
