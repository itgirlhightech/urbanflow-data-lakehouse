from pyspark.sql import functions as F


def check_schema(df, expected_columns):
    missing_columns = set(expected_columns) - set(df.columns)

    if missing_columns:
        return {
            "status": "FAILED",
            "missing_columns": list(missing_columns)
        }

    return {
        "status": "PASSED",
        "missing_columns": []
    }


def check_nulls(df, columns):
    null_counts = {}

    for column in columns:
        count = df.filter(F.col(column).isNull()).count()

        if count > 0:
            null_counts[column] = count
    
    if null_counts: 
        return {
            "status" : "FAILED",
            "null_counts" : null_counts
        }

    return {
        "status" : "PASSED",
        "null_counts": {}
    }

def check_duplicates(df, column):
    total_records = df.count()
    distinct_records = df.select(column).distinct().count()

    duplicates = total_records - distinct_records

    if duplicates > 0 :
        return {
            "status": "FAILED",
            "duplicates_count": duplicates
        }
    return {
        "status": "PASSED",
        "duplicates_count": 0
    }

def check_referencial_integrity(df, reference_df, column, reference_column):
    unmatched = (
        df.select(column)
        .distinct()
        .join(
            reference_df.select(reference_column).distinct(),
            F.col(column) == F.col(reference_column),
            "left_anti"
        )
    )
    invalid_count = unmatched.count()

    if invalid_count > 0:
        return {
            "status": "FAILED",
            "invalid_count": invalid_count
        }
    
    return {
        "status": "PASSED",
        "invalid_count": 0
    }

def check_mobility_rules(df):
    expected_validity = (
        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") <= 19.64) &
        (F.col("duracao_segundos") > 0) &
        (F.col("duracao_segundos") <= 3730) &
        (F.col("passenger_count") >= 1)
    )

    inconsistent = df.filter(
        F.coalesce(expected_validity, F.lit(False)) !=
        F.col("is_mobility_valid")
    )

    inconsistent_count = inconsistent.count()

    if inconsistent_count > 0:
        return {
            "status": "FAILED",
            "inconsistent_count": inconsistent_count
        }

    return {
        "status": "PASSED",
        "inconsistent_count": 0
    }


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("UrbanFlow-Quality-Test") \
        .master("local[2]") \
        .getOrCreate()

    df = spark.createDataFrame(
        [
        (1, "Manhattan"),
        (2, "Queens"),
        (2, "Queens"),
        (3, "Brooklyn")
        ],
        ["id", "borough"]
        
    )

    df_locations = spark.createDataFrame(
    [(161,), (237,), (999,)],
    ["PULocationID"]
   )

    df_reference = spark.createDataFrame(
        [(161,), (237,), (236,)],
        ["LocationID"]
    )

    df_mobility = spark.createDataFrame(
    [
        (2.5, 1200, 1, True),
        (0.0, 500, 1, False),
        (3.0, 1500, 2, True),
        (5.0, 2000, 1, False)
    ],
    [
        "trip_distance",
        "duracao_segundos",
        "passenger_count",
        "is_mobility_valid"
    ]
    )

    print(check_duplicates(df))
    print(check_referencial_integrity(df_locations, df_reference, "PULocationID", "LocationID"))
    print(check_mobility_rules(df_mobility))


    spark.stop()

