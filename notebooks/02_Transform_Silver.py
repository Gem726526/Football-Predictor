"""
Transformation Job: Bronze to Silver
------------------------------------
Reads raw JSON files, normalizes types, applies business logic for match results,
and consolidates all leagues into a single optimized Delta table.

Layer: Silver
Source: ADLS /mnt/bronze/
Sink: ADLS /mnt/silver/matches_silver (Delta Format)
"""

from pyspark.sql.functions import col, to_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from functools import reduce 
from pyspark.sql import DataFrame

# Configuration
LEAGUE_NAMES = ["EPL", "Bundesliga", "Serie_A", "Ligue_1", "La_Liga"]
INPUT_BASE_PATH = "/mnt/bronze"
OUTPUT_PATH = "/mnt/silver/matches_silver"

def load_and_tag_data(league_names: list) -> DataFrame:
    """
    Iterates through league names, loads raw data, and appends a 'League' column.
    Combines all dataframes into a single Spark DataFrame.
    """
    dataframes = []

    for name in league_names:
        file_path = f"{INPUT_BASE_PATH}/{name}_raw.json"
        try:
            # Read JSON with implicit schema inference
            df = spark.read.json(file_path)
            
            # Add League identifier column
            df_tagged = df.withColumn("League", lit(name))
            dataframes.append(df_tagged)
            print(f"Loaded DataFrame for: {name}")
            
        except Exception as e:
            print(f"Warning: Could not load data for {name}. Error: {str(e)}")

    if not dataframes:
        raise ValueError("No dataframes could be loaded. Check Bronze path.")

    # Union all dataframes into one
    return reduce(DataFrame.unionByName, dataframes)

def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Applies type casting and derives 'MatchStatus' and 'Winner' columns.
    """
    return df.withColumn(
        "MatchDate", to_timestamp(col("DateUtc"))
    ).withColumn(
        "MatchStatus", 
        when(col("HomeTeamScore").isNull(), "Scheduled").otherwise("Played")
    ).withColumn(
        "Winner",
        when(col("HomeTeamScore") > col("AwayTeamScore"), col("HomeTeam"))
        .when(col("AwayTeamScore") > col("HomeTeamScore"), col("AwayTeam"))
        .when(
            (col("HomeTeamScore") == col("AwayTeamScore")) & 
            (col("HomeTeamScore").isNotNull()), 
            "Draw"
        ).otherwise(None)
    )

# Main Execution
if __name__ == "__main__":
    # 1. Load and Union
    df_raw_combined = load_and_tag_data(LEAGUE_NAMES)
    
    # 2. Transform
    df_silver = apply_transformations(df_raw_combined)
    
    # 3. Select final schema
    final_df = df_silver.select(
        col("League"),
        col("MatchNumber"),
        col("RoundNumber"),
        col("MatchDate"),
        col("Location"),
        col("HomeTeam"),
        col("AwayTeam"),
        col("HomeTeamScore"),
        col("AwayTeamScore"),
        col("MatchStatus"),
        col("Winner")
    )

    # 4. Write to Delta Lake
    print(f"Writing data to {OUTPUT_PATH}...")
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(OUTPUT_PATH)
        
    print("Silver Layer update complete.")