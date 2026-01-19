"""
Feature Engineering Job: Silver to Gold
---------------------------------------
Calculates rolling form metrics and enriches match data with Opponent/Goal details.
Features include:
 - Form_Last5: Total points in the last 5 games.
 - Form_Venue: Total points in the last 5 games at specific venue (Home/Away).
 - Opponent, Goals_For, Goals_Against: Added for frontend display.

Layer: Gold
Source: ADLS /mnt/silver/matches_silver
Sink: ADLS /mnt/gold/team_features (Delta Format)
"""

from pyspark.sql.functions import col, when, sum as _sum, lit
from pyspark.sql.window import Window

INPUT_PATH = "/mnt/silver/matches_silver"
OUTPUT_PATH = "/mnt/gold/team_features"

def create_team_stats
(df_played):
    """
    Explodes match data into team-centric view (Home & Away perspectives).
    Calculates points per match and captures opponent/goal details.
    """
    # Home Perspective
    df_home = df_played.
select(
        col("League"),
    col("MatchDate"),
    col("HomeTeam").alias("Team"),
    lit("Home").alias("Venue"),
    col("AwayTeam").alias("Opponent"),
    col("HomeTeamScore").alias("Goals_For"),
    col("AwayTeamScore").alias("Goals_Against"),
    (when
(col
("HomeTeamScore") > col
("AwayTeamScore"), 3)
         .when
(col
("HomeTeamScore") == col
("AwayTeamScore"), 1)
         .otherwise
(0)).alias
("Points")
    )

    # Away Perspective
    df_away = df_played.
select(
        col("League"),
    col("MatchDate"),
    col("AwayTeam").alias("Team"),
    lit("Away").alias("Venue"),
    col("HomeTeam").alias("Opponent"),
    col("AwayTeamScore").alias("Goals_For"),
    col("HomeTeamScore").alias("Goals_Against"),
    (when
(col
("AwayTeamScore") > col
("HomeTeamScore"), 3)
         .when
(col
("AwayTeamScore") == col
("HomeTeamScore"), 1)
         .otherwise
(0)).alias
("Points")
    )

    # Combine both views
return df_home.union
(df_away)

# Main Execution
if __name__ == "__main__":
    # 1. Load Data
    df_silver = spark.read.format
("delta").load
(INPUT_PATH)
    
    # Filter for completed matches only
    df_played = df_silver.filter
(col
("MatchStatus") == "Played")

    # 2. Normalize to Team-Centric Grain
    df_all_matches = create_team_stats
(df_played)

    # 3. Define Window Specifications for Rolling Metrics
    # Partition by League AND Team to prevent cross-league data contamination
    window_last5 = Window.partitionBy
("League", "Team") \
                          .orderBy
("MatchDate") \
                          .rowsBetween
(-4, 0)
                          
    window_venue_last5 = Window.partitionBy
("League", "Team", "Venue") \
                               .orderBy
("MatchDate") \
                               .rowsBetween
(-4, 0)

    # 4. Calculate Features
    df_features = df_all_matches.withColumn
(
        "Form_Last5", _sum
("Points").over
(window_last5)
    ).withColumn
(
        "Form_Venue", _sum
("Points").over
(window_venue_last5)
    )

    # 5. Write to Gold
print(f
"Writing feature table to {OUTPUT_PATH}...")
    df_features.write \
        .format
("delta") \
        .mode
("overwrite") \
        .option
("mergeSchema", "true") \
        .
save
(OUTPUT_PATH)

print("Gold Layer (Feature Engineering) update complete.")