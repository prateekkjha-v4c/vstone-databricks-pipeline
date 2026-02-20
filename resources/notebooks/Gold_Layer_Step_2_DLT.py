import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from state_mapping import (
    DLT_OPT_PROPERTIES, STG_GEO_TABLE, STG_DICT_TABLE, 
    STG_FACT_TABLE, GEO_TRACK_COLUMNS, FACT_PRIORITY_COLS
)

# ======================================================================================
# --- 1. SOURCE VIEWS (Streaming) ---
# Entry points for raw data from the Silver layer.
# ======================================================================================

@dlt.view(comment="Raw streaming view for Geography data.")
def stg_geo_stream():
    return spark.readStream.table(STG_GEO_TABLE)

@dlt.view(comment="Raw streaming view for Market Fact data.")
def stg_fact_stream():
    return spark.readStream.table(STG_FACT_TABLE)

@dlt.view(comment="Raw streaming view for Metric Dictionary data.")
def stg_dict_stream():
    return spark.readStream.table(STG_DICT_TABLE)

# ======================================================================================
# --- 2. DATA QUALITY: QUARANTINE ---
# Diverts corrupt records into a separate table for auditing.
# ======================================================================================

@dlt.table(name="gold_quarantined", comment="Audit table for failed records.")
def gold_quarantined():
    geo_fails = dlt.read_stream("stg_geo_stream").filter("state IS NULL OR county IS NULL") \
        .withColumn("quarantine_source", F.lit(STG_GEO_TABLE)) \
        .withColumn("fail_reason", F.lit("Missing State or County Name"))

    fact_fails = dlt.read_stream("stg_fact_stream").filter("region_type IS NULL") \
        .withColumn("quarantine_source", F.lit(STG_FACT_TABLE)) \
        .withColumn("fail_reason", F.lit("Missing Region Type"))

    return geo_fails.unionByName(fact_fails, allowMissingColumns=True) \
        .withColumn("quarantine_ts", F.current_timestamp())

# ======================================================================================
# --- 3. ID LOOKUP TABLES (Normalization) ---
# Stable surrogate keys for downstream joins.
# ======================================================================================

@dlt.table(name="id_lookup_state")
def id_lookup_state():
    return spark.read.table(STG_GEO_TABLE).select("state").distinct() \
        .withColumn("state_id", F.row_number().over(Window.orderBy("state")))

@dlt.table(name="id_lookup_county")
def id_lookup_county():
    return spark.read.table(STG_GEO_TABLE).select("county").distinct() \
        .withColumn("county_id", F.row_number().over(Window.orderBy("county")))

@dlt.table(name="id_lookup_region_type")
def id_lookup_region_type():
    return spark.read.table(STG_FACT_TABLE).select("region_type").distinct() \
        .withColumn("region_type_id", F.row_number().over(Window.orderBy("region_type")))

@dlt.table(name="id_lookup_metric")
def id_lookup_metric():
    return spark.read.table(STG_DICT_TABLE).select("metric_name", "definition").distinct() \
        .withColumn("metric_id", F.row_number().over(Window.orderBy("metric_name")))

# ======================================================================================
# --- 4. SCD TYPE 2 HELPER (Fixed) ---
# Uses delayed decoration to avoid property '__name__' setter errors.
# ======================================================================================

def apply_scd2_logic(target_base_name, target_final_name, source_view, keys, sequence_col, table_desc, source_name, expectations=None):
    # Step A: Create hidden base table
    dlt.create_streaming_table(name=target_base_name, table_properties=DLT_OPT_PROPERTIES)
    
    # Step B: Apply SCD Type 2 tracking
    dlt.apply_changes(
        target=target_base_name, 
        source=source_view, 
        keys=keys, 
        sequence_by=F.col(sequence_col), 
        stored_as_scd_type="2"
    )

    # Step C: Dynamic function renaming to satisfy DLT compiler
    def scd_final_wrapper():
        return dlt.read(target_base_name) \
            .withColumn("start_dt", F.col("__start_at")) \
            .withColumn("end_dt", F.col("__end_at")) \
            .withColumn("is_active", F.when(F.col("__end_at").isNull(), 1).otherwise(0)) \
            .withColumn("source", F.lit(source_name)) \
            .drop("__start_at", "__end_at")

    # Rename the inner function before it is registered as a DLT node
    scd_final_wrapper.__name__ = f"func_{target_final_name}"
    
    # Programmatic decoration
    final_table = dlt.table(name=target_final_name, comment=table_desc)(scd_final_wrapper)
    if expectations:
        dlt.expect_all_or_drop(expectations)(final_table)

# ======================================================================================
# --- 5. STREAM PREP VIEWS ---
# ======================================================================================

@dlt.view
def stg_state_prep():
    return dlt.read_stream("stg_geo_stream").alias("s").join(dlt.read("id_lookup_state").alias("l"), "state", "inner") \
        .select("l.state_id", "s.state", "s.load_dt")

@dlt.view
def stg_county_prep():
    return dlt.read_stream("stg_geo_stream").alias("s").join(dlt.read("id_lookup_county").alias("l"), "county", "inner") \
        .select("l.county_id", "s.county", "s.load_dt")

@dlt.view
def stg_region_type_prep():
    return dlt.read_stream("stg_fact_stream").alias("s").join(dlt.read("id_lookup_region_type").alias("l"), "region_type", "inner") \
        .select("l.region_type_id", "s.region_type", "s.load_dt")

@dlt.view
def stg_dict_prep():
    return dlt.read_stream("stg_dict_stream").alias("s").join(dlt.read("id_lookup_metric").alias("l"), "metric_name", "inner") \
        .select("l.metric_id", "s.metric_name", "s.definition", "s.load_dt")

# --- 6. EXECUTE SCD2 DIMENSIONS ---
apply_scd2_logic("dim_state_base", "dim_state", "stg_state_prep", ["state_id"], "load_dt", "State Dim", STG_GEO_TABLE)
apply_scd2_logic("dim_county_base", "dim_county", "stg_county_prep", ["county_id"], "load_dt", "County Dim", STG_GEO_TABLE)
apply_scd2_logic("dim_region_type_base", "dim_region_type", "stg_region_type_prep", ["region_type_id"], "load_dt", "Region Type", STG_FACT_TABLE)
apply_scd2_logic("dim_metric_dictionary_base", "dim_metric_dictionary", "stg_dict_prep", ["metric_id"], "load_dt", "Metric Dictionary", STG_DICT_TABLE)

# ======================================================================================
# --- 7. FINAL CONSUMPTION TABLES (Gold) ---
# ======================================================================================

@dlt.table(name="dim_geography_gold", comment="Master Geography Dimension.")
def dim_geography_gold():
    geo_stg = dlt.read_stream("stg_geo_stream").alias("stream")
    states = dlt.read("dim_state").filter("is_active = 1").select("state", "state_id")
    counties = dlt.read("dim_county").filter("is_active = 1").select("county", "county_id")
    
    return geo_stg.join(states, "state", "inner") \
                  .join(counties, "county", "inner") \
                  .select("stream.unique_city_id", "stream.city", "stream.fips", 
                          "stream.metroname_zillow", "state_id", "county_id", "stream.load_dt") \
                  .withColumn("source", F.lit(STG_GEO_TABLE))

@dlt.table(
    name="fact_market_metrics_gold",
    partition_cols=["region_type_id"],
    comment="Consolidated Fact table."
)
@dlt.expect_or_drop("valid_region_grain", "region_type_id IS NOT NULL")
def fact_market_metrics_gold():
    fact_df = spark.read.table(STG_FACT_TABLE).alias("f")
    region_dim = dlt.read("dim_region_type").filter("is_active = 1").select("region_type", "region_type_id")
    
    return fact_df.join(region_dim, "region_type", "inner") \
                  .drop("region_type") \
                  .withColumn("fact_market_metrics_id", F.monotonically_increasing_id()) \
                  .withColumn("source", F.lit(STG_FACT_TABLE)) \
                  .distinct()

# --- 8. DATE DIMENSION ---
@dlt.table(name="dim_date_gold", comment="Static Date Dimension 1970-2020.")
def dim_date_gold():
    return spark.sql("""
        SELECT 
            date,
            year(date) AS year,
            month(date) AS month,
            quarter(date) AS quarter,
            dayofmonth(date) AS day_of_month,
            weekofyear(date) AS week_of_year,
            dayofweek(date) AS day_of_week,
            date_format(date, 'EEEE') AS day_name,
            date_format(date, 'MMMM') AS month_name,
            CASE WHEN month(date) >= 10 THEN year(date) + 1 ELSE year(date) END AS fiscal_year,
            CASE WHEN dayofweek(date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
            date_format(date, 'yyyy-MM') AS year_month,
            dayofyear(date) AS day_of_year,
            'STATIC_GENERATOR' AS source
        FROM (SELECT explode(sequence(to_date('1970-01-01'), to_date('2020-12-31'), interval 1 day)) as date)
    """)