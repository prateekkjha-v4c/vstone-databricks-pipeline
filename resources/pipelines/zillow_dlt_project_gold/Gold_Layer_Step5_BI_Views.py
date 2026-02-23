import logging
from pyspark.sql.utils import AnalysisException

# --- 1. LOGGING CONFIGURATION ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDeployment")

# --- 2. SECURE CONFIGURATION IMPORT ---
try:
    from state_mapping import (
        GOLD_CATALOG, FULL_SCHEMA_PATH, PERMISSIONS_TABLE,
        VIEW_INVESTMENT_SCORECARD, VIEW_MARKET_PULSE,
        VIEW_MARKET_PULSE_DDM, VIEW_MARKET_PULSE_CLS, VIEW_INVESTMENT_RLS,
        FACT_MARKET_METRICS_GOLD, DIM_GEOGRAPHY_GOLD, DIM_STATE, DIM_COUNTY
    )
except ImportError as ie:
    logger.error(f"Failed to import configuration: {str(ie)}")
    raise

# --- 3. ENTERPRISE GLOSSARY (Tables Only) ---
ENTERPRISE_GLOSSARY = {
    PERMISSIONS_TABLE: {
        "description": "Authorization matrix mapping system users to their permitted geographic states for RLS.",
        "is_view": False
    }
}

# --- 4. PARAMETERIZED SQL STATEMENTS ---
sql_statements = [
    f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_PATH}",
    
    # Tables retain comments for security auditing
    f"""CREATE TABLE IF NOT EXISTS {PERMISSIONS_TABLE} 
        (user_email STRING, allowed_state STRING) 
        USING DELTA
        COMMENT '{ENTERPRISE_GLOSSARY[PERMISSIONS_TABLE]['description']}'""",

    f"DELETE FROM {PERMISSIONS_TABLE} WHERE user_email = 'prateek.k.jha@v4c.ai'",
    f"INSERT INTO {PERMISSIONS_TABLE} VALUES ('prateek.k.jha@v4c.ai', 'CT')",

    # Views: Comments removed for clean deployment
    f"""CREATE OR REPLACE VIEW {VIEW_INVESTMENT_SCORECARD} 
    AS SELECT 
        f.date, s.state, c.county, g.city,
        f.zhvi_allhomes AS home_value,
        f.zri_allhomes AS monthly_rent,
        COALESCE(ROUND(try_divide((f.zri_allhomes * 12), f.zhvi_allhomes), 2), 0) AS annual_rental_yield,
        f.pricetorentratio_allhomes,
        COALESCE(
            ROUND(
                try_divide(
                    (f.zhvi_allhomes - LAG(f.zhvi_allhomes, 12) OVER (PARTITION BY g.unique_city_id ORDER BY f.date)),
                    LAG(f.zhvi_allhomes, 12) OVER (PARTITION BY g.unique_city_id ORDER BY f.date)
                ), 2
            ), 0
        ) AS yoy_appreciation
    FROM {FACT_MARKET_METRICS_GOLD} f
    JOIN {DIM_GEOGRAPHY_GOLD} g ON f.regionname = g.unique_city_id
    JOIN {DIM_STATE} s ON g.state_id = s.state_id
    JOIN {DIM_COUNTY} c ON g.county_id = c.county_id
    WHERE s.is_active = 1 AND c.is_active = 1""",

    f"""CREATE OR REPLACE VIEW {VIEW_MARKET_PULSE} 
    AS SELECT 
        f.date, s.state, c.county, g.city,
        f.pctoflistingswithpricereductions_allhomes AS price_cut_pct,
        f.medianpricecutdollar_allhomes AS price_cut_amt,
        f.inventoryraw_allhomes AS total_inventory,
        f.zhvi_allhomes AS median_value,
        ROUND(try_divide(f.zhvipersqft_allhomes, f.zhvi_allhomes), 4) AS value_density_ratio
    FROM {FACT_MARKET_METRICS_GOLD} f
    JOIN {DIM_GEOGRAPHY_GOLD} g ON f.regionname = g.unique_city_id
    JOIN {DIM_STATE} s ON g.state_id = s.state_id
    JOIN {DIM_COUNTY} c ON g.county_id = c.county_id
    WHERE s.is_active = 1 AND c.is_active = 1""",

    f"""CREATE OR REPLACE VIEW {VIEW_MARKET_PULSE_DDM} AS
    SELECT 
        date, state,
        CASE WHEN current_user() LIKE '%@internal.com' THEN city ELSE '*******' END AS city_masked
    FROM {VIEW_MARKET_PULSE}""",

    f"""CREATE OR REPLACE VIEW {VIEW_INVESTMENT_RLS} AS
    SELECT *
    FROM {VIEW_INVESTMENT_SCORECARD} s
    WHERE EXISTS (
        SELECT 1 FROM {PERMISSIONS_TABLE} p
        WHERE p.user_email = current_user()
        AND (p.allowed_state = s.state OR p.allowed_state = 'ALL')
    )""",

    f"""CREATE OR REPLACE VIEW {VIEW_MARKET_PULSE_CLS} AS
    SELECT 
        date, state,
        CASE 
            WHEN current_user() IN ('admin@example.com', 'lead_dev@example.com') 
            THEN county ELSE 'REDACTED' 
        END AS source_info
    FROM {VIEW_MARKET_PULSE}"""
]

# --- 5. DISCOVERABILITY FUNCTIONS ---
def apply_table_metadata():
    """Applies descriptions only to physical tables, skipping Views."""
    logger.info("Applying metadata for physical tables...")
    for table_path, metadata in ENTERPRISE_GLOSSARY.items():
        if metadata.get("is_view", False):
            continue
            
        try:
            desc = metadata.get("description", "").strip()
            if desc:
                spark.sql(f"COMMENT ON TABLE {table_path} IS '{desc}'")
                logger.info(f"Metadata applied to table: {table_path}")
        except Exception as e:
            logger.warning(f"Metadata skipped for {table_path}: {str(e)}")

# --- 6. DEPLOYMENT CORE ---
def validate_environment():
    dependencies = [FACT_MARKET_METRICS_GOLD, DIM_GEOGRAPHY_GOLD, DIM_STATE, DIM_COUNTY]
    for table in dependencies:
        if not spark.catalog.tableExists(table):
            logger.error(f"Dependency {table} not found.")
            return False
    return True

def run_gold_deployment():
    logger.info("Starting Gold layer deployment...")
    
    if not validate_environment():
        raise Exception("Pre-deployment validation failed.")

    for i, statement in enumerate(sql_statements):
        cleaned_sql = statement.strip().rstrip(';')
        if not cleaned_sql:
            continue
        try:
            logger.info(f"Step {i+1}/{len(sql_statements)}: Executing SQL...")
            spark.sql(cleaned_sql)
        except Exception as e:
            logger.error(f"Step {i+1} failed: {str(e)}")
            raise e

    apply_table_metadata()
    logger.info(f"Success: Gold layer deployed to {FULL_SCHEMA_PATH}.")

if __name__ == "__main__":
    try:
        run_gold_deployment()
    except Exception as final_e:
        logger.error(f"Deployment process failed: {final_e}")
        raise final_e