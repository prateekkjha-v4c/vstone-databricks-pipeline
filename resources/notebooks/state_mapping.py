# --- schema_mapping.py ---

# 1. Target Environment Definitions
GOLD_CATALOG = 'data_gold'
GOLD_SCHEMA = 'gold'
FULL_SCHEMA_PATH = f"{GOLD_CATALOG}.{GOLD_SCHEMA}"

PERMISSIONS_TABLE = f"{FULL_SCHEMA_PATH}.user_permissions"
VIEW_INVESTMENT_SCORECARD = f"{FULL_SCHEMA_PATH}.view_investment_scorecard"
VIEW_MARKET_PULSE = f"{FULL_SCHEMA_PATH}.view_market_pulse"
VIEW_MARKET_PULSE_DDM = f"{FULL_SCHEMA_PATH}.view_market_pulse_ddm"
VIEW_MARKET_PULSE_CLS = f"{FULL_SCHEMA_PATH}.view_market_pulse_cls"
VIEW_INVESTMENT_RLS = f"{FULL_SCHEMA_PATH}.view_investment_scorecard_rls"

# NEW: Gold Table References
FACT_MARKET_METRICS_GOLD = f"{FULL_SCHEMA_PATH}.fact_market_metrics_gold"
DIM_GEOGRAPHY_GOLD = f"{FULL_SCHEMA_PATH}.dim_geography_gold"
DIM_STATE = f"{FULL_SCHEMA_PATH}.dim_state"
DIM_COUNTY = f"{FULL_SCHEMA_PATH}.dim_county"

# 2. Source & Staging Table Paths
# Staging (Direct inputs for the Gold DLT pipeline)
STG_GEO_TABLE = "data_gold.data_stg.dim_geography"
STG_DICT_TABLE = "data_gold.data_stg.dim_metric_dictionary"
STG_FACT_TABLE = "data_gold.data_stg.fact_market_metrics"

# Silver (Upstream sources)
cities_dim = 'data_silver.silver.dim_cities_crosswalk'
county_dim = 'data_silver.silver.dim_countycrosswalk_zillow'

# Silver Fact Tables mapping
fact_configs = {
    "city": "data_silver.silver.fact_city_time_series",
    "county": "data_silver.silver.fact_county_time_series",
    "metro": "data_silver.silver.fact_metro_time_series",
    "neighborhood": "data_silver.silver.fact_neighborhood_time_series",
    "state": "data_silver.silver.fact_state_time_series",
    "zip": "data_silver.silver.fact_zip_time_series"
}

# List version of silver fact tables
facts_tables = [
    "fact_city_time_series", 
    "fact_county_time_series", 
    "fact_metro_time_series",
    "fact_neighborhood_time_series", 
    "fact_state_time_series", 
    "fact_zip_time_series"
]

# 3. Delta Live Tables (DLT) Performance Properties
DLT_OPT_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableChangeDataFeed": "true" 
}

# 4. Data Processing & Tracking Logic
# Standard cleanup for dimensions
cols_to_drop = ["row_id", "load_dt", "source"]

# SCD Type 2 tracking: These columns trigger a new historical record upon change
GEO_TRACK_COLUMNS = ["city", "fips", "metroname_zillow", "county_id", "state_id"]

# Fact table ordering: Optimizes file statistics and Z-Ordering
FACT_PRIORITY_COLS = ["regionname", "date", "region_type_id"]

# 5. Geographic Normalization Mapping
STATE_MAP = {
    'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
    'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota',
    'MO': 'Missouri', 'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina',
    'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia',
    'VT': 'Vermont', 'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'
}

# 6. Consolidated Fact Metrics List
# Ensures consistent column presence across all geographic grains
common_metrics = [
    "date", 
    "regionname", 
    "inventoryraw_allhomes", 
    "inventoryseasonallyadjusted_allhomes", 
    "medianlistingprice_1bedroom", 
    "medianlistingprice_2bedroom", 
    "medianlistingprice_3bedroom", 
    "medianlistingprice_4bedroom", 
    "medianlistingprice_5bedroomormore", 
    "medianlistingprice_allhomes", 
    "medianlistingprice_condocoop", 
    "medianlistingprice_duplextriplex", 
    "medianlistingprice_singlefamilyresidence", 
    "medianlistingpricepersqft_1bedroom", 
    "medianlistingpricepersqft_2bedroom", 
    "medianlistingpricepersqft_3bedroom", 
    "medianlistingpricepersqft_4bedroom", 
    "medianlistingpricepersqft_5bedroomormore", 
    "medianlistingpricepersqft_allhomes", 
    "medianlistingpricepersqft_condocoop", 
    "medianlistingpricepersqft_duplextriplex", 
    "medianlistingpricepersqft_singlefamilyresidence", 
    "medianpctofpricereduction_allhomes", 
    "medianpctofpricereduction_condocoop", 
    "medianpctofpricereduction_singlefamilyresidence", 
    "medianpricecutdollar_allhomes", 
    "medianpricecutdollar_condocoop", 
    "medianpricecutdollar_singlefamilyresidence", 
    "medianrentalprice_1bedroom", 
    "medianrentalprice_2bedroom", 
    "medianrentalprice_3bedroom", 
    "medianrentalprice_4bedroom", 
    "medianrentalprice_5bedroomormore", 
    "medianrentalprice_allhomes", 
    "medianrentalprice_condocoop", 
    "medianrentalprice_duplextriplex", 
    "medianrentalprice_multifamilyresidence5plusunits", 
    "medianrentalprice_singlefamilyresidence", 
    "medianrentalprice_studio", 
    "medianrentalpricepersqft_1bedroom", 
    "medianrentalpricepersqft_2bedroom", 
    "medianrentalpricepersqft_3bedroom", 
    "medianrentalpricepersqft_4bedroom", 
    "medianrentalpricepersqft_5bedroomormore", 
    "medianrentalpricepersqft_allhomes", 
    "medianrentalpricepersqft_condocoop", 
    "medianrentalpricepersqft_duplextriplex", 
    "medianrentalpricepersqft_multifamilyresidence5plusunits", 
    "medianrentalpricepersqft_singlefamilyresidence", 
    "medianrentalpricepersqft_studio", 
    "pctofhomesdecreasinginvalues_allhomes", 
    "pctofhomesincreasinginvalues_allhomes", 
    "pctoflistingswithpricereductions_allhomes", 
    "pctoflistingswithpricereductions_condocoop", 
    "pctoflistingswithpricereductions_singlefamilyresidence", 
    "pctoflistingswithpricereductionsseasadj_allhomes", 
    "pctoflistingswithpricereductionsseasadj_condocoop", 
    "pctoflistingswithpricereductionsseasadj_singlefamilyresidence", 
    "pricetorentratio_allhomes", 
    "zhvi_1bedroom", 
    "zhvi_2bedroom", 
    "zhvi_3bedroom", 
    "zhvi_4bedroom", 
    "zhvi_5bedroomormore", 
    "zhvi_allhomes", 
    "zhvi_bottomtier", 
    "zhvi_condocoop", 
    "zhvi_middletier", 
    "zhvi_singlefamilyresidence", 
    "zhvi_toptier", 
    "zhvipersqft_allhomes", 
    "zri_allhomes", 
    "zri_allhomesplusmultifamily", 
    "zri_multifamilyresidencerental", 
    "zri_singlefamilyresidencerental", 
    "zripersqft_allhomes"
]

# 7. Audit & Profiling Configurations
AUDIT_SCHEMA = "audit"
PROFILE_TABLE_NAME = "profile_summary"
PROFILE_FULL_PATH = f"{GOLD_CATALOG}.{AUDIT_SCHEMA}.{PROFILE_TABLE_NAME}"

# 8. Enterprise Business Glossary (Abstracted)
# We use this as a source of truth for which tables to profile and how to describe them
GOLD_METADATA_CONFIG = {
    "dim_geography_gold": {
        "description": "Master Geography Dimension with hierarchical mapping of City, County, and State.",
        "columns": {"city": "Official city name."}
    },
    "dim_county": {
        "description": "County dimension for regional aggregation.",
        "columns": {"county": "Name of the county."}
    },
    "dim_state": {
        "description": "State dimension for high-level filtering.",
        "columns": {"state": "Full state name."}
    },
    "dim_region_type": {
        "description": "Lookup table for geographic grains (e.g., zip, city, county).",
        "columns": {"region_type": "The category of geographic grain."}
    },
    "dim_metric_dictionary": {
        "description": "Business Glossary mapping internal headers to human-readable definitions.",
        "columns": {"definition": "Official business logic definition."}
    },
    "fact_market_metrics_gold": {
        "description": "Enterprise Fact table for time-series market metrics.",
        "columns": {"date": "Measurement period."}
    }
}

# The default list of tables to profile, derived directly from the config keys
DEFAULT_PROFILE_LIST = list(GOLD_METADATA_CONFIG.keys())