# --- tests/test_config.py ---
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# 1. Environment Definitions
BRONZE_CATALOG = "data_bronze"
BRONZE_SCHEMA = "bronze"
BRONZE_PATH = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}"

# 2. Corrected Table List (Fixed all typos here)
EXPECTED_TABLES = [
    "cities_crosswalk", 
    "zip_time_series", 
    "state_time_series", 
    "neighborhood_time_series", 
    "metro_time_series", 
    "datadictionary", 
    "countycrosswalk_zillow", 
    "county_time_series", 
    "city_time_series"
]

# 3. Validation Metadata
MANDATORY_COLS = ["load_dt", "source"]

# Allowed values for the 'source' column
VALID_SOURCES = [f"{t}.csv" for t in EXPECTED_TABLES] + ["chunk1.csv", "chunk2.csv", "chunk3.json", "chunk4.xml"]

# Geographic lookup for value constraint testing
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

# Column-specific value constraints
VALUE_CONSTRAINTS = {
    "cities_crosswalk": {"State": list(STATE_MAP.keys())},
    "countycrosswalk_zillow": {"StateName": list(STATE_MAP.values())}
}

# 4. Global Schema Contract (load_dt is Timestamp, others are String)
# This is used for assertSchemaEqual
COMMON_CONTRACT = StructType([
    StructField("load_dt", TimestampType(), True),
    StructField("source", StringType(), True)
])