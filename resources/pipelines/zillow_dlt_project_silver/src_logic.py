import re
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from config.schema_config import SCHEMA_MAPPINGS, BRONZE_PREFIX

@pandas_udf("double")
def normalize_currency_udf(v: pd.Series) -> pd.Series:
    return v.round(2)

def standardize_header(name: str) -> str:
    if not name: return name
    return re.sub(r'[^a-z0-9]', '', name.lower())

def build_silver_schema(table_name):
    mapping = SCHEMA_MAPPINGS.get(table_name, {})
    # row_id is defined with the IDENTITY property
    schema_parts = ["row_id BIGINT GENERATED ALWAYS AS IDENTITY"]
    
    for col, dtype in mapping.items():
        if col.lower() not in ["row_id", "load_dt", "source"]:
            schema_parts.append(f"{col} {dtype}")
            
    schema_parts.append("load_dt TIMESTAMP")
    schema_parts.append("source STRING")
    return ", ".join(schema_parts)

def transform_logic(df, ds_name, is_streaming=False):
    """
    Standardizes data and prepares for the IDENTITY column.
    """
    # 1. Header Standardization
    for col in df.columns:
        df = df.withColumnRenamed(col, standardize_header(col))
    
    mapping = SCHEMA_MAPPINGS.get(ds_name, {})
    select_expr = []
    
    # 2. Map business columns
    for target_col, target_type in mapping.items():
        # Do NOT include row_id in the select. 
        # Writing to a table with an Identity column requires omitting that column.
        if target_col.lower() in ["row_id", "load_dt", "source"]:
            continue
        
        lookup_key = standardize_header(target_col)
        if lookup_key in df.columns:
            if target_type.upper() == "DATE":
                casted_col = F.to_date(F.col(lookup_key))
            elif target_type.upper() == "DOUBLE":
                raw_cast = F.expr(f"try_cast({lookup_key} as DOUBLE)")
                casted_col = normalize_currency_udf(F.coalesce(raw_cast, F.lit(0.0)))
            else:
                casted_col = F.expr(f"try_cast({lookup_key} as {target_type})")
            select_expr.append(casted_col.alias(target_col))
        else:
            default_val = F.lit(0.0) if target_type.upper() == "DOUBLE" else F.lit(None)
            select_expr.append(default_val.cast(target_type).alias(target_col))
    
    # 3. Intermediate DataFrame
    # No row_id logic here; Spark is purely doing business transformations.
    return df.select(select_expr) \
             .withColumn("load_dt", F.current_timestamp()) \
             .withColumn("source", F.lit(f"{BRONZE_PREFIX}.{ds_name}"))