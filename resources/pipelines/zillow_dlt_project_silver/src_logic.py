import re
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from config.schema_config import SCHEMA_MAPPINGS, BRONZE_PREFIX

@pandas_udf("double")
def normalize_currency_udf(v: pd.Series) -> pd.Series:
    """Standardize currency values to 2 decimal places."""
    return v.round(2)

def standardize_header(name: str) -> str:
    """lowercase and remove non-alphanumeric chars."""
    if not name: return name
    return re.sub(r'[^a-z0-9]', '', name.lower())

def build_silver_schema(table_name):
    """
    Builds the DDL schema string. 
    Defines row_id as a native Identity column to ensure sequential autoincrement.
    """
    mapping = SCHEMA_MAPPINGS.get(table_name, {})
    # IDENTITY (START WITH 1 INCREMENT BY 1) ensures the 1, 2, 3... pattern requested.
    schema_parts = ["row_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate primary key'"]
    
    for col, dtype in mapping.items():
        if col.lower() not in ["row_id", "load_dt", "source"]:
            schema_parts.append(f"{col} {dtype} COMMENT 'Enterprise standardized {col}'")
            
    schema_parts.append("load_dt TIMESTAMP COMMENT 'Ingestion timestamp'")
    schema_parts.append("source STRING COMMENT 'Source table path'")
    return ", ".join(schema_parts)

def transform_logic(df, ds_name):
    """
    Standardizes data. 
    Crucially drops 'row_id' to allow Delta Identity to take over.
    """
    # 1. Clean existing headers from source
    for col in df.columns:
        df = df.withColumnRenamed(col, standardize_header(col))
    
    mapping = SCHEMA_MAPPINGS.get(ds_name, {})
    select_expr = []
    
    for target_col, target_type in mapping.items():
        # CRITICAL: Do NOT include row_id in the select statement.
        # If the DLT table schema has row_id but the DF does not, Delta populates it.
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
            # Handle missing columns from source
            default_val = F.lit(0.0) if target_type.upper() == "DOUBLE" else F.lit(None)
            select_expr.append(default_val.cast(target_type).alias(target_col))
    
    # Return the DF without row_id
    return (
        df.select(select_expr)
        .withColumn("load_dt", F.current_timestamp())
        .withColumn("source", F.lit(f"{BRONZE_PREFIX}.{ds_name}"))
    )