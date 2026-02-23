import dlt
from pyspark.sql import functions as F
from src_logic import build_silver_schema, transform_logic
from config.schema_config import DIMENSION_TABLES, STREAMING_TABLES, BRONZE_PREFIX, TABLE_DESCRIPTIONS

def create_medallion_pipeline():
    
    # --- DIMENSION TABLES (Batch) ---
    def generate_dimension_table(ds_name):
        t_name = ds_name.lower()
        target_table = f"dim_{t_name}"
        
        @dlt.table(
            name=target_table,
            comment=TABLE_DESCRIPTIONS.get(t_name, ""),
            table_properties={"quality": "silver"},
            schema=build_silver_schema(t_name) # DDL contains IDENTITY
        )
        def dim_logic():
            # Crucial: transform_logic must NOT return a 'row_id' column
            return transform_logic(
                spark.read.table(f"{BRONZE_PREFIX}.{ds_name}"), 
                t_name, 
                is_streaming=False
            )

    # --- FACT TABLES (Streaming) ---
    def generate_streaming_table(ds_name):
        t_name = ds_name.lower()
        target_table = f"fact_{t_name}"
        
        @dlt.table(
            name=target_table,
            comment=TABLE_DESCRIPTIONS.get(t_name, ""),
            table_properties={"quality": "silver"},
            schema=build_silver_schema(t_name) # DDL contains IDENTITY
        )
        def fact_logic():
            # Crucial: transform_logic must NOT return a 'row_id' column
            return transform_logic(
                spark.readStream.table(f"{BRONZE_PREFIX}.{ds_name}"), 
                t_name, 
                is_streaming=True
            )

    # Registering tables
    for ds in DIMENSION_TABLES:
        generate_dimension_table(ds)

    for ds in STREAMING_TABLES:
        generate_streaming_table(ds)

create_medallion_pipeline()