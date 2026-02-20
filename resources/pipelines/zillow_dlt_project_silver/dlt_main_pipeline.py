import dlt
from src_logic import build_silver_schema, transform_logic
from config.schema_config import DIMENSION_TABLES, STREAMING_TABLES, BRONZE_PREFIX, TABLE_DESCRIPTIONS

def create_medallion_pipeline():
    for ds in DIMENSION_TABLES:
        table_name = ds.lower()
        # Discoverability: Add table descriptions from config
        comment = TABLE_DESCRIPTIONS.get(table_name, "Silver Dimension")
        
        @dlt.table(name=f"dim_{table_name}", schema=build_silver_schema(table_name), comment=comment)
        def dimension_table(ds_name=table_name):
            return transform_logic(spark.read.table(f"{BRONZE_PREFIX}.{ds_name}"), ds_name)

    for ds in STREAMING_TABLES:
        table_name = ds.lower()
        comment = TABLE_DESCRIPTIONS.get(table_name, "Silver Fact Table")
        
        @dlt.table(name=f"fact_{table_name}", schema=build_silver_schema(table_name), comment=comment)
        def fact_table(ds_name=table_name):
            return transform_logic(spark.readStream.table(f"{BRONZE_PREFIX}.{ds_name}"), ds_name)

create_medallion_pipeline()