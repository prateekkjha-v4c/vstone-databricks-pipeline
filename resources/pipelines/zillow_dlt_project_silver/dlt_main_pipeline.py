import dlt
from src_logic import build_silver_schema, transform_logic
from config.schema_config import DIMENSION_TABLES, STREAMING_TABLES, BRONZE_PREFIX, TABLE_DESCRIPTIONS

def create_medallion_pipeline():
    
    def generate_dimension_table(ds_name):
        table_name = ds_name.lower()
        comment = TABLE_DESCRIPTIONS.get(table_name, "Silver Dimension")
        
        # 1. Define the raw function
        def dim_func():
            return transform_logic(spark.read.table(f"{BRONZE_PREFIX}.{ds_name}"), table_name)
        
        # 2. Rename the raw function FIRST (this works on standard functions)
        dim_func.__name__ = f"dim_{table_name}_wrapper"
        
        # 3. Apply the DLT decorator programmatically to the renamed function
        dlt.table(
            name=f"dim_{table_name}", 
            schema=build_silver_schema(table_name), 
            comment=comment
        )(dim_func)

    def generate_streaming_table(ds_name):
        table_name = ds_name.lower()
        comment = TABLE_DESCRIPTIONS.get(table_name, "Silver Fact Table")
        
        # 1. Define the raw function
        def fact_func():
            return transform_logic(spark.readStream.table(f"{BRONZE_PREFIX}.{ds_name}"), table_name)
            
        # 2. Rename the raw function FIRST
        fact_func.__name__ = f"fact_{table_name}_wrapper"
        
        # 3. Apply the DLT decorator programmatically
        dlt.table(
            name=f"fact_{table_name}", 
            schema=build_silver_schema(table_name), 
            comment=comment
        )(fact_func)

    # Execution loop
    for ds in DIMENSION_TABLES:
        generate_dimension_table(ds)

    for ds in STREAMING_TABLES:
        generate_streaming_table(ds)

create_medallion_pipeline()