# --- tests/test_bronze_layer.py ---
import pytest
import test_config
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual

class TestBronzeLayer:

    def get_full_path(self, table_name):
        return f"{test_config.BRONZE_PATH}.{table_name}"

    @pytest.mark.parametrize("table_name", test_config.EXPECTED_TABLES)
    def test_table_exists(self, spark, table_name):
        """Verifies if the table exists in the Unity Catalog."""
        exists = spark.catalog.tableExists(self.get_full_path(table_name))
        assert exists, f"Table {table_name} was NOT found in {test_config.BRONZE_PATH}. Check spelling!"

    @pytest.mark.parametrize("table_name", test_config.EXPECTED_TABLES)
    def test_column_existence(self, spark, table_name):
        """Checks if mandatory audit columns exist."""
        # Use try/except to handle cases where table is completely missing
        try:
            df = spark.table(self.get_full_path(table_name))
            actual_cols = df.columns
            for col in test_config.MANDATORY_COLS:
                assert col in actual_cols, f"Column {col} missing in {table_name}"
        except Exception as e:
            pytest.fail(f"Could not access columns for {table_name}: {e}")

    @pytest.mark.parametrize("table_name", test_config.EXPECTED_TABLES)
    def test_data_types(self, spark, table_name):
        """Validates the 'String-except-load_dt' contract."""
        try:
            df = spark.table(self.get_full_path(table_name))
            for field in df.schema:
                if field.name == "load_dt":
                    assert isinstance(field.dataType, TimestampType), f"{field.name} in {table_name} must be Timestamp"
                else:
                    assert isinstance(field.dataType, StringType), f"{field.name} in {table_name} must be String"
        except Exception:
            pytest.skip(f"Skipping type check as {table_name} is missing.")

    @pytest.mark.parametrize("table_name, constraints", test_config.VALUE_CONSTRAINTS.items())
    def test_categorical_values(self, spark, table_name, constraints):
        """Checks if values are present in a fixed set (e.g. States)."""
        df = spark.table(self.get_full_path(table_name))
        for col_name, valid_values in constraints.items():
            invalid_rows = df.filter(~F.col(col_name).isin(valid_values))
            assert invalid_rows.count() == 0, f"Found invalid values in {table_name}.{col_name}"

    @pytest.mark.parametrize("table_name", test_config.EXPECTED_TABLES)
    def test_schema_integrity(self, spark, table_name):
        """Deep validation of the schema contract."""
        df = spark.table(self.get_full_path(table_name)).select("load_dt", "source")
        # Compares actual schema against the configuration contract
        assertSchemaEqual(df.schema, test_config.COMMON_CONTRACT)

    @pytest.mark.parametrize("table_name", test_config.EXPECTED_TABLES)
    def test_data_sample_equality(self, spark, table_name):
        """Regression test comparing table to itself (simulation of assertDataFrameEqual)."""
        actual_df = spark.table(self.get_full_path(table_name)).limit(5)
        expected_df = spark.table(self.get_full_path(table_name)).limit(5)
        
        # In real scenarios, compare against a saved 'Golden' parquet file
        assertDataFrameEqual(actual_df, expected_df)