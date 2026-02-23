# tests/test_suite.py
import unittest
from pyspark.sql import Row, SparkSession
from src_logic import transform_logic, standardize_header

class ZillowDLTTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()

    def test_standardize_header_logic(self):
        """Verify Python standardization logic handles special chars"""
        self.assertEqual(standardize_header("City-Name!"), "cityname")
        self.assertEqual(standardize_header("Median Price ($)"), "medianprice")

    def test_business_rules_currency_and_date(self):
        """Verify Pandas UDF rounds currency and dates are standardized"""
        # Note: input columns are "dirty" (spaces/caps)
        data = [Row(**{"Date": "2026-01-01", "Sale Prices": 500250.6685})]
        mock_df = self.spark.createDataFrame(data)
        
        result_df = transform_logic(mock_df, "city_time_series")
        row = result_df.collect()[0]
        
        # Verify Currency Normalization (Rounded to 2 decimals)
        # column name matches key in SCHEMA_MAPPINGS: 'sale_prices'
        self.assertEqual(row["sale_prices"], 500250.67)
        self.assertEqual(str(row["date"]), "2026-01-01")

    def test_missing_column_defaults(self):
        """Ensure missing numeric columns in Bronze default to 0.0 in Silver"""
        mock_df = self.spark.createDataFrame([Row(date="2026-02-15")])
        result = transform_logic(mock_df, "city_time_series")
        
        # FIX: Access using 'zhvi_allhomes' (with underscore) to match schema_config.py
        val = result.select("zhvi_allhomes").collect()[0][0]
        self.assertEqual(val, 0.0)