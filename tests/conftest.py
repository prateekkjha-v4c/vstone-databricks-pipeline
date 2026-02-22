# --- tests/conftest.py ---
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Initializes Spark Session for the testing suite."""
    return SparkSession.builder.getOrCreate()