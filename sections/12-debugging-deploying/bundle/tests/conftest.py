"""One Spark session for the whole test run.

On Databricks, getOrCreate returns the session that is already there. On a CI runner with
pyspark and a JVM installed, it starts a local one. The tests never know the difference.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()
