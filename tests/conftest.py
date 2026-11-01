"""Shared test setup: import paths and a local SparkSession."""

import os
import sys

import pytest

TESTS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TESTS)
for folder in (os.path.join(ROOT, "producer"), os.path.join(ROOT, "spark"),
               os.path.join(ROOT, "dashboard"), TESTS):
    sys.path.insert(0, folder)


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession shared by all tests."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]").appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
