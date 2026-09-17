"""
Shared pytest setup.

The suite runs two ways and must behave the same in both:

    pytest                                              (host, CI)
    spark-submit /app/tests/test_transforms.py          (Spark container)

The container route needs no pytest, so `test_transforms.py` keeps its own
runner and its `check()` helper, which records failures instead of raising.
The `checked` fixture below fails the pytest test if any check recorded a
failure, so a soft check is still a hard failure under pytest.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    """One SparkSession for the whole session: each start costs ~5 seconds."""
    from tests.test_transforms import spark_session

    session = spark_session()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def checked(request):
    """Fail the test if check() recorded a failure while it ran."""
    from tests import test_transforms

    before = len(test_transforms.FAILURES)
    yield
    new = test_transforms.FAILURES[before:]
    if new:
        pytest.fail("failed checks: " + "; ".join(new), pytrace=False)
