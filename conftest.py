"""Ensures the project root (this file's directory) is importable as `engine`, `main`, etc."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


@pytest.fixture(scope="session")
def spark():
    """
    Single SparkSession shared by every test in the run (session-scoped --
    starting/stopping the JVM per test would be far slower than the tests
    themselves). Built with `with_delta=False`: the checks/ tests exercise
    `checks.diff_rows` / `checks.data_quality`, neither of which touches
    Delta, so this skips the Ivy dependency-resolution step entirely
    (see `engine.spark_session.create_spark_session`).
    """
    from engine.spark_session import create_spark_session

    session = create_spark_session(app_name="sdggroup_challenge-tests", with_delta=False)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
