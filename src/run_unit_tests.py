import pytest
import sys

def main():
    sys.dont_write_bytecode = True

    retcode = pytest.main(["./tests/unit_test", "-v", "-p", "no:cacheprovider"])

    # Fail the cell execution if there are any test failures.
    assert retcode == 0, "The pytest invocation failed. See the log for details."