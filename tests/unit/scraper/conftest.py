import os
from typing import Callable

import pytest


@pytest.fixture(scope="package")
def load_fixture() -> Callable[[str], str]:
    """
    Utility to load HTML snapshots from the fixtures/ folder.
    Usage: load_fixture("understat_match_789.html")
    """

    def _loader(filename: str) -> str:
        # Resolve path relative to this conftest file
        base_path = os.path.join(os.path.dirname(__file__), "fixtures")
        full_path = os.path.join(base_path, filename)

        if not os.path.exists(full_path):
            pytest.fail(f"Fixture not found: {full_path}")

        with open(full_path, "r", encoding="utf-8") as f:
            return f.read()

    return _loader
