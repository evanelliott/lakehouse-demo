# tests/unit/scraper/test_scraper_utils.py
from typing import Any, Dict, List

import pytest

from src.exceptions import IntraBatchDuplicateError
from src.utils.string_cleaner import clean_string_whitespace
from src.utils.validation import process_batch_duplicates

# =========================================================================
# UNIVERSAL STRING SANITISATION ENGINE TESTS (U-UTL-03)
# =========================================================================


@pytest.mark.utl
@pytest.mark.parametrize(
    "dirty_input, expected_clean",
    [
        ("  Bukayo Saka  ", "Bukayo Saka"),
        ("\nMartin Odegaard\r\n", "Martin Odegaard"),
        ("Kevin\u00a0De\u00a0Bruyne", "Kevin De Bruyne"),  # HTML non-breaking spaces (\u00a0)
        ("\tErling Haaland\n", "Erling Haaland"),
        ("   ", ""),  # Complete whitespace reduction
    ],
)
def test_universal_string_sanitisation(dirty_input: str, expected_clean: str) -> None:
    """
    U-UTL-03: Asserts the central string utility cleanly trims trailing line
    breaks, hidden tabs, and normalises HTML non-breaking space variants.
    """
    assert clean_string_whitespace(dirty_input) == expected_clean


# =========================================================================
# UNIVERSAL DEDUPLICATION MATRIX & COLLISION GATES (U-UTL-01 & U-UTL-02)
# =========================================================================


@pytest.mark.utl
@pytest.mark.parametrize(
    "mock_batch, expected_count, expect_exception",
    [
        # Scenario 1: Symmetrical unique rows across any arbitrary dataset
        (
            [{"id": 1, "name": "Arsenal"}, {"id": 2, "name": "Man City"}],
            2,
            None,
        ),
        # Scenario 2: U-UTL-01 - Full Identity Overlap (Deduplicates seamlessly without error)
        (
            [
                {"id": 1, "name": "Arsenal"},
                {"id": 1, "name": "Arsenal"},  # Exact copy match
            ],
            1,
            None,
        ),
        # Scenario 3: U-UTL-02 - Partial PK Collision (Different value fields - Hard Crash)
        (
            [
                {"id": 1, "name": "Arsenal"},
                {"id": 1, "name": "Gooners FC"},  # Clashing attribute profile
            ],
            0,
            IntraBatchDuplicateError,
        ),
        # Scenario 4: Boundary evaluation - Multiple full overlaps mixed with unique entries
        (
            [
                {"id": 10, "player": "Saka"},
                {"id": 10, "player": "Saka"},
                {"id": 11, "player": "Saliba"},
            ],
            2,
            None,
        ),
    ],
)
def test_global_deduplication_utility_matrix(
    mock_batch: List[Dict[str, Any]], expected_count: int, expect_exception: Any
) -> None:
    """
    U-UTL-01 & U-UTL-02: Verifies universal dictionary deduplication and primary
    key collision tracking mechanics using parameterised layout graphs.
    """
    if expect_exception:
        with pytest.raises(expect_exception) as exc_info:
            process_batch_duplicates(mock_batch, primary_key="id")

        assert "PK Collision" in str(exc_info.value) or "Collision" in str(exc_info.value)
    else:
        result = process_batch_duplicates(mock_batch, primary_key="id")
        assert len(result) == expected_count
