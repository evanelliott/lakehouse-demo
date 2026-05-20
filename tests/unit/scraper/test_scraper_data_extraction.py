# tests/unit/scraper/test_scraper_data_extraction.py
import json
from typing import TYPE_CHECKING, Any, Dict, cast

import pytest
from pyspark.sql import Row

from src.exceptions import SelectorNotFoundError, SourceDataMissingError
from src.scrapers.parsers import parse_premier_injuries_raw, parse_understat_raw

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Path coordinates for our offline local file snapshots
FIXTURE_DIR = "tests/unit/scraper/fixtures"


def _get_mutable_row_reference(payload_block: Any) -> Dict[str, Any]:
    """
    Internal helper to dynamically extract a mutable dictionary reference.

    Traverses flat lists, dictionary layers, and home/away (h/a) blocks that
    can be either lists (shots) or dictionaries (rosters).
    """
    if isinstance(payload_block, dict):
        if "h" in payload_block:  # Handles "rosters" and "shots" split structures
            side_data = payload_block["h"]
            if isinstance(side_data, list) and len(side_data) > 0:
                # Grab the first shot dict item out of the list array safely
                row_ref_list: Dict[str, Any] = side_data[0]
                return row_ref_list
            elif isinstance(side_data, dict):
                first_key = list(side_data.keys())[0]
                row_ref_dict: Dict[str, Any] = side_data[first_key]
                return row_ref_dict

        # FIX: Extract index [0] to pass a string key instead of the full list object
        keys_list = list(payload_block.keys())
        if keys_list:
            first_team_id = keys_list[0]
            row_ref_team: Dict[str, Any] = payload_block[first_team_id]
            return row_ref_team

    elif isinstance(payload_block, list) and len(payload_block) > 0:
        # FIX: Grab the first record dict element out of flat datasets (players, dates)
        row_ref_flat: Dict[str, Any] = payload_block[0]
        return row_ref_flat

    return cast(Dict[str, Any], payload_block)


# =========================================================================
# HAPPY PATH EXTRACTION GATEWAY (U-EXT-01, U-EXT-02, U-EXT-07)
# =========================================================================


@pytest.mark.ext
def test_understat_league_extraction_success(spark: "SparkSession") -> None:
    """U-EXT-01: Confirms JSON extraction maps cleanly to Teams, Players, and Dates."""

    with open(f"{FIXTURE_DIR}/raw_league_response.json", "r") as f:
        payload: Dict[str, Any] = json.load(f)

    for dataset in ["teams", "players", "dates"]:
        results = parse_understat_raw(payload, target_dataset=dataset)
        assert len(results) == 2

        # FIX: Convert dict rows to PySpark Row objects to satisfy RowLike generic bounds
        row_data = [Row(**record) for record in results]
        df = spark.createDataFrame(row_data)
        assert df.count() == len(results)


@pytest.mark.ext
def test_understat_match_extraction_success(spark: "SparkSession") -> None:
    """U-EXT-02: Confirms JSON extraction maps cleanly to Rosters and Shots."""

    with open(f"{FIXTURE_DIR}/raw_match_response.json", "r") as f:
        payload: Dict[str, Any] = json.load(f)

    for dataset in ["rosters", "shots"]:
        results = parse_understat_raw(payload, target_dataset=dataset)
        assert len(results) > 0

        # FIX: Convert dict rows to PySpark Row objects to satisfy RowLike generic bounds
        row_data = [Row(**record) for record in results]
        df = spark.createDataFrame(row_data)
        assert df.count() == len(results)


@pytest.mark.ext
def test_premier_injuries_grid_ingestion_success(spark: "SparkSession") -> None:
    """U-EXT-07: Confirms HTML DOM rows convert into valid injury dictionaries."""

    with open(f"{FIXTURE_DIR}/injury_table.html", "r") as f:
        html_content = f.read()

    results = parse_premier_injuries_raw(html_content)
    assert len(results) == 2

    row_data = [Row(**record) for record in results]
    df = spark.createDataFrame(row_data)

    assert df.filter(df["id"] == 1164).collect()[0]["title"] == "50%"


# =========================================================================
# UNIVERSAL PARAMETERIZED EXTRACTION CONTRACT GATES (U-EXT-03 to U-EXT-06)
# =========================================================================


@pytest.mark.ext
@pytest.mark.parametrize(
    "source_type, fixture_file, target_key",
    [
        ("json", "raw_league_response.json", "teams"),
        ("json", "raw_league_response.json", "players"),
        ("json", "raw_league_response.json", "dates"),
        ("json", "raw_match_response.json", "rosters"),
        ("json", "raw_match_response.json", "shots"),
        ("html", "injury_table.html", "injury-table injury-table-full injury-table-epl"),
    ],
)
def test_universal_missing_id_key_contract(
    source_type: str, fixture_file: str, target_key: str
) -> None:
    """U-EXT-03: Missing core envelope root keys / signatures triggers a hard fail."""
    with open(f"{FIXTURE_DIR}/{fixture_file}", "r") as f:
        raw_content = f.read()

    if source_type == "json":
        payload = json.loads(raw_content)
        del payload[target_key]
        with pytest.raises(SourceDataMissingError):
            parse_understat_raw(payload, target_dataset=target_key)
    else:
        corrupted_html = raw_content.replace(target_key, 'class="corrupted-drift')
        with pytest.raises(SelectorNotFoundError):  # Maps to U-EXT-08 layout drift
            parse_premier_injuries_raw(corrupted_html)


@pytest.mark.ext
@pytest.mark.parametrize(
    "source_type, fixture_file, target_key",
    [
        ("json", "raw_league_response.json", "teams"),
        ("json", "raw_league_response.json", "players"),
        ("json", "raw_league_response.json", "dates"),
        ("json", "raw_match_response.json", "rosters"),
        ("json", "raw_match_response.json", "shots"),
        ("html", "injury_table.html", "injury_table.html"),
    ],
)
def test_universal_missing_fields_contract(
    source_type: str, fixture_file: str, target_key: str
) -> None:
    """U-EXT-04: Missing inner row schema elements drops batch, throwing ValueError."""
    with open(f"{FIXTURE_DIR}/{fixture_file}", "r") as f:
        raw_content = f.read()

    if source_type == "json":
        payload = json.loads(raw_content)
        # Dynamically locate the inner record map to clear target keys
        target_row = _get_mutable_row_reference(payload[target_key])
        target_row.clear()  # Wipes row parameters to trigger a validation alert

        with pytest.raises(ValueError):
            parse_understat_raw(payload, target_dataset=target_key)
    else:
        corrupted_html = raw_content.replace('data-id="1164"', "")
        with pytest.raises(ValueError):
            parse_premier_injuries_raw(corrupted_html)


@pytest.mark.ext
@pytest.mark.parametrize(
    "source_type, fixture_file, target_key",
    [
        ("json", "raw_league_response.json", "teams"),
        ("json", "raw_league_response.json", "players"),
        ("json", "raw_league_response.json", "dates"),
        ("json", "raw_match_response.json", "rosters"),
        ("json", "raw_match_response.json", "shots"),
        ("html", "injury_table.html", "injury_table.html"),
    ],
)
def test_universal_empty_payload_contract(
    source_type: str, fixture_file: str, target_key: str
) -> None:
    """U-EXT-05: Empty payload record arrays across any data set raises a ValueError."""
    if source_type == "json":
        empty_payload: Dict[str, Any] = {target_key: []}
        with pytest.raises(ValueError):
            parse_understat_raw(empty_payload, target_dataset=target_key)
    else:
        with open(f"{FIXTURE_DIR}/{fixture_file}", "r") as f:
            html = f.read()
        # Strip out player rows to mock an empty table grid response
        empty_html = html.replace('class="player-row', 'class="no-rows')
        with pytest.raises(ValueError):
            parse_premier_injuries_raw(empty_html)


@pytest.mark.ext
@pytest.mark.parametrize(
    "source_type, fixture_file, target_key",
    [
        ("json", "raw_league_response.json", "teams"),
        ("json", "raw_league_response.json", "players"),
        ("json", "raw_league_response.json", "dates"),
        ("json", "raw_match_response.json", "rosters"),
        ("json", "raw_match_response.json", "shots"),
        ("html", "injury_table.html", "injury_table.html"),
    ],
)
def test_universal_schema_data_type_safety_contract(
    source_type: str, fixture_file: str, target_key: str
) -> None:
    """U-EXT-06: Mismatched, uncastable column types drop data batch, raising a TypeError."""
    with open(f"{FIXTURE_DIR}/{fixture_file}", "r") as f:
        raw_content = f.read()

    if source_type == "json":
        payload = json.loads(raw_content)
        target_row = _get_mutable_row_reference(payload[target_key])

        # Determine current unique ID key signature to overwrite with uncastable type
        pk_key = "id" if "id" in target_row else "player_id"
        target_row[pk_key] = ["uncastable_array_signature"]

        with pytest.raises(TypeError):
            parse_understat_raw(payload, target_dataset=target_key)
    else:
        corrupted_html = raw_content.replace('data-id="1164"', 'data-id="STRING_CEILING"')
        with pytest.raises(TypeError):
            parse_premier_injuries_raw(corrupted_html)


# =========================================================================
# CENTRAL STRING SANITISATION TEST GATEWAY (U-EXT-11)
# =========================================================================


@pytest.mark.ext
@pytest.mark.parametrize(
    "dirty_string, expected_clean",
    [
        ("  Bukayo Saka  ", "Bukayo Saka"),
        ("\nMartin Odegaard\r\n", "Martin Odegaard"),
        ("Kevin\u00a0De\u00a0Bruyne", "Kevin De Bruyne"),  # Cleans non-breaking space variants
        ("\tErling Haaland\n", "Erling Haaland"),
    ],
)
def test_extraction_string_preprocessing_sanitisation(
    dirty_string: str, expected_clean: str
) -> None:
    """U-EXT-11: Asserts the central string utility normalises whitespace bounds."""
    from src.utils.string_cleaner import clean_string_whitespace

    assert clean_string_whitespace(dirty_string) == expected_clean
