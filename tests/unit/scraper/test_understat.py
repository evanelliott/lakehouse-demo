import json
from typing import Callable

import pytest

from src.exceptions import IntraBatchDuplicateError, SourceDataMissingError
from src.scraper.understat import UnderstatParser


@pytest.mark.scraper
def test_understat_match_extraction_success(load_fixture: Callable[[str], str]) -> None:
    """
    U-SCR-01: Positive test for raw extraction.
    Ensures the parser extracts the exact expected dictionary from a valid snapshot.
    """
    # Arrange
    html = load_fixture("understat_match_29088.html")
    parser = UnderstatParser()

    # Act
    result = parser.parse_match(html)

    # Assert: Hard-coded expected object representing the specific snapshot data
    expected = {
        "match_id": "29088",
        "home_team": "Manchester United",
        "away_team": "Leeds",
        "home_goals": 1,
        "away_goals": 2,
        "home_xg": 1.36555,
        "away_xg": 2.95118,
        "match_date": "2026-04-13 19:00:00",
    }

    assert result == expected
    assert isinstance(result, dict)


@pytest.mark.scraper
def test_understat_shot_extraction_success(load_fixture: Callable[[str], str]) -> None:
    """
    U-SCR-06: Positive test for secondary API shot extraction.
    Verifies that the parser correctly structures the raw JSON from /getMatchData.
    """
    # Arrange: Load the JSON snapshot you saw in the network tab
    raw_json = load_fixture("understat_rosters_shots_29088.json")
    json_data = json.loads(raw_json)
    parser = UnderstatParser()

    # Act
    result = parser.parse_shots(json_data)

    # Assert: Verify the first shot matches the expected contract
    assert len(result) >= 1
    first_shot = result[0]

    assert first_shot["id"] == "678783"
    assert first_shot["player"] == "Amad Diallo Traore"
    assert first_shot["xg"] == 0.017660638317465782
    assert first_shot["minute"] == 11
    assert "result" in first_shot


@pytest.mark.scraper
def test_understat_roster_extraction_success(load_fixture: Callable[[str], str]) -> None:
    """
    U-SCR-07: Positive test for roster/lineup extraction.
    """
    # Arrange
    raw_json = load_fixture("understat_rosters_shots_29088.json")
    json_data = json.loads(raw_json)
    parser = UnderstatParser()

    # Act
    result = parser.parse_rosters(json_data)

    # Assert
    assert len(result) > 0
    # Find a specific player to verify the data shape
    player = next(p for p in result if p["player_name"] == "Noah Okafor")
    assert player["is_start"] is True
    assert player["minutes"] > 0
    assert player["team_side"] == "a"


@pytest.mark.scraper
def test_understat_league_players_extraction_success(load_fixture: Callable[[str], str]) -> None:
    """
    U-SCR-08: Positive test for league-wide player aggregate extraction.
    Verifies string-to-numeric casting for seasonal stats.
    """
    # Arrange
    raw_json = load_fixture("understat_epl_2025.json")
    json_data = json.loads(raw_json)
    parser = UnderstatParser()

    # Act
    result = parser.parse_league_players(json_data)

    # Assert
    assert len(result) == 2
    haaland = next(p for p in result if p["name"] == "Erling Haaland")

    assert haaland["player_id"] == "8260"
    assert haaland["matches"] == 34
    assert haaland["goals"] == 26
    assert isinstance(haaland["xg"], float)
    assert haaland["xg"] == pytest.approx(28.63835, rel=1e-4)
    assert haaland["team"] == "Manchester City"


@pytest.mark.scraper
def test_understat_dom_contract_change_fails(load_fixture: Callable[[str], str]) -> None:
    """
    U-SCR-03: Negative test for DOM drift.
    Raises SourceDataMissingError if the required <script> tag is missing.
    """
    # Arrange: Use a snapshot specifically modified to lack the data script
    html = load_fixture("understat_missing_script.html")
    parser = UnderstatParser()

    # Act & Assert
    with pytest.raises(SourceDataMissingError) as exc:
        parser.parse_match(html)

    assert "match_info script tag not found" in str(exc.value)


@pytest.mark.scraper
def test_understat_intra_batch_duplicate_shot_fails(load_fixture: Callable[[str], str]) -> None:
    """
    U-SC-05: Negative test for data uniqueness.
    Ensures the parser halts if the JSON source contains duplicate shot IDs.
    """
    # Arrange: Load a JSON fixture specifically crafted with duplicate shot IDs
    raw_json = load_fixture("understat_duplicate_shots.json")
    json_data = json.loads(raw_json)
    parser = UnderstatParser()

    # Act & Assert
    with pytest.raises(IntraBatchDuplicateError) as exc:
        # We now target the specific method responsible for shot logic
        parser.parse_shots(json_data)

    assert "Duplicate shot ID" in str(exc.value)


@pytest.mark.scraper
def test_understat_intra_batch_duplicate_roster_fails(load_fixture: Callable[[str], str]) -> None:
    """
    U-SC-05: Negative test for data uniqueness.
    Ensures the parser halts if the JSON source contains duplicate roster IDs.
    """
    # Arrange: Load a JSON fixture specifically crafted with duplicate roster IDs
    raw_json = load_fixture("understat_duplicate_rosters.json")
    json_data = json.loads(raw_json)
    parser = UnderstatParser()

    # Act & Assert
    with pytest.raises(IntraBatchDuplicateError) as exc:
        # We now target the specific method responsible for shot logic
        parser.parse_rosters(json_data)

    assert "Duplicate roster ID" in str(exc.value)
