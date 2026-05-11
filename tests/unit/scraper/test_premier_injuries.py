from typing import Callable

import pytest

from src.exceptions import IntraBatchDuplicateError, SelectorNotFoundError
from src.scraper.premier_injuries import PremierInjuriesParser


@pytest.mark.scraper
def test_premier_injuries_extraction_success(load_fixture: Callable[[str], str]) -> None:
    """
    U-SCR-02: Positive test for raw table extraction.
    Verifies full 7-column schema and text cleaning logic.
    """
    # Arrange
    html = load_fixture("premier_injuries_west_ham.html")
    parser = PremierInjuriesParser()

    # Act
    result = parser.parse_team_injuries(html)

    # Assert
    expected = [
        {
            "condition": "Currently Being Assessed",
            "further_detail": "Tight Quadriceps",
            "player_name": "Adama Traore Diarra",
            "potential_return": "17/05/2026",
            "reason": "Thigh Injury",
            "status": "50%",
            "team": "West Ham United",
        },
        {
            "team": "West Ham United",
            "player_name": "Lukasz Fabianski",
            "reason": "Lower Back Injury",
            "further_detail": "Back Injury",
            "potential_return": "No Return Date",
            "condition": "Not Available",
            "status": "Ruled Out",
        },
    ]

    assert result == expected
    assert isinstance(result, list)


@pytest.mark.scraper
def test_premier_injuries_dom_contract_change_fails(
    load_fixture: Callable[[str], str],
) -> None:
    """
    U-SCR-04: Negative test for DOM drift.
    """
    html = load_fixture("premier_injuries_missing_table.html")
    parser = PremierInjuriesParser()

    with pytest.raises(SelectorNotFoundError) as exc:
        parser.parse_team_injuries(html)

    assert "Injury table not found" in str(exc.value)


@pytest.mark.scraper
def test_premier_injuries_intra_batch_duplicate_fails(
    load_fixture: Callable[[str], str],
) -> None:
    """
    U-SC-06: Negative test for data uniqueness.
    """
    html = load_fixture("premier_injuries_duplicate_player.html")
    parser = PremierInjuriesParser()

    with pytest.raises(IntraBatchDuplicateError) as exc:
        parser.parse_team_injuries(html)

    assert "Duplicate player detected" in str(exc.value)
