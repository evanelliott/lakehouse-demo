# tests/unit/scraper/test_scraper_network_handshake.py
from typing import TYPE_CHECKING, Dict

import pytest

from src.exceptions import (
    InfrastructureTimeoutError,
    ScraperBlockedException,
    SecurityContractException,
)
from src.scrapers.network_client import (
    execute_premier_injuries_get,
    execute_understat_ajax_call,
)

if TYPE_CHECKING:
    import requests

# Static header dictionary definitions matching your cURL blueprints
MOCK_HEADERS: Dict[str, str] = {
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15",
}


# =========================================================================
# UNDERSTAT HANDSHAKE LAYER (U-NET-01 to U-NET-05)
# =========================================================================


@pytest.mark.net
@pytest.mark.vcr
@pytest.mark.parametrize(
    "landing_url, data_url",
    [
        # League Data Flow Coordinates
        ("https://understat.com/league/EPL/2025", "https://understat.com/getLeagueData/EPL/2025"),
        # Match Data Flow Coordinates (Your newly discovered pair)
        ("https://understat.com/match/29088", "https://understat.com/getMatchData/29088"),
    ],
)
def test_understat_handshake_success(landing_url: str, data_url: str) -> None:
    """
    U-NET-01: Asserts the 2-step HTTP session handshake acquires active
    cookies and extracts a 200 OK JSON payload successfully.
    """
    response: "requests.Response" = execute_understat_ajax_call(
        landing_url=landing_url, data_url=data_url, headers=MOCK_HEADERS
    )

    assert response.status_code == 200
    content_type = response.headers.get("Content-Type", "").lower()
    assert "application/json" in content_type or "text/javascript" in content_type


@pytest.mark.net
@pytest.mark.vcr
@pytest.mark.parametrize(
    "landing_url, data_url",
    [
        ("https://understat.com/league/EPL/2025", "https://understat.com/getLeagueData/EPL/2025"),
        ("https://understat.com/match/29088", "https://understat.com/getMatchData/29088"),
    ],
)
def test_understat_session_rejected(landing_url: str, data_url: str) -> None:
    """
    U-NET-02: Verifies that an invalid, expired, or missing cookie context
    is intercepted, dropping the payload and raising a SecurityContractException.
    """
    malformed_cookies: Dict[str, str] = {"PHPSESSID": "EXPIRED_OR_MALFORMED_TOKEN"}

    with pytest.raises(SecurityContractException) as exc_info:
        execute_understat_ajax_call(
            landing_url=landing_url,
            data_url=data_url,
            headers=MOCK_HEADERS,
            cookies=malformed_cookies,
        )

    assert "Session credentials rejected" in str(exc_info.value)


@pytest.mark.net
@pytest.mark.vcr
@pytest.mark.parametrize(
    "landing_url, data_url",
    [
        ("https://understat.com/league/EPL/2025", "https://understat.com/getLeagueData/EPL/2025"),
        ("https://understat.com/match/29088", "https://understat.com/getMatchData/29088"),
    ],
)
def test_understat_header_drop_forbidden(landing_url: str, data_url: str) -> None:
    """
    U-NET-03: Verifies that omitting the mandatory 'X-Requested-With'
    header catches a 403 Forbidden status code and raises a clear exception.
    """
    broken_headers = MOCK_HEADERS.copy()
    del broken_headers["X-Requested-With"]

    with pytest.raises(ScraperBlockedException) as exc_info:
        execute_understat_ajax_call(
            landing_url=landing_url, data_url=data_url, headers=broken_headers
        )

    assert "403" in str(exc_info.value) or "Forbidden" in str(exc_info.value)


@pytest.mark.net
@pytest.mark.vcr
@pytest.mark.parametrize(
    "landing_url, data_url",
    [
        ("https://understat.com/league/EPL/2025", "https://understat.com/getLeagueData/EPL/2025"),
        ("https://understat.com/match/29088", "https://understat.com/getMatchData/29088"),
    ],
)
def test_understat_rate_limited(landing_url: str, data_url: str) -> None:
    """
    U-NET-04: Verifies that encountering a WAF/429 Rate Limit response
    rejects the corrupted payload data footprint and raises a ScraperBlockedException.
    """
    with pytest.raises(ScraperBlockedException) as exc_info:
        execute_understat_ajax_call(
            landing_url=landing_url,
            data_url=data_url,
            headers=MOCK_HEADERS,
            force_status_mock=429,
        )

    assert "Rate limit hit" in str(exc_info.value) or "429" in str(exc_info.value)


@pytest.mark.net
@pytest.mark.vcr
@pytest.mark.parametrize(
    "landing_url, data_url",
    [
        ("https://understat.com/league/EPL/2025", "https://understat.com/getLeagueData/EPL/2025"),
        ("https://understat.com/match/29088", "https://understat.com/getMatchData/29088"),
    ],
)
def test_understat_server_outage_timeout(landing_url: str, data_url: str) -> None:
    """
    U-NET-05: Verifies that connection timeouts or 5xx server drops are
    caught cleanly and raise an explicit InfrastructureTimeoutError.
    """
    with pytest.raises(InfrastructureTimeoutError) as exc_info:
        execute_understat_ajax_call(
            landing_url=landing_url,
            data_url=data_url,
            headers=MOCK_HEADERS,
            force_status_mock=503,
        )

    # FIX: Assert the status code integer string directly to bypass case drift
    assert "503" in str(exc_info.value)


# =========================================================================
# PREMIER INJURIES HANDSHAKE LAYER (U-NET-06 to U-NET-09)
# =========================================================================


@pytest.mark.net
@pytest.mark.vcr
def test_premier_injuries_handshake_success() -> None:
    """
    U-NET-06: Asserts a standard GET request to Premier Injuries returns
    a 200 OK containing valid text/html payload properties.
    """
    response = execute_premier_injuries_get(
        url="https://www.premierinjuries.com/injury-table.php", headers=MOCK_HEADERS
    )

    assert response.status_code == 200
    assert "text/html" in response.headers.get("Content-Type", "").lower()


@pytest.mark.net
@pytest.mark.vcr
def test_premier_injuries_header_drop_forbidden() -> None:
    """
    U-NET-07: Verifies that omitting browser User-Agent properties catches
    a 403 Forbidden firewall response and raises a clear exception.
    """
    broken_headers: Dict[str, str] = {"User-Agent": "Python-requests/2.31.0"}

    with pytest.raises(ScraperBlockedException) as exc_info:
        execute_premier_injuries_get(
            url="https://www.premierinjuries.com/injury-table.php", headers=broken_headers
        )

    assert "403" in str(exc_info.value) or "Blocked by WAF" in str(exc_info.value)


@pytest.mark.net
@pytest.mark.vcr
def test_premier_injuries_rate_limited() -> None:
    """
    U-NET-08: Verifies that a 429 Too Many Requests response is caught
    cleanly, rejecting the empty payload and raising a ScraperBlockedException.
    """
    with pytest.raises(ScraperBlockedException) as exc_info:
        execute_premier_injuries_get(
            url="https://www.premierinjuries.com/injury-table.php",
            headers=MOCK_HEADERS,
            force_status_mock=429,
        )

    assert "429" in str(exc_info.value)


@pytest.mark.net
@pytest.mark.vcr
def test_premier_injuries_server_outage_timeout() -> None:
    """
    U-NET-09: Verifies that connection failures, gateway drops, or timeouts
    are caught gracefully and raise an InfrastructureTimeoutError.
    """
    with pytest.raises(InfrastructureTimeoutError) as exc_info:
        execute_premier_injuries_get(
            url="https://www.premierinjuries.com/injury-table.php",
            headers=MOCK_HEADERS,
            force_status_mock=502,
        )

    # FIX: Assert the status code integer string directly to bypass case drift
    assert "502" in str(exc_info.value)
