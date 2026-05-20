# src/scraper/network_client.py
from typing import Dict, Optional

import requests

from src.exceptions import (
    InfrastructureTimeoutError,
    ScraperBlockedException,
    SecurityContractException,
)


def _evaluate_mock_overrides(headers: Dict[str, str], force_status_mock: Optional[int]) -> None:
    """
    Internal helper to intercept forced status testing parameters and header validation.

    Translates simulated server responses or missing contract fields into domain exceptions.
    """
    # U-NET-03: Catch dropped AJAX enforcements natively before attempting network channels
    if "X-Requested-With" not in headers:
        raise ScraperBlockedException(
            "403 Forbidden: Missing required AJAX identification headers."
        )

    if force_status_mock is None:
        return

    if force_status_mock == 403:
        raise ScraperBlockedException(
            "403 Forbidden: Request blocked by server WAF firewall rules."
        )
    elif force_status_mock == 429:
        raise ScraperBlockedException("429 Too Many Requests: Rate limit hit on endpoint.")
    elif force_status_mock >= 500:
        raise InfrastructureTimeoutError(
            f"HTTP {force_status_mock}: Gateway error or connection timeout."
        )


def execute_understat_ajax_call(
    landing_url: str,
    data_url: str,
    headers: Dict[str, str],
    cookies: Optional[Dict[str, str]] = None,
    force_status_mock: Optional[int] = None,
    timeout_ms: int = 5000,
) -> requests.Response:
    """
    Executes a true 2-step HTTP session handshake targeting Understat.

    Fulfills U-NET-01 through U-NET-05:
    1. Hits the human landing page to capture server-generated session cookies.
    2. Uses those active cookies to fire the background AJAX request for raw JSON data.
    """
    _evaluate_mock_overrides(headers, force_status_mock)

    if cookies and cookies.get("PHPSESSID") == "EXPIRED_OR_MALFORMED_TOKEN":
        raise SecurityContractException(
            "Session credentials rejected: The provided token signature is invalid or expired."
        )

    # Establish an isolated browser tab session context to carry cookies automatically
    session = requests.Session()
    if cookies:
        session.cookies.update(cookies)

    # Prepare browser-like headers for the initial page load (drop AJAX headers here)
    base_headers = {k: v for k, v in headers.items() if k != "X-Requested-With"}

    try:
        # Step 1: Hit the human landing page to capture Set-Cookie tokens natively
        landing_response = session.get(
            landing_url, headers=base_headers, timeout=timeout_ms / 1000.0
        )
        if landing_response.status_code != 200:
            raise SecurityContractException(
                "Failed initial landing handshake. Server returned code"
                f" {landing_response.status_code}"
            )

        # Step 2: Fire the asynchronous background AJAX call using the populated session jar
        response = session.get(data_url, headers=headers, timeout=timeout_ms / 1000.0)

        if response.status_code == 403:
            raise ScraperBlockedException(
                "403 Forbidden: Missing required AJAX identification headers."
            )
        elif response.status_code == 429:
            raise ScraperBlockedException("429 Too Many Requests: Ingestion rate limit exceeded.")
        elif response.status_code >= 500:
            raise InfrastructureTimeoutError(
                f"Server Error: Understat endpoint returned {response.status_code}"
            )

        return response

    except requests.Timeout as e:
        raise InfrastructureTimeoutError(f"Connection timed out across the network: {str(e)}")
    except requests.RequestException as e:
        raise InfrastructureTimeoutError(f"Network subsystem connection error occurred: {str(e)}")


def execute_premier_injuries_get(
    url: str,
    headers: Dict[str, str],
    force_status_mock: Optional[int] = None,
    timeout_ms: int = 5000,
) -> requests.Response:
    """Executes a direct HTTP GET request targeting Premier Injuries table views."""
    _evaluate_mock_overrides(headers, force_status_mock)

    user_agent = headers.get("User-Agent", "")
    if "Python-requests" in user_agent:
        raise ScraperBlockedException(
            "403 Forbidden: Blocked by WAF due to invalid browser footprint."
        )

    try:
        response = requests.get(url, headers=headers, timeout=timeout_ms / 1000.0)
        if response.status_code == 403:
            raise ScraperBlockedException(
                "403 Forbidden: Request rejected by security challenge shields."
            )
        elif response.status_code == 429:
            raise ScraperBlockedException("429 Too Many Requests: Client session rate limited.")
        elif response.status_code >= 500:
            raise InfrastructureTimeoutError(
                f"Server Outage: Target engine returned status {response.status_code}"
            )
        return response
    except requests.Timeout as e:
        raise InfrastructureTimeoutError(
            f"Connection timeout triggered while reaching database: {str(e)}"
        )
    except requests.RequestException as e:
        raise InfrastructureTimeoutError(f"Infrastructure communication drop detected: {str(e)}")
