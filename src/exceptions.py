# src/exceptions.py


class LakehouseError(Exception):
    """Base exception for all Premier League Lakehouse data pipeline failures."""

    pass


# =========================================================================
# NETWORK & HANDSHAKE EXCEPTIONS (U-NET)
# =========================================================================


class SecurityContractException(LakehouseError):
    """Raised when session tokens or authentication handshakes are expired or rejected."""

    pass


class ScraperBlockedException(LakehouseError):
    """Raised when an endpoint triggers a WAF intervention, 403 Forbidden, or 429 Rate Limit."""

    pass


class InfrastructureTimeoutError(LakehouseError):
    """Raised when a remote server outage, 5xx error, or connection timeout occurs."""

    pass


# =========================================================================
# INGESTION & DATA QUALITY EXCEPTIONS (U-EXT / U-UTL)
# =========================================================================


class IntraBatchDuplicateError(LakehouseError):
    """Raised when duplicate records are found within a single scrape/batch."""

    pass


class SourceDataMissingError(LakehouseError):
    """Raised when the expected data script/tag/envelope is missing from the source payload."""

    pass


class SelectorNotFoundError(LakehouseError):
    """Raised when a specific CSS/HTML selector or table grid cannot be found in the DOM."""

    pass


class FutureDateError(LakehouseError):
    """Raised when a record has an effective or processing date in the future."""

    pass


# =========================================================================
# WAREHOUSE TRANSFORMATION EXCEPTIONS (SCD2 / ER)
# =========================================================================


class SCD2TimelineException(LakehouseError):
    """Raised when a new record contradicts or overlaps the existing temporal history."""

    pass


class UnresolvedEntityError(LakehouseError):
    """Raised when a raw team or player name cannot be mapped to an internal Golden ID."""

    pass
