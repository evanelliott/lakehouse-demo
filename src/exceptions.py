class IntraBatchDuplicateError(Exception):
    """Raised when duplicate records are found within a single scrape/batch."""

    pass


class SourceDataMissingError(Exception):
    """Raised when the expected data script/tag is missing from the source HTML."""

    pass


class SelectorNotFoundError(Exception):
    """Raised when a specific CSS/HTML selector cannot be found in the DOM."""

    pass


class FutureDateError(Exception):
    """Raised when a record has an effective date in the future."""

    pass


class SCD2TimelineException(Exception):
    """Raised when a new record contradicts the existing temporal history."""

    pass


class UnresolvedEntityError(Exception):
    """Raised when a raw name cannot be mapped to a Golden ID."""

    pass
