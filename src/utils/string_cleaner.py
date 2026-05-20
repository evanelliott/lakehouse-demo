# src/utils/string_cleaner.py
import re


def clean_string_whitespace(value: str) -> str:
    """
    Normalises string whitespace, strips layout syntax, and trims bounds.

    Fulfills U-UTL-03: Trims trailing line breaks, tabs, and maps HTML
    non-breaking spaces (\u00a0) to clean spaces before running regex normalisation.
    """
    if not value:
        return ""

    # Replace HTML non-breaking spaces (\u00a0) with regular spaces
    cleaned: str = value.replace("\u00a0", " ")

    # Map multiple consecutive whitespace variants (tabs, newlines, spaces) to a single space
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Perform terminal bounds stripping
    return cleaned.strip()
