# src/transforms/entity_resolution.py
from pyspark.sql import DataFrame

from src.exceptions import UnresolvedEntityError


def _resolve_generic_entities(
    input_df: DataFrame,
    lookup_df: DataFrame,
    input_key: str,
    lookup_key: str,
) -> DataFrame:
    """
    Internal helper to validate and resolve master entities.

    Raises an UnresolvedEntityError pointing to the first invalid key if any incoming
    record cannot be matched. Drops the raw source key column upon successful mapping.
    """
    # 1. Detect missing master reference keys via a structural DataFrame subtraction
    unresolved_df = input_df.select(input_key).subtract(lookup_df.select(lookup_key))

    if not unresolved_df.isEmpty():
        culprit_row = unresolved_df.first()
        culprit_name = culprit_row[input_key] if culprit_row else "Unknown"
        raise UnresolvedEntityError(f"Unresolved entity detected: {culprit_name}")

    # 2. Execute the strict equality inner join mapping
    resolved_df = input_df.join(lookup_df, input_df[input_key] == lookup_df[lookup_key], "inner")

    # 3. Clean and isolate the output schema by purging the raw unstandardised input key
    return resolved_df.drop(input_key)


def resolve_team_entities(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """
    Resolves incoming raw team names against a Golden Reference team lookup DataFrame.
    """
    return _resolve_generic_entities(
        input_df=input_df,
        lookup_df=lookup_df,
        input_key="team_name",
        lookup_key="raw_name",
    )


def resolve_player_entities(input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """
    Resolves incoming raw player names against a Golden Reference player lookup DataFrame.
    """
    return _resolve_generic_entities(
        input_df=input_df,
        lookup_df=lookup_df,
        input_key="player_name",
        lookup_key="raw_player_name",
    )
