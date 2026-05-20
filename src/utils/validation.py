# src/utils/validation.py
from typing import Any, Dict, List, TypeVar

from src.exceptions import IntraBatchDuplicateError

# Declaring a generic type variable bound to a dictionary shape for strict Mypy typing
T = TypeVar("T", bound=Dict[str, Any])


def process_batch_duplicates(batch: List[T], primary_key: str) -> List[T]:
    """
    Enforces the data lake duplication contract over any arbitrary dataset batch.

    Fulfills U-UTL-01: Filters out and skips exact identical overlapping record rows.
    Fulfills U-UTL-02: Raises an IntraBatchDuplicateError on primary key value collisions
    possessing mismatched field attributes.
    """
    seen_pks: Dict[Any, T] = {}
    unique_records: List[T] = []

    for record in batch:
        pk_value: Any = record.get(primary_key)

        if pk_value is None:
            raise ValueError(
                f"CRITICAL: Record is missing mandatory primary key field '{primary_key}'"
            )

        if pk_value in seen_pks:
            # U-UTL-01: Full Identity Match Evaluation
            if record == seen_pks[pk_value]:
                continue  # Skip duplicate record overlap safely without pipeline alerts

            # U-UTL-02: Attribute Profile Misalignment Conflict (Hard Fail)
            raise IntraBatchDuplicateError(
                f"PK Collision tracked on primary key '{primary_key}' with value '{pk_value}'. "
                f"Existing record attributes do not match the incoming record profile."
            )

        # Store a deep pointer reference to evaluate downstream attribute alignment variations
        seen_pks[pk_value] = record
        unique_records.append(record)

    return unique_records
