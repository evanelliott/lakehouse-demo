# src/scraper/parsers.py
import re
from typing import Any, Dict, List, Union

from bs4 import BeautifulSoup

from src.exceptions import SelectorNotFoundError, SourceDataMissingError


def parse_understat_raw(payload: Dict[str, Any], target_dataset: str) -> List[Dict[str, Any]]:
    """
    Core extraction engine for Understat JSON inputs.

    Fulfills U-EXT-03 through U-EXT-06: Flatten team scopes and normalise variant
    field schemas cleanly before files touch data-lake storage zones.
    """
    if target_dataset not in payload:
        raise SourceDataMissingError(f"Missing root key envelope: '{target_dataset}'")

    records: Union[List[Any], Dict[str, Any]] = payload[target_dataset]

    if not records:
        raise ValueError(f"Empty record array payload discovered for: '{target_dataset}'")

    record_list: List[Dict[str, Any]] = []

    if target_dataset in ["rosters", "shots"] and isinstance(records, dict):
        for side in ["h", "a"]:
            if side in records:
                side_data = records[side]
                if isinstance(side_data, dict):
                    record_list.extend(list(side_data.values()))
                elif isinstance(side_data, list):
                    record_list.extend(side_data)
    else:
        record_list = list(records.values()) if isinstance(records, dict) else records

    field_mapping: Dict[str, tuple[str, str]] = {
        "teams": ("id", "title"),
        "players": ("id", "player_name"),
        "dates": ("id", "datetime"),
        "rosters": ("id", "player"),
        "shots": ("id", "player"),
    }

    pk_field, label_field = field_mapping[target_dataset]
    validated_batch: List[Dict[str, Any]] = []

    for row in record_list:
        if pk_field not in row or label_field not in row:
            raise ValueError(f"Row item missing critical attributes inside '{target_dataset}'")

        try:
            int(row[pk_field])
            str(row[label_field])
        except (ValueError, TypeError):
            raise TypeError(f"Invalid schema data type detected inside '{target_dataset}' row")

        validated_batch.append({"id": int(row[pk_field]), "title": str(row[label_field])})

    if not validated_batch:
        raise ValueError(f"No active internal rows could be extracted from: '{target_dataset}'")

    return validated_batch


def parse_premier_injuries_raw(html_content: str) -> List[Dict[str, Any]]:
    """
    Core extraction engine for Premier Injuries HTML inputs.

    Fulfills U-EXT-03 through U-EXT-08: Parses real-world table grid layouts
    by handling mobile tags and isolating nested identifier tokens.
    """
    soup = BeautifulSoup(html_content, "lxml")

    # U-EXT-03 & 08: Missing ID Key / Layout Drift Check
    # Look strictly for the named class table node
    if not soup.find("table", class_="injury-table"):
        raise SelectorNotFoundError("Premier Injuries table node layout modification detected!")

    # Target player rows explicitly via class signature
    rows = soup.find_all("tr", class_=re.compile(r"player-row"))

    if not rows:
        # U-EXT-05: Handle empty table response structures
        raise ValueError("Empty dataset: No active injury rows parsed from HTML grid")

    records: List[Dict[str, Any]] = []

    for row in rows:
        cells = row.find_all("td")
        # Ensure row contains the required structural column cells array size
        if len(cells) < 6:
            continue

        # 1. Extract Player ID out of the tracking link block in column index 6
        tracking_link = cells[6].find("a", class_="track")
        if not tracking_link:
            continue

        player_id_attr = tracking_link.get("data-id")

        # 2. Extract Status text out of column index 5, dropping responsive mobile titles
        status_cell = cells[5]
        # Drop the mobile title node dynamically to avoid squashing issues ("Status50%" -> "50%")
        if status_cell.find("div", class_="mob-title"):
            status_cell.find("div", class_="mob-title").decompose()

        status_attr = status_cell.text.strip()

        # U-EXT-04: If field parsing leaves an isolated null attribute, crash
        if player_id_attr is None or not status_attr:
            raise ValueError("Row item missing critical inner attributes")

        # U-EXT-06: Schema Type enforcement casting gate
        try:
            player_id = int(str(player_id_attr).strip())
            status_text = str(status_attr).strip()
        except (ValueError, TypeError):
            raise TypeError("Mismatched data types discovered during DOM data conversion")

        records.append({"id": player_id, "title": status_text})

    # Catch empty edge cases after collection loops finalize (U-EXT-05 fallback)
    if not records:
        raise ValueError("Empty dataset: No active injury records extracted from rows")

    return records
