import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, Tag

from src.exceptions import IntraBatchDuplicateError, SelectorNotFoundError


class PremierInjuriesParser:
    def _clean(self, text: str, label: str) -> str:
        """
        Cleans injury text: collapses whitespace, handles specific Premier Injuries
        artifacts like 'See Player Page', and strips manager quote wrappers.
        """
        # 1. First, remove the mobile label (e.g., 'Further Detail')
        text = text.replace(label, "", 1)

        # 2. Collapse all whitespace/newlines first so we have a clean string to work with
        text = re.sub(r"\s+", " ", text).strip()

        # 3. Remove 'See Player Page' (case-insensitive, handles missing spaces)
        text = re.sub(r"See Player Page", "", text, flags=re.IGNORECASE).strip()

        # 4. Strip those manager quote wrappers (e.g., "Apr 09: 'He is out'" -> "Apr 09: He is out")
        text = text.replace(": '", ": ").replace(".'", ".").replace("?'", "?")

        # 5. Final strip of any leading/trailing single/double quotes
        text = text.strip("'").strip('"')

        return text.strip()

    def parse_team_injuries(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[Dict[str, Any]] = []
        player_names: set[str] = set()
        current_team: Optional[str] = None

        table = soup.find("table", class_="injury-table")
        if not isinstance(table, Tag):
            raise SelectorNotFoundError("Injury table not found in HTML.")

        for row in table.find_all("tr"):
            row_classes: List[str] = row.get("class", [])

            if "heading" in row_classes:
                team_div = row.find("div", class_="injury-team")
                if team_div:
                    current_team = team_div.get_text(strip=True)

            elif "player-row" in row_classes:
                cols = row.find_all("td")
                if len(cols) >= 6:
                    # Capture raw player name first for duplicate check
                    player_name = self._clean(cols[0].get_text(), "Player")

                    if player_name in player_names:
                        raise IntraBatchDuplicateError(f"Duplicate player detected: {player_name}")

                    player_names.add(player_name)

                    results.append(
                        {
                            "team": current_team,
                            "player_name": player_name,
                            "reason": self._clean(cols[1].get_text(), "Reason"),
                            "further_detail": self._clean(cols[2].get_text(), "Further Detail"),
                            "potential_return": self._clean(cols[3].get_text(), "Potential Return"),
                            "condition": self._clean(cols[4].get_text(), "Condition"),
                            "status": self._clean(cols[5].get_text(), "Status"),
                        }
                    )

        return results
