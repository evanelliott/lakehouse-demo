import json
import re
from typing import Any, Dict, List

from src.exceptions import IntraBatchDuplicateError, SourceDataMissingError


class UnderstatParser:
    def parse_match(self, html: str) -> Dict[str, Any]:
        """
        Extracts core metadata from the initial HTML skeleton.
        Captures match ID, teams, goals, xG totals, and match date.
        """
        info_match = re.search(r"match_info\s+=\s+JSON\.parse\(['\"](.+?)['\"]\)", html)
        if not info_match:
            raise SourceDataMissingError("match_info script tag not found in HTML.")

        info_raw = json.loads(info_match.group(1).encode("utf-8").decode("unicode_escape"))

        return {
            "match_id": str(info_raw.get("id", "0")),
            "home_team": info_raw.get("team_h", "Unknown"),
            "away_team": info_raw.get("team_a", "Unknown"),
            "home_goals": int(info_raw.get("h_goals", 0)),
            "away_goals": int(info_raw.get("a_goals", 0)),
            "home_xg": float(info_raw.get("h_xg", 0.0)),
            "away_xg": float(info_raw.get("a_xg", 0.0)),
            "match_date": info_raw.get("date"),
        }

    def parse_league_players(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parses the 'players' list from the /getLeagueData/EPL/{year} JSON.
        """
        raw_players = json_data.get("players", [])
        processed_players: List[Dict[str, Any]] = []

        for p in raw_players:
            processed_players.append(
                {
                    "player_id": str(p.get("id")),
                    "name": p.get("player_name"),
                    "team": p.get("team_title"),
                    "position": p.get("position"),
                    "matches": int(p.get("games", 0)),
                    "minutes": int(p.get("time", 0)),
                    "goals": int(p.get("goals", 0)),
                    "assists": int(p.get("assists", 0)),
                    "xg": float(p.get("xG", 0.0)),
                    "xa": float(p.get("xA", 0.0)),
                    "shots": int(p.get("shots", 0)),
                    "key_passes": int(p.get("key_passes", 0)),
                    "npxg": float(p.get("npxG", 0.0)),
                    "xg_chain": float(p.get("xGChain", 0.0)),
                    "xg_buildup": float(p.get("xGBuildup", 0.0)),
                }
            )

        return processed_players

    def parse_rosters(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parses the 'rosters' object from the /getMatchData/{id} JSON."""
        processed_rosters: List[Dict[str, Any]] = []
        roster_ids: set[str] = set()

        # Rosters are typically grouped by 'h' and 'a'
        for side in ["h", "a"]:
            team_roster = json_data.get("rosters", {}).get(side, {})
            for p_id, p_info in team_roster.items():
                str_p_id = str(p_id)
                if str_p_id in roster_ids:
                    raise IntraBatchDuplicateError(f"Duplicate roster ID: {p_id}")

                roster_ids.add(str_p_id)
                processed_rosters.append(
                    {
                        "player_id": str_p_id,
                        "player_name": p_info.get("player"),
                        "is_start": p_info.get("position") != "Sub",
                        "position": p_info.get("position"),
                        "minutes": int(p_info.get("time", 0)),
                        "team_side": side,
                    }
                )
        return processed_rosters

    def parse_shots(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parses the JSON response from /getMatchData/{id}."""
        processed_shots: List[Dict[str, Any]] = []
        shot_ids: set[str] = set()

        # getMatchData returns dict with 'h' and 'a' keys for shots
        for side in ["h", "a"]:
            for s in json_data.get("shots", {}).get(side, []):
                s_id = str(s.get("id"))
                if s_id in shot_ids:
                    raise IntraBatchDuplicateError(f"Duplicate shot ID: {s_id}")

                shot_ids.add(s_id)
                processed_shots.append(
                    {
                        "id": s_id,
                        "player": s.get("player"),
                        "xg": float(s.get("xG", 0)),
                        "minute": int(s.get("minute", 0)),
                        "result": s.get("result"),
                    }
                )
        return processed_shots
