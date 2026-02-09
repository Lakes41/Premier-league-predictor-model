import requests
import sys
import os
import pickle
from typing import Any, Dict, List

import pandas as pd
from src.config.secrets import settings, project_root
from src.exception import CustomException
from src.logger import logging


class DataCollector:
    def __init__(self):
        self.api_key = settings.API_KEY
        self.api_base_url = settings.API_BASE_URL

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            url = f"{self.api_base_url}/{endpoint}"
            headers = {"x-apisports-key": self.api_key}
            logging.info(f"GET {url} params={params}")
            resp = requests.get(url=url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logging.error(f"Request failed on {endpoint}: {e}")
            raise CustomException(e, sys)

    def fetch_teams(self, season: int, league: int = 39) -> pd.DataFrame:
        try:
            team_dict = self._get("teams", {"league": league, "season": season})
            team_data = team_dict.get("response", [])
            team_df = pd.DataFrame([item["team"] for item in team_data])
            logging.info("Teams fetched successfully")
            return team_df
        except Exception as e:
            logging.error(f"Error fetching teams: {e}")
            raise CustomException(e, sys)

    def fetch_team_stats(self, team_id: int, season: int, league: int = 39) -> Dict[str, Any]:
        try:
            stats_dict = self._get(
                "teams/statistics", {"league": league, "season": season, "team": team_id}
            )
            stats_data = stats_dict.get("response", {})
            if isinstance(stats_data, list):
                stats_data = stats_data[0] if stats_data and isinstance(stats_data[0], dict) else {}
            if stats_data is None:
                stats_data = {}
            if not isinstance(stats_data, dict):
                logging.warning(
                    f"Unexpected statistics response type for team_id={team_id}, season={season}: "
                    f"{type(stats_data).__name__}. Using empty dict."
                )
                stats_data = {}
            logging.info(f"Statistics fetched successfully for team_id={team_id} season={season}")
            return stats_data
        except Exception as e:
            logging.error(f"Error fetching team stats: {e}")
            raise CustomException(e, sys)

    def save_data(self, season: int) -> None:
        try:
            teams_df = self.fetch_teams(season)
            team_ids: List[int] = teams_df["id"].tolist()
            team_stats: List[Dict[str, Any]] = []

            base_dir = os.path.join(str(project_root()), "data", str(season))
            os.makedirs(base_dir, exist_ok=True)
            with open(os.path.join(base_dir, f"teams_{season}.pkl"), "wb") as f:
                pickle.dump(teams_df, f)
            logging.info(f"Teams saved for season {season}")

            for team_id in team_ids:
                stats = self.fetch_team_stats(team_id, season)

                def _sum_cards(color: str) -> int:
                    buckets = stats.get("cards", {}).get(color, {})
                    return sum(int(bucket.get("total") or 0) for bucket in buckets.values())

                team_obs = {
                    "id": stats.get("team", {}).get("id"),
                    "form": stats.get("form"),
                    "fixture_hw": stats.get("fixtures", {}).get("wins", {}).get("home"),
                    "fixtures_aw": stats.get("fixtures", {}).get("wins", {}).get("away"),
                    "fixtures_hl": stats.get("fixtures", {}).get("loses", {}).get("home"),
                    "fixtures_al": stats.get("fixtures", {}).get("loses", {}).get("away"),
                    "fixtures_hd": stats.get("fixtures", {}).get("draws", {}).get("home"),
                    "fixtures_ad": stats.get("fixtures", {}).get("draws", {}).get("away"),
                    "goals_h": stats.get("goals", {}).get("for", {}).get("total", {}).get("home"),
                    "goals_a": stats.get("goals", {}).get("for", {}).get("total", {}).get("away"),
                    "conceded_h": stats.get("goals", {}).get("against", {}).get("total", {}).get("home"),
                    "conceded_a": stats.get("goals", {}).get("against", {}).get("total", {}).get("away"),
                    "clean_sheet_h": stats.get("clean_sheet", {}).get("home"),
                    "clean_sheet_a": stats.get("clean_sheet", {}).get("away"),
                    "fts_h": stats.get("failed_to_score", {}).get("home"),
                    "fts_a": stats.get("failed_to_score", {}).get("away"),
                    "penalty_scored": stats.get("penalty", {}).get("scored", {}).get("total"),
                    "penalty_missed": stats.get("penalty", {}).get("missed", {}).get("total"),
                    "cards_yellow": _sum_cards("yellow"),
                    "cards_red": _sum_cards("red"),
                }

                team_stats.append(team_obs)

            team_df = pd.DataFrame(team_stats)
            with open(os.path.join(base_dir, f"team_stats_{season}.pkl"), "wb") as f:
                pickle.dump(team_df, f)
            logging.info(f"Team stats saved for season {season}")
        except Exception as e:
            logging.error(f"Error saving data: {e}")
            raise CustomException(e, sys)

if __name__ == "__main__":
    try:
        collector = DataCollector()
        seasons = [2024, 2023]
        for season in seasons:
            collector.save_data(season)
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise CustomException(e, sys)
