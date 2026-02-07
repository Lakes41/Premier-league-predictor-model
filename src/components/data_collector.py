import requests
import sys
import os
import pandas as pd
import pickle
from src.config.secrets import settings
from src.exception import CustomException
from src.logger import logging

class DataCollector:
    def __init__(self):
        self.api_key = settings.API_KEY
        self.api_base_url = settings.API_BASE_URL

    def fetch_teams(self, season: int, league: int = 39) -> pd.DataFrame:
        try:
            # Define the API endpoint and parameters
            endpoint = "teams"
            params = {
                "league": league,
                "season": season
            }
            url = f"{self.api_base_url}/{endpoint}"
            headers = {'x-apisports-key': self.api_key}
            logging.info(f"Fetching data from {url} with params {params}")
            
            # Make the API GET request
            response_t = requests.get(url= url, headers=headers, params=params)
            if response_t.status_code == 200:
                team_dict = response_t.json()
                team_data = team_dict['response']
                team_df = pd.DataFrame([team_data[i]['team'] for i in range(len(team_data))])
            return team_df
            logging.info("Data fetched successfully")
            return pd.DataFrame(data)
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise CustomException(e, sys)
        
        def fetch_team_stats(self, team_id: int, season: int, league: int = 39) -> Response:
            try:
                # Define the API endpoint and parameters
                endpoint = "teams/statistics"
                params = {
                    "league": league,
                    "season": season,
                    "team": team_id
                }
                url = f"{self.api_base_url}/{endpoint}"
                headers = {'x-apisports-key': self.api_key}
                logging.info(f"Fetching data from {url} with params {params}")
                
                # Make the API GET request
                response_s = requests.get(url= url, headers=headers, params=params)
                if response_s.status_code == 200:
                    stats_dict = response_s.json()
                    stats_data = stats_dict['response']
                return stats_data
                logging.info("Data fetched successfully")
                
            except Exception as e:
                logging.error(f"Error fetching data: {e}")
                raise CustomException(e, sys)
        
        def save_data(self, season: int) -> None:
            try:
                # Fetch team data
                teams_df = self.fetch_teams(season)
                team_ids = teams_df['id'].tolist()
                team_stats = []
                
                # Save team data to a pickle file   
                with open(f"../data/teams_{season}.pkl", "wb") as f:
                    pickle.dump(teams_df, f)
                logging.info(f"Data saved successfully for season {season}")
                
                for team_id in team_ids:
                    # Fetch team stats for id
                    stats = self.fetch_team_stats(team_id, season)
                    
                    # Grab Relevvant data
                    team_obs = {
                        id : stats['team']['id'],                        
                        form: stats['form'],
                        
                        fixture_hw : stats['fixtures']['wins']['home'],
                        fixtures_aw : stats['fixtures']['wins']['away'],
                        fixtures_hl : stats['fixtures']['loses']['home'],
                        fixtures_al : stats['fixtures']['loses']['away'],
                        fixtures_hd : stats['fixtures']['draws']['home'],
                        fixtures_ad : stats['fixtures']['draws']['away'],

                        goals_h : stats['goals']['for']['total']['home'],
                        goals_a : stats['goals']['for']['total']['away'],
                        conceded_h : stats['goals']['against']['total']['home'],
                        conceded_a : stats['goals']['against']['total']['away'],

                        clean_sheet_h : stats['clean_sheet']['home'],
                        clean_sheet_a : stats['clean_sheet']['away'],
                        
                        fts_h : stats['failed_to_score']['home'],
                        fts_a : stats['failed_to_score']['away'],
                        
                        penalty_scored : stats['penalty']['scored']['total'],
                        penalty_missed : stats['penalty']['missed']['total'],
                        
                        cards_yellow : stats['cards']['yellow']['total'],
                        cards_red : stats['cards']['red']['total']}

                    team_stats.append(team_obs)
                team_df = pd.DataFrame(team_stats)
                with open(f"../data/team_stats_{season}.pkl", "wb") as f:
                    pickle.dump(stats_data, f)
                logging.info(f"Data saved successfully for season {season}")
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
    