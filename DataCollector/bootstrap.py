import pytz
import requests
import json
import pandas as pd
from datetime import datetime

from player import Player


class Bootstrap:

    def __init__(self):
        # get data from API
        self.data = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/").json()

        # initialise columns for tables
        event_columns = ["id", "deadline_time", "finished"]
        team_columns = ["id", "name", "short_name", "strength"]
        element_columns = ["id", "team", "element_type", "first_name", "second_name", "web_name",
                           "chance_of_playing_this_round", "form", "points_per_game", "status",
                           "minutes", "goals_scored", "assists", "clean_sheets", "yellow_cards", "red_cards",
                           "influence", "creativity", "threat", "now_cost"]
        element_type_columns = ["id", "singular_name"]

        # get events, teams, elements and element_types from bootstrap data with specified columns
        self.events = pd.read_json(json.dumps(self.data["events"]))[event_columns].set_index("id")
        self.teams = pd.read_json(json.dumps(self.data["teams"]))[team_columns].set_index("id")
        self.elements = pd.read_json(json.dumps(self.data["elements"]))[element_columns].set_index("id")
        self.element_types = pd.read_json(json.dumps(self.data["element_types"]))[element_type_columns].set_index("id")

        # get other useful helpers for events
        try:
            self.next_event_id = min(self.events[self.events.finished == 0].index)
        except ValueError:
            self.next_event_id = None
        self.is_event_in_progress = not all(self.events[
            self.events.deadline_time < datetime.utcnow().replace(tzinfo=pytz.utc)
        ].finished)

    def update_info(self, engine):

        # dump events
        self.events.to_sql("events", con=engine, index="id", if_exists="replace")

        # dump teams
        self.teams.to_sql("teams", con=engine, index="id", if_exists="replace")

        # dump element types
        self.element_types.to_sql("element_types", con=engine, index="id", if_exists="replace")

        # extract player info from elements property
        player_columns = ["team", "element_type", "first_name", "second_name", "web_name", "now_cost"]
        player_info = self.elements[player_columns].copy()

        # get existing initial costs from players table
        initial_costs = pd.read_sql("SELECT id, initial_cost FROM players;", con=engine, index_col="id")

        # left join this onto the player info table, so any new players will have "null" initial cost
        player_info = player_info.join(initial_costs, on="id", how="left")

        # locate any new players, and retrieve their initial cost
        new_players = player_info.loc[player_info.initial_cost.isnull(), :]
        for idx in new_players.index:
            player_info.loc[idx, "initial_cost"] = Player(idx).get_initial_cost()

        # finally add player info to database
        player_info.to_sql("players", con=engine, index="id", if_exists="replace")

    def get_player_list(self):

        # return a list of all players

        return list(self.elements.index)

    def get_player_data(self, player_id):

        # for player_id, get the below columns from self.elements
        columns = [
            "chance_of_playing_this_round",
            "form",
            "status"
        ]

        return self.elements.loc[player_id, columns].values


if __name__ == "__main__":
    bootstrap = Bootstrap()
    print(bootstrap.is_event_in_progress)
