import requests
import json
import pandas as pd


class Player:

    def __init__(self, player_id):
        self.data = requests.get(
            f"https://fantasy.premierleague.com/api/element-summary/{player_id}/").json()
        self.fixtures = pd.read_json(
            json.dumps(self.data["fixtures"]))  # Pandas DataFrame of upcoming fixtures in cron order
        self.history = pd.read_json(
            json.dumps(self.data["history"]))  # Pandas DataFrame of previous fixtures in cron order

    def get_player_data(self, event):

        if event is None:
            raise ValueError("Event not specified")

        output = []

        # get all fixtures for event given
        next_fixtures = self.fixtures[self.fixtures.event == event]

        # check last three games
        last_three = self.history.iloc[-3:, :]
        points = list(reversed(list(last_three.total_points)))
        minutes = list(reversed(list(last_three.minutes)))

        # generate player stats
        stats = []
        columns = ["total_points", "minutes", "goals_scored", "assists",
                   "clean_sheets", "goals_conceded", "yellow_cards", "red_cards",
                   "bonus", "influence", "creativity", "threat"]
        for column in columns:
            stats.append(self.get_column_average(column, is_home=1))
            stats.append(self.get_column_average(column, is_home=0))

        # loop over each fixture in next event
        for idx in next_fixtures.index:

            # get fixture data
            fixture = next_fixtures.loc[idx, :]

            # get info on fixture_id, next opposition, kickoff time
            fixture_id = fixture.id
            is_home = fixture.is_home
            next_opposition = fixture.team_a if is_home else fixture.team_h
            kickoff_time = fixture.kickoff_time

            # add each generated row to the output
            output.append([fixture_id, next_opposition, is_home, kickoff_time, *stats, *points, *minutes])

        return output

    def get_response(self, fixture_code):

        # get the total points for the given fixture code

        return int(self.history[self.history['fixture'] == fixture_code]["total_points"])

    def get_initial_cost(self):

        return self.history.loc[self.history.index[0], "value"]

    def get_column_average(self, column, is_home=None):
        try:
            if is_home is None:
                return round(self.history[column].mean(),2)
            else:
                return round(self.history[self.history['was_home'] == is_home][column].mean(),2)
        except:
            return 0


if __name__ == "__main__":
    player = Player(114)
    print(player.get_response(311))
