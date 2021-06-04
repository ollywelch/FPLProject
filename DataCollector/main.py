import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz
import sqlalchemy as db
from tqdm import tqdm

from bootstrap import Bootstrap
from player import Player


# Initialise database connection
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_ADDRESS = os.environ.get("DB_ADDRESS")
DB_NAME = os.environ.get("DB_NAME")

ENGINE = db.create_engine(
    f"mariadb+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_ADDRESS}/{DB_NAME}?charset=utf8mb4"
)


def get_team_data(bootstrap):
    data = []
    for team_id in range(1, 21):
        team_players = bootstrap.elements[bootstrap.elements.team == team_id]
        player = Player(team_players.index[0])
        avg_gf = player.get_column_average('team_h_score')
        avg_ga = player.get_column_average('team_a_score')
        data.append([team_id, avg_gf, avg_ga])
    return pd.DataFrame(data, columns=['opposition', 'opposition_gf', 'opposition_ga']).set_index('opposition')


def collect_next_event():
    """
    Function to collect data for the next unfinished event, and return to a pandas DataFrame
    """

    # initialise Bootstrap class instance
    bootstrap = Bootstrap()

    # initialise the DataFrame
    columns = [
        "player_id",
        "event_id",
        "timestamp",
        "chance_of_playing_this_round",
        "form",
        "status",
        "fixture_code",
        "opposition",
        "is_home",
        "kickoff_time",
        "points_h",
        "points_a",
        "minutes_h",
        "minutes_a",
        "goals_scored_h",
        "goals_scored_a",
        "assists_h",
        "assists_a",
        "clean_sheets_h",
        "clean_sheets_a",
        "goals_conceded_h",
        "goals_conceded_a",
        "yellow_cards_h",
        "yellow_cards_a",
        "red_cards_h",
        "red_cards_a",
        "bonus_h",
        "bonus_a",
        "influence_h",
        "influence_a",
        "creativity_h",
        "creativity_a",
        "threat_h",
        "threat_a",
        "points_1",
        "points_2",
        "points_3",
        "minutes_1",
        "minutes_2",
        "minutes_3",
        "opposition_strength",
        "response"
    ]
    df = []

    # get a list of all player ids
    player_list = bootstrap.get_player_list()

    # initialise next event and timestamp ready for input into table
    next_event = bootstrap.next_event_id
    timestamp = datetime.utcnow().replace(tzinfo=pytz.utc)

    # loop through each player, gathering the required information
    for player_id in tqdm(player_list):  # TODO test double event handling

        # get data from bootstrap static
        bootstrap_data = bootstrap.get_player_data(player_id)

        # retrieve data from element summary page
        player_data = Player(player_id).get_player_data(next_event)

        for player_fixture in player_data:
            # get opposition data
            team_id = player_fixture[1]
            opposition_strength = bootstrap.teams.loc[team_id, "strength"]

            # create the row and append to df to build df
            row = [
                player_id,
                next_event,
                timestamp,
                *bootstrap_data,
                *player_fixture,
                opposition_strength,
                pd.NA  # placeholder to fill response column
            ]
            df.append(row)

    # return a pandas DataFrame object
    df = pd.DataFrame(df, columns=columns)

    # get team data
    team_data = get_team_data(bootstrap)

    df = df.join(team_data, on='opposition')

    columns.insert(7, 'opposition_gf')
    columns.insert(8, 'opposition_ga')

    return df[columns].set_index("player_id")


def handler(event, context):
    """
    Master Lambda handler to run every hour and update the database with the required information
    """

    # initialise instance of Bootstrap class for later use
    bootstrap = Bootstrap()

    print("Updating general info...")

    # update general info about players, teams and events
    bootstrap.update_info(ENGINE)

    print("General info updated!")

    # Fill in any blank response data where fixture was played > 12 hours ago

    print("Filling blank response data...")

    # check all unfinished entries with kickoff < now - 12 hours
    data = pd.read_sql("SELECT * FROM data WHERE response is NULL;", con=ENGINE, index_col="entry_id")
    completed_games = data[
        (data.kickoff_time < (datetime.utcnow() - timedelta(hours=12)))
    ]

    # for each completed game, fill with points scored
    for entry_id in tqdm(completed_games.index):
        # get most recent points
        player_id = completed_games.loc[entry_id, "player_id"]
        fixture_code = completed_games.loc[entry_id, "fixture_code"]
        try:
            response = Player(player_id).get_response(fixture_code)
            # insert into data table where entry_id matches completed games table
            ENGINE.execute(f"UPDATE data SET response={response} WHERE entry_id={entry_id}")
        except TypeError:
            ENGINE.execute(f"DELETE FROM data WHERE entry_id={entry_id}")
    print("Blank response data filled!")

    # check if an event is in progress, collect new data only if not

    if bootstrap.is_event_in_progress:

        print("Event in progress, not collecting new data.")

    else:

        print("Event not in progress, collecting data...")

        # get player data
        player_data = collect_next_event()

        # delete outdated data from database if exists and reset auto increment property
        ENGINE.execute("DELETE FROM data WHERE response is NULL")

        try:
            new_auto_increment = list(ENGINE.execute("SELECT MAX(entry_id) FROM data"))[0][0] + 1
        except:
            new_auto_increment = 1
        ENGINE.execute(f"ALTER TABLE data AUTO_INCREMENT={new_auto_increment}")

        # dump newly collected data to db
        player_data.to_sql("data", con=ENGINE, if_exists="append")

    return {"response": 200}


if __name__ == "__main__":
    handler(None, None)
