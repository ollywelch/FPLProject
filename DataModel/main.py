import os
import sqlalchemy as db
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor

from custom_regressor import CustomRegressor
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

# Initialise database connection
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_ADDRESS = os.environ.get("DB_ADDRESS")
DB_NAME = os.environ.get("DB_NAME")

ENGINE = db.create_engine(
    f"mariadb+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_ADDRESS}/{DB_NAME}?charset=utf8mb4"
)


def get_datasets():

    # grab data from database
    df = pd.read_sql("SELECT * FROM data;", con=ENGINE, index_col='entry_id')

    # delete unneeded columns
    del_columns = [
        "timestamp",
        "opposition",
        "fixture_code",
        "kickoff_time"
    ]
    for col in del_columns:
        del df[col]

    # get dummies for categorical variables
    df = pd.get_dummies(df, columns=['status'])

    # split into training, validation and prediction sets
    df_predict = df[df.response.isna()]
    df_train, df_val = train_test_split(df[df.response.notna()], test_size=0.1, random_state=0)

    return df_train, df_val, df_predict


def handler(event, context):

    # get training, validation and prediction datasets
    df_train, df_val, df_predict = get_datasets()

    player_ids = df_predict.player_id.values

    for df in [df_train, df_val, df_predict]:
        del df['player_id']
        del df['event_id']
        df['chance_of_playing_this_round'].fillna(100, inplace=True)
        df.fillna(0, inplace=True)

    # DATA PIPELINE:
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('regressor', CustomRegressor())
    ])

    # Set parameters to gridsearch over
    max_depth = [9]
    min_samples_leaf = [10]
    min_samples_split = [3]

    params = [{'max_depth': md, 'min_samples_leaf': msl, 'min_samples_split': mss}
                  for md in max_depth for msl in min_samples_leaf for mss in min_samples_split]

    param_grid = [{
        'regressor__reg_params': params
    }]

    # param_grid = [{
    #     'regressor__max_depth': max_depth,
    #     'regressor__min_samples_leaf': min_samples_leaf,
    #     'regressor__min_samples_split': min_samples_split,
    # }]

    # Fit the gridsearched model to the training data with 4-fold CV
    grid = GridSearchCV(pipe, n_jobs=-1, param_grid=param_grid, cv=4, verbose=1, scoring='r2')

    X_train, y_train = df_train.loc[:, df_train.columns != 'response'], df_train.response.astype('int').values
    X_val, y_val = df_val.loc[:, df_val.columns != 'response'], df_val.response.astype('int').values
    X_predict = df_predict.loc[:, df_predict.columns != 'response']

    grid.fit(X_train, y_train)

    # Give best CV score, as well as training and test set performance with the gridsearched parameters
    print(f'Best score obtained with params {grid.best_params_}')
    print(f'Best CV score {grid.best_score_}')
    print(f'Performance on training set {grid.score(X_train, y_train)}')
    print(f'Performance on validation set: {grid.score(X_val, y_val)}')

    predictions = grid.predict(X_predict)

    predictions = [[player_ids[idx], predictions[idx]] for idx in range(len(player_ids))]

    predictions = pd.DataFrame(predictions, columns=['id', 'prediction']).groupby('id').sum()

    player_info = pd.read_sql("SELECT * FROM players", con=ENGINE, index_col='id')

    predictions = predictions.join(player_info, on='id')

    predictions.round(2).to_sql("predictions", con=ENGINE, index='id', if_exists='replace')

    return {"response": 200}


if __name__ == "__main__":
    handler(None, None)
