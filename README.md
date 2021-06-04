# FPL Project

## Project Structure

- ### DataCollector

  - main.py - implements main data collecting functionality using data pipeline detailed below

  - player.py - Player class, collects required data from element summary API
  
  - bootstrap.py - Bootstrap class, methods to collect required data from bootstrap-static API

- ### DataModel

  - main.py - implements main data modelling functionality, creating predictions based on the data collected using scikit-learn
  - custom_regressor.py - implements hurdle regression, combining logistic regression to predict if a player will play, followed by random forest regression to predict the score given they do play.

### Data Pipeline

- Get data from bootstrap-static
- Update data in events, teams, and element-types tables
- Loop through each player
  - Update general info in players table from bootstrap data
  - Separate remaining bootstrap data ready to be input to data table 
  - Make request to element summary API
  - Process history and upcoming fixture data
  - Merge this data with bootstrap data
  - Add as row to database. If more than one fixture - add as a separate row 
  with different opposition data and blank previous game information
    

  
  
