# FPL Project Plan

## To Do

- Implement data collection in Python
  - Plan project structure
  - Collect as much data as possible from API
  - Test and implement
- Deploy data collector
  - Deploy local SQL database
  - Deploy Python code locally with Docker - look to migrate to AWS
- Model data
  - Collect data from database
  - Train model
  - Get predictions
- Deploy data model
- Iterate previous steps
  - Streamline data collection process
  - Model driven development
  - Collect only important features - add in more features where necessary

## Python Data Collector

### Project Structure

- ### FPL

  - main.py - implements main data collecting functionality

  - player.py - Player class, collects required data from element summary API
  
  - bootstrap.py - Bootstrap class, methods to collect required data from bootstrap-static API

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
    

  
  
