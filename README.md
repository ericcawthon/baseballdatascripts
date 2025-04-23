# baseballdatascripts
## Tuesday, April 22nd
- created `load_rs_gamelogs.py` to load in [game log data from Retrosheet](https://www.retrosheet.org/gamelogs/).  I'm only loading 2024 data for now (see the rawdata sub-directory).
- created `ETL_games_by_team.py` to create a new table that lists a record per team game (effectively doubling the number of rows of first table) to enable easier aggregation in future
- added files with the CREATE TABLE statements for both tables

---