import mysql.connector
from config.config_vars import db_config

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor(dictionary=True)

insert_list = []


# pull distinct teams list
cursor.execute("SELECT distinct team FROM games_by_team_2024 ORDER BY team")
for row in cursor.fetchall():
    team = row['team']
    # pull game data for each team one at a time
    cursor.execute("SELECT game_date, day_of_week, team, home_away, game_number, opponent, result, runs_scored, runs_allowed, attendance, game_length_minutes FROM games_by_team_2024 WHERE team = '" + team + "'")

    # iterate through games, aggregating data by team
    series_count = 0
    opponent = ''
    home_away = ''
    series_start_date = ''
    series_end_date = ''
    games_dow = ''
    wins = 0
    losses = 0
    series_result = ''
    series_length = 0
    runs_scored = 0
    runs_allowed = 0
    attendance_total = 0
    attendance_average = 0
    game_length_minutes_total = 0
    game_length_minutes_average = 0

    for row in cursor.fetchall():
        if (series_count == 0):
            # starting a new team - initiate values
            series_count += 1
            opponent = row['opponent']
            home_away = row['home_away']
            series_start_date = row['game_date']
            series_end_date = row['game_date']
            games_dow = row['day_of_week']
            series_length = 1
            runs_scored = row['runs_scored']
            runs_allowed = row['runs_allowed']
            attendance_total = row['attendance']
            game_length_minutes_total = row['game_length_minutes']
            wins = 0
            losses = 0
            if (row['result'] == 'W'):
                wins += 1
            elif (row['result'] == 'L'):
                losses += 1
        elif (opponent != row['opponent'] or home_away != row['home_away']):
            # end of series - calculate results
            if (wins > losses):
                series_result = 'W'
            elif (wins < losses):
                series_result = 'L'
            else:
                series_result = 'T'

            attendance_average = attendance_total / series_length
            game_length_minutes_average = game_length_minutes_total / series_length
            # add series data to insert_list
            # team, series nubmer, opponent, home_away,  series start date, series end date, series outcome (W/L/T), series length, 
            # runs_scored, runs_allowed, attendance_toal, attendance_average, game_length_minutes_total, game_length_minutes_average
            insert_list.append((team,
                                series_count, 
                                opponent, 
                                home_away, 
                                series_start_date, 
                                series_end_date, 
                                games_dow,
                                series_result, 
                                wins,
                                losses,
                                series_length, 
                                runs_scored,
                                runs_allowed,
                                attendance_total,
                                attendance_average,
                                game_length_minutes_total,
                                game_length_minutes_average))

            # reset values for new series
            series_count += 1
            opponent = row['opponent']
            home_away = row['home_away']
            series_start_date = row['game_date']
            games_dow = row['day_of_week']
            series_length = 1
            runs_scored = row['runs_scored']
            runs_allowed = row['runs_allowed']
            attendance_total = row['attendance']
            game_length_minutes_total = row['game_length_minutes']
            wins = 0
            losses = 0
            if (row['result'] == 'W'):
                wins += 1
            elif (row['result'] == 'L'):
                losses += 1
        else:
            # continue current series
            # increment game number, W and L values
            if (row['result'] == 'W'):
                wins += 1
            elif (row['result'] == 'L'):
                losses += 1
            series_length += 1
            series_end_date = row['game_date']
            games_dow = games_dow + "-" + row['day_of_week']
            runs_scored += row['runs_scored']
            runs_allowed += row['runs_allowed']
            attendance_total += row['attendance']
            game_length_minutes_total += row['game_length_minutes']

# write insert_list to database
insert_query = """INSERT INTO series_by_team_2024 (team, series_number, opponent, home_away, series_start_date, series_end_date, games_dow, series_result, wins, losses, series_length, runs_scored, runs_allowed, attendance_total, attendance_average, game_length_minutes_total, game_length_minutes_average) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
cursor.executemany(insert_query, insert_list)
connection.commit()
connection.close()