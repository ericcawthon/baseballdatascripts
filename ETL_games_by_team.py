import mysql.connector
from config.config_vars import db_config

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor(dictionary=True)

# extract games logs from database
cursor.execute("SELECT game_date, day_of_week, visiting_team, visiting_game_number, home_team, home_game_number, visiting_score, home_score, total_outs, day_night, park_id, attendance, game_length_minutes, visiting_line_score, home_line_score FROM games_2024")

# iterate through records, creating insert statements for each team in the recrod
insert_list = []
for row in cursor.fetchall():
    print(row)
    insert_list.append((row['game_date'], row['day_of_week'], row['home_team'], 'H', row['home_game_number'], row['visiting_team'], row['visiting_game_number'], row['home_score'], row['visiting_score'], row['total_outs'], row['day_night'], row['park_id'], row['attendance'], row['game_length_minutes'], row['home_line_score'], row['visiting_line_score']))
    insert_list.append((row['game_date'], row['day_of_week'], row['visiting_team'], 'A', row['visiting_game_number'], row['home_team'], row['home_game_number'], row['visiting_score'], row['home_score'], row['total_outs'], row['day_night'], row['park_id'], row['attendance'], row['game_length_minutes'], row['home_line_score'], row['visiting_line_score']))

# load into database

insert_query = """INSERT INTO  games_by_team_2024 (game_date, day_of_week, team, home_away, game_number, opponent, opponent_game_number, runs_scored, runs_allowed, total_outs, day_night, park_id, attendance, game_length_minutes, team_line_score, opponent_line_score) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
cursor.executemany(insert_query, insert_list)
connection.commit()
connection.close()