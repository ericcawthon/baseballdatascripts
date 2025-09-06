import mysql.connector
import json
from config.config_vars import db_config

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor(dictionary=True)

# extract games logs from database
cursor.execute("SELECT game_date, visiting_team, visiting_score, home_team, home_score FROM `games_2020` ORDER BY game_date")


insert_list = []
rows = 1
streaks = {}

# iterate through every game
for row in cursor.fetchall():

    if rows == 1:
        current_date = row['game_date']
        rows += 1

    # check for new date - if new, determine who has longest streak and rest 
    # current_date to new date
    if row['game_date'] != current_date:
        sorted_streaks = sorted(streaks.items(), key=lambda item: item[1], reverse=True)
        print(f"Date: {current_date}, Longest Streaks: {sorted_streaks}")
        print ("-----------------------------------------------------")
        insert_list.append((current_date, sorted_streaks[0][1], json.dumps(sorted_streaks)))
        current_date = row['game_date']
    if row['home_team'] not in streaks:
        streaks[row['home_team']] = 0
    if row['visiting_team'] not in streaks:
        streaks[row['visiting_team']] = 0

    # compare home score to visting score
    # if home team wins, set visiting team's streak to 0 and increment home team's streak by 1
    if row['home_score'] > row['visiting_score']:
        streaks[row['home_team']] += 1
        streaks[row['visiting_team']] = 0
    # if visiting team wins, set home team's streak to 0 and increment visiting team's streak by 1
    else:
        streaks[row['visiting_team']] += 1
        streaks[row['home_team']] = 0

insert_query = """INSERT INTO  win_streaks_by_day (game_date, longest_streak, sorted_streaks) VALUES (%s, %s, %s) """
cursor.executemany(insert_query, insert_list)
connection.commit()
connection.close()




