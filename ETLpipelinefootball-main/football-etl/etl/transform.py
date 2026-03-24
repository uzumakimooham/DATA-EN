import pandas as pd
from datetime import datetime

def transform_data(data):
    teams = pd.DataFrame(data['teams'])[['id', 'name']]
    element_types = pd.DataFrame(data['element_types'])[['id', 'singular_name_short']]
    players = pd.DataFrame(data['elements'])

    
    df = players[['web_name', 'team', 'goals_scored', 'minutes', 'points_per_game', 'assists', 'dreamteam_count', 'element_type', 'birth_date', 'now_cost']]
    df = df.merge(teams, left_on='team', right_on='id')
    df = df.merge(element_types, left_on='element_type', right_on='id', suffixes=('', '_pos'))

    # อายุ
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    df['age'] = (datetime.now() - df['birth_date']).dt.days // 365
    
    
    #แปลงเป็นบาทไทย
    df['now_cost'] = (df['now_cost'] * 100000 * 40).astype(int)
    
    
    # ลงเล่น
    df['matches'] = df['minutes'] / 90
    df = df[df['matches'] > 0]
    df['matches'] = df['matches'].round().astype(int)
    
    leaderboard = df.sort_values(by='goals_scored', ascending=False).head(100)

    # rename
    leaderboard = leaderboard.rename(columns={
        'web_name': 'player_name',
        'name': 'team_name',
        'goals_scored': 'goals',
        'points_per_game': 'average_rating',
        'assists': 'assists',
        'dreamteam_count': 'player_of_the_week',
        'singular_name_short': 'position',
        'now_cost': 'salary_per_week'
    })

    return leaderboard[['player_name','team_name','position','age','salary_per_week','goals','assists','matches','average_rating','player_of_the_week']]