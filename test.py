import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import time
import random
import os
from dotenv import load_dotenv, dotenv_values

team = 'ARI'
year = '2023'
URL = f'https://www.footballguys.com/stats/game-logs-against/teams?team={team}&year={year}'

r = requests.get(URL)
soup = BeautifulSoup(r.content, 'lxml')

teams_raw = soup.find('select',{'class':'big-select'})
teams_options = teams_raw.find_all('option')
teams = []
for team in teams_options:
    teams.append(team['value'])

tables = soup.find_all('table',{'class':'table sortable-table'})
df = pd.read_html(StringIO(str(tables[0])))[0]
df = df[0:0]

def table_parser(df, team, year):
    URL = f'https://www.footballguys.com/stats/game-logs-against/teams?team={team}&year={year}'
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'lxml')
    tables = soup.find_all('table',{'class':'table sortable-table'})
    temp_df = pd.read_html(StringIO(str(tables[0])))[0]
    temp_df['pos'] = 'qb'
    temp_df['opp'] = team
    df = pd.concat([df, temp_df])
    temp_df = pd.read_html(StringIO(str(tables[1])))[0]
    temp_df['pos'] = 'rb'
    temp_df['opp'] = team
    df = pd.concat([df, temp_df])
    temp_df = pd.read_html(StringIO(str(tables[2])))[0]
    temp_df['pos'] = 'wr'
    temp_df['opp'] = team
    df = pd.concat([df, temp_df])
    temp_df = pd.read_html(StringIO(str(tables[3])))[0]
    temp_df['pos'] = 'te'
    temp_df['opp'] = team
    df = pd.concat([df, temp_df])
    return df

for team in teams:
    df = table_parser(df, team, year)
    time.sleep(random.uniform(2,5))

df['team-pos'] = df['team'] + '-' + df['pos']

df_sum_teampos = df.copy().drop(columns=['name'])
df_sum_teampos = df_sum_teampos.groupby(by=['team', 'pos', 'week', 'opp', 'team-pos']).sum()
df_sum_teampos = df_sum_teampos.reset_index()
df_sum_teampos.head(5)

df_avg_teampos = df_sum_teampos.copy().drop(columns=['opp', 'week'])
df_avg_teampos = df_avg_teampos.groupby(by=['team', 'pos', 'team-pos']).mean()
df_avg_teampos = df_avg_teampos.reset_index()
df_avg_teampos.head(5)

df_strength_merge = pd.merge(df_sum_teampos, df_avg_teampos, on=['team-pos'], how='left')
df_strength_merge.head()

df_strength_raw = pd.DataFrame()
df_strength_raw['opp'] = df_strength_merge['opp']
df_strength_raw['pos'] = df_strength_merge['pos_x']
df_strength_raw['team'] = df_strength_merge['team_x']
df_strength_raw['week'] = df_strength_merge['week']
df_strength_raw['att'] = df_strength_merge['att_x'] - df_strength_merge['att_y']
df_strength_raw['cmp'] = df_strength_merge['cmp_x'] - df_strength_merge['cmp_y']
df_strength_raw['pyd'] = df_strength_merge['pyd_x'] - df_strength_merge['pyd_y']
df_strength_raw['ptd'] = df_strength_merge['ptd_x'] - df_strength_merge['ptd_y']
df_strength_raw['int'] = df_strength_merge['int_x'] - df_strength_merge['int_y']
df_strength_raw['rsh'] = df_strength_merge['rsh_x'] - df_strength_merge['rsh_y']
df_strength_raw['rshyd'] = df_strength_merge['rshyd_x'] - df_strength_merge['rshyd_y']
df_strength_raw['rshtd'] = df_strength_merge['rshtd_x'] - df_strength_merge['rshtd_y']
df_strength_raw['targ'] = df_strength_merge['targ_x'] - df_strength_merge['targ_y']
df_strength_raw['rec'] = df_strength_merge['rec_x'] - df_strength_merge['rec_y']
df_strength_raw['recyd'] = df_strength_merge['recyd_x'] - df_strength_merge['recyd_y']
df_strength_raw['rectd'] = df_strength_merge['rectd_x'] - df_strength_merge['rectd_y']
df_strength_raw.head()

df_strength = df_strength_raw.copy().drop(columns=['team', 'week'])
df_strength = df_strength.groupby(by=['opp', 'pos']).mean()
df_strength = df_strength.reset_index()
df_strength['fpts'] = (df_strength['pyd'] * 0.04) + (df_strength['ptd'] * 5) + (df_strength['int'] * -2) + (df_strength['rshyd'] * 0.1) + (df_strength['rshtd'] * 6) + (df_strength['rec'] * 0.5) + (df_strength['recyd'] * 0.1) + (df_strength['rectd'] * 6)
df_strength.head()

load_dotenv()
df_strength.to_csv(f'{os.getenv('FILES_DIR')}/df_strength.csv')

# -----------------------------------------------------------------------------

df['team-pos'] = df['team'] + '-' + df['pos']
df['name-pos-team'] = df['name'] + '-' + df['pos'] + '-' + df['team']

df_sum_teampos = df.copy().drop(columns=['name'])
df_sum_teampos = df_sum_teampos.groupby(by=['name-pos-team', 'team', 'pos', 'week', 'opp', 'team-pos']).sum()
df_sum_teampos = df_sum_teampos.reset_index()
df_sum_teampos.head(5)

df_avg_teampos = df_sum_teampos.copy().drop(columns=['opp', 'week'])
df_avg_teampos = df_avg_teampos.groupby(by=['name-pos-team', 'team', 'pos', 'team-pos']).mean()
df_avg_teampos = df_avg_teampos.reset_index()
df_avg_teampos.head(5)

df_strength_merge = pd.merge(df_sum_teampos, df_avg_teampos, on=['name-pos-team'], how='left')
df_strength_merge.head()

df_strength_raw = pd.DataFrame()
df_strength_raw['name-pos-team'] = df_strength_merge['name-pos-team']
df_strength_raw['opp'] = df_strength_merge['opp']
df_strength_raw['pos'] = df_strength_merge['pos_x']
df_strength_raw['team'] = df_strength_merge['team_x']
df_strength_raw['week'] = df_strength_merge['week']
df_strength_raw['att'] = df_strength_merge['att_x'] - df_strength_merge['att_y']
df_strength_raw['cmp'] = df_strength_merge['cmp_x'] - df_strength_merge['cmp_y']
df_strength_raw['pyd'] = df_strength_merge['pyd_x'] - df_strength_merge['pyd_y']
df_strength_raw['ptd'] = df_strength_merge['ptd_x'] - df_strength_merge['ptd_y']
df_strength_raw['int'] = df_strength_merge['int_x'] - df_strength_merge['int_y']
df_strength_raw['rsh'] = df_strength_merge['rsh_x'] - df_strength_merge['rsh_y']
df_strength_raw['rshyd'] = df_strength_merge['rshyd_x'] - df_strength_merge['rshyd_y']
df_strength_raw['rshtd'] = df_strength_merge['rshtd_x'] - df_strength_merge['rshtd_y']
df_strength_raw['targ'] = df_strength_merge['targ_x'] - df_strength_merge['targ_y']
df_strength_raw['rec'] = df_strength_merge['rec_x'] - df_strength_merge['rec_y']
df_strength_raw['recyd'] = df_strength_merge['recyd_x'] - df_strength_merge['recyd_y']
df_strength_raw['rectd'] = df_strength_merge['rectd_x'] - df_strength_merge['rectd_y']
df_strength_raw.head()

df_strength_sum = df_strength_raw.copy().drop(columns=['name-pos-team'])
df_strength_sum = df_strength_sum.groupby(by=['opp', 'pos', 'team', 'week']).sum()
df_strength_sum = df_strength_sum.reset_index()
df_strength_sum.head(5)

df_strength = df_strength_sum.copy().drop(columns=['team', 'week'])
df_strength = df_strength.groupby(by=['opp', 'pos']).mean()
df_strength = df_strength.reset_index()
df_strength['fpts'] = (df_strength['pyd'] * 0.04) + (df_strength['ptd'] * 5) + (df_strength['int'] * -2) + (df_strength['rshyd'] * 0.1) + (df_strength['rshtd'] * 6) + (df_strength['rec'] * 0.5) + (df_strength['recyd'] * 0.1) + (df_strength['rectd'] * 6)
df_strength.head()

load_dotenv()
df_strength.to_csv(f'{os.getenv('FILES_DIR')}/df_strength_2.csv')