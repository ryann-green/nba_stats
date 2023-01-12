# !/usr/bin/env python
# coding: utf-8

# In[ ]:
print ("Hi")

from nba_api.stats.endpoints import teamgamelogs
import pandas as pd
import warnings
import time
warnings.simplefilter('ignore')

current_seasons=teamgamelogs.SeasonNullable.current_season
print(current_seasons)

# import json
# # Opening JSON file
# f = open('../gs_creds/client_secret.json')

# # returns JSON object as 
# # a dictionary
# data = json.load(f)
# for i in data:
#     print(i)


# In[2]:


df=teamgamelogs.TeamGameLogs(season_nullable=current_seasons).get_data_frames()[0]


# In[3]:


team_games=[]
for i,row in df.iterrows():
    team_game= f"{row['TEAM_ABBREVIATION']}_{row['GAME_ID']}"
    team_games.append(team_game)
df['team_games']=team_games
true_df=df.set_index('team_games')
true_df.index.name = 'team_game'
true_df




# In[4]:


from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

# The path of client_secret json file that you have downloaded
# path = '/nfs/home/data/'
 
scopes = ['https://www.googleapis.com/auth/spreadsheets', 
          "https://www.googleapis.com/auth/drive.file", 
          "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("./gs_creds/client_secret.json", scopes)
def write_googlesheet(df,spreadsheet_key,sheet_title,starting_cell,overwrite):
    d2g.upload(df,
           spreadsheet_key,
           sheet_title,
           credentials=creds,
           col_names=True,
           row_names=True,
           start_cell = starting_cell,
           clean=overwrite)


# In[5]:


true_df.columns


# In[6]:


write_googlesheet(true_df,'1WScIAA7DFu1VuhBjQ2H86KoAWG0K7Qvg6EpRpG6S68I','game_logs','A1',True)  


# In[7]:


true_df


# #### 

# In[8]:


from nba_api.stats import endpoints


# In[9]:


player_game_logs=endpoints.playergamelogs.PlayerGameLogs(season_nullable=current_seasons).get_data_frames()[0]
player_game_logs.index.name= "index"


# In[10]:


player_game_logs_real=player_game_logs[['SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID','TEAM_ABBREVIATION', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
       'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM',
       'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK',
       'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS']]
player_game_logs_real


# In[11]:


write_googlesheet(player_game_logs_real,'1WScIAA7DFu1VuhBjQ2H86KoAWG0K7Qvg6EpRpG6S68I','player_logs','A1',True) 


# In[12]:


from nba_api.stats.static import teams
# get_teams returns a list of 30 dictionaries, each an NBA team.
nba_teams = teams.get_teams()
print('Number of teams fetched: {}'.format(len(nba_teams)))
nba_teams_real=pd.DataFrame(nba_teams)
write_googlesheet(nba_teams_real,'1WScIAA7DFu1VuhBjQ2H86KoAWG0K7Qvg6EpRpG6S68I','teams','A1',True) 


# In[13]:


team_ids=nba_teams_real['id']
team_ids


# In[14]:


from nba_api.stats.static import players


# In[15]:


nba_players = players.get_active_players()
print('Number of active players fetched: {}'.format(len(nba_players)))


# In[16]:


nba_players_real=pd.DataFrame(nba_players)
nba_players_real


# In[17]:


player_ids=nba_players_real['id']
player_ids


# In[18]:


write_googlesheet(nba_players_real,'1WScIAA7DFu1VuhBjQ2H86KoAWG0K7Qvg6EpRpG6S68I','players','A1',True) 


# In[19]:


top_75_players=endpoints.leagueleaders.LeagueLeaders(season=current_seasons).get_data_frames()[0].iloc[:75]["PLAYER_ID"]
top_75_players


# In[20]:


seasons=['2020-21','2021-22','2022-23']


# In[21]:


# ls=[]
# dict={}
# for season in seasons:
#     for team in team_ids[:3]:
#         for player in top_75_players [:3]:
#             try:
#                 player_vs_team=endpoints.teamvsplayer.TeamVsPlayer(vs_player_id=player,team_id=team,opponent_team_id=team,season=season).vs_player_overall.get_data_frame()
#                 pvt_df=player_vs_team[['GROUP_VALUE', 'PLAYER_ID', 'GP', 'W_PCT',
#                        'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA',
#                        'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA',
#                        'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS']]
#                 pvt_df['team_id']=team
#                 pvt_df['season']=season
#                 ls.append(pvt_df)
#             except:
#                 print("Variable not found")
    


# In[ ]:





# In[22]:


# ls=[]
# dict={}
# for season in seasons:
#     for team in team_ids:
#         for player in top_75_players:
#             try:
#                 player_vs_team=endpoints.teamvsplayer.TeamVsPlayer(vs_player_id=player,team_id=team,opponent_team_id=team,season=season,season_type_playoffs="Regular Season").vs_player_overall.get_data_frame()
#                 pvt_df=player_vs_team[['GROUP_VALUE', 'PLAYER_ID', 'GP', 'W_PCT',
#                        'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA',
#                        'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA',
#                        'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS']]
#                 pvt_df['team_id']=team
#                 pvt_df['season']=season
#                 ls.append(pvt_df)
#                 time.sleep(.600)
#             except:
#                 print(f"data for {player} against {team} in {season} not found")
            
    


# In[ ]:


top_75_ls=[]
for season in seasons:
    print(f"Starting to get data for {season}")
    for player in top_75_players:
        try:
            print(f"getting data for {player}")
            player_game_logs=endpoints.playergamelogs.PlayerGameLogs(season_nullable=season,player_id_nullable=player).get_data_frames()[0]
            player_game_logs.index.name= "initial_index"
            player_game_logs_real=player_game_logs[['SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID','TEAM_ABBREVIATION', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
           'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM',
           'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK',
           'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS']]
            top_75_ls.append(player_game_logs_real)
            time.sleep(1)
        except:
            print(f"No data for {player} in {season}")


# In[ ]:


top_75=pd.concat(top_75_ls)
opponents=[]
home_or_away=[]
for i,row in top_75.iterrows():
    opponents.append(str(row['MATCHUP'].split(".")[-1].split("@")[-1]))
    if "@" in row['MATCHUP']:
        home_or_away.append("Away")
    else:
        home_or_away.append("Home")
        
top_75['opponents']=opponents
top_75['location']=home_or_away


# In[ ]:


top_75


# In[ ]:


write_googlesheet(top_75.reset_index(),'1WScIAA7DFu1VuhBjQ2H86KoAWG0K7Qvg6EpRpG6S68I','top_75_splits','A1',True) 


# In[ ]:
print("done")



