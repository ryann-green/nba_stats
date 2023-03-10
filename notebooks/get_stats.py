import nba_api.stats.endpoints as nba
import pandas as pd
import warnings
warnings.simplefilter('ignore')
from sqlalchemy import create_engine
from urllib.parse import quote


# get the current season
current_seasons=nba.teamgamelogs.SeasonNullable.current_season
print(current_seasons)



# function to add table to local database
def add_table(df,table):

    # script to create sql database for needed tables
    engine = create_engine('mysql+pymysql://root:AmariG2021!@localhost/nba_stats')
    df.to_sql(f"{table}",con=engine, if_exists='replace')
    
# get all teams
from nba_api.stats.static import teams

# get_teams returns a list of 30 dictionaries, each an NBA team.
nba_teams = teams.get_teams()
nba_teams_real=pd.DataFrame(nba_teams)

add_table(nba_teams_real,"teams")


# get the team game logs
df=nba.teamgamelogs.TeamGameLogs(season_nullable=current_seasons).get_data_frames()[0]

team_games=[]

for i,row in df.iterrows():
    team_game= f"{row['TEAM_ABBREVIATION']}_{row['GAME_ID']}"
    team_games.append(team_game)
df['team_games']=team_games
true_df=df.set_index('team_games')
true_df.index.name = 'team_game'

add_table(true_df.reset_index(),'team_box')

# get player information for active players
from nba_api.stats.static import players

nba_players = players.get_active_players()
print('Number of active players fetched: {}'.format(len(nba_players)))

nba_players_real=pd.DataFrame(nba_players)
add_table(nba_players_real,'players')

# get player game logs  
from nba_api.stats import endpoints

player_game_logs=endpoints.playergamelogs.PlayerGameLogs(season_nullable=current_seasons).get_data_frames()[0]
player_game_logs.index.name= "index"

player_game_logs_real=player_game_logs[['SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID','TEAM_ABBREVIATION', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
       'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM',
       'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK',
       'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS']]

add_table(player_game_logs_real.reset_index(),'player_box')

# Get top 75 players of the season

top_75_players=endpoints.leagueleaders.LeagueLeaders(season=current_seasons).get_data_frames()[0].iloc[:75]["PLAYER_ID"]
top_75_players
seasons=['2020-21','2021-22','2022-23']

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
            # time.sleep(1)
        except:
            print(f"No data for {player} in {season}")
            
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

add_table(top_75,'top_players')