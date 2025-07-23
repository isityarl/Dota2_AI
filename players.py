import requests
from neo4j import GraphDatabase
import time

URI = 'neo4j+s://de9f0fd0.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '3xRqM8a5BtXMRp-SlrheVsU0lz3sd5JPpxJJqxsjRbM'

class Player:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ingest_players(self):
        with self.driver.session(database='neo4j') as session:
            take_teams_ids = session.run('MATCH (t:Team) RETURN t.team_id AS id')
            teams_ids = [record['id'] for record in take_teams_ids]    

            if not teams_ids:
                print('no teams')
                return
            print(f'{len(teams_ids)} team found')

            for team_id in teams_ids:
                print(f'Processing id: {team_id}')
                try:
                    response = requests.get(f"https://api.opendota.com/api/teams/{team_id}/players")
                    response.raise_for_status()
                    players_data = response.json()

                    active_players = [p for p in players_data if p.get('is_current_team_member')]
                    print(f'found {len(active_players)} active players')

                    query = """
                    UNWIND $players AS player_data
                    // Find the team this player belongs to
                    MATCH (t:Team {team_id: $team_id})
                    
                    // Merge the player node using their unique account_id
                    MERGE (p:Player {account_id: player_data.account_id})
                    SET p.name = player_data.name
                    
                    // Merge the relationship to show they play for the team
                    MERGE (p)-[r:PLAYS_FOR]->(t)
                    
                    RETURN count(p) as players_processed
                    """

                    session.run(query=query, players=active_players, team_id=team_id)

                except requests.exceptions.RequestException as e:
                    print(e)

                time.sleep(1)        

if __name__ == '__main__':
    ingestor = Player(URI, USERNAME, PASSWORD)
    ingestor.ingest_players()
    ingestor.close()

    print('Done') 