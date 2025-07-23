import requests
from neo4j import GraphDatabase
import time

URI = 'neo4j+s://de9f0fd0.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '3xRqM8a5BtXMRp-SlrheVsU0lz3sd5JPpxJJqxsjRbM'

MANUAL_PLAYER_MAP = {
    '36': [ # NAVI
        {'name': 'Niku', 'account_id': 185590374},
        {'name': 'pma', 'account_id': 835864135},
        {'name': 'KG_Zayac', 'account_id': 111030315},
        {'name': 'Riddys', 'account_id': 130991304},
        {'name': 'gotthejuice', 'account_id': 957204049},
    ],
    '8724984': [ # Virtus.pro
        {'name': 'V-Tune', 'account_id': 152455523},
        {'name': 'Antares', 'account_id': 161839895},
        {'name': '命運 reinprince', 'account_id': 122817493},
        {'name': 'lorenof', 'account_id': 210053851},
        {'name': 'Daxao', 'account_id': 177411785},
    ],
    '9823272': [ # Team Yandex
        {'name': 'prblms', 'account_id': 417235485},
        {'name': 'TA2000', 'account_id': 96183976},
        {'name': 'CHIRA_JUNIOR', 'account_id': 312436974},
        {'name': 'Noticed', 'account_id': 195108598},
        {'name': 'Solo', 'account_id': 134556694},
    ],

}

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
                players_to_add = []
                try:
                    response = requests.get(f"https://api.opendota.com/api/teams/{team_id}/players")
                    response.raise_for_status()
                    players_data = response.json()

                    active_players = [p for p in players_data if p.get('is_current_team_member')]
                    if active_players:
                        print(f"Found {len(active_players)} players")
                        players_to_add.extend(active_players)
                   
                except requests.exceptions.RequestException as e:
                    print(e)

                team_id_str = str(team_id)
                if team_id_str in MANUAL_PLAYER_MAP:
                    manual_players = MANUAL_PLAYER_MAP[team_id_str]
                    print(f'{len(manual_players)} found manual players')
                    players_to_add.extend(manual_players)

                if players_to_add:
                    unique_players = list({p['account_id']: p for p in players_to_add}.values())
                    print(f"Ingesting a total of {len(unique_players)} unique players for this team.")
                    self.run_query(session, unique_players, team_id)
                else:
                    print(f"     No players to ingest for team {team_id}.")
                    
                time.sleep(3)
                
    def run_query(self, session, players, team_id):
        """Helper function to run the Cypher query for a list of players."""
        query = """
        UNWIND $players AS player_data
        MATCH (t:Team {team_id: $team_id})
        MERGE (p:Player {account_id: player_data.account_id})
        SET p.name = player_data.name
        MERGE (p)-[r:PLAYS_FOR]->(t)
        """
        session.run(query, players=players, team_id=team_id)

if __name__ == '__main__':
    ingestor = Player(URI, USERNAME, PASSWORD)
    ingestor.ingest_players()
    ingestor.close()

    print('Done') 