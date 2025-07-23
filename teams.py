import requests
from neo4j import GraphDatabase

URI = 'neo4j+s://de9f0fd0.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '3xRqM8a5BtXMRp-SlrheVsU0lz3sd5JPpxJJqxsjRbM'

TEAM_REGION_MAP = {
    # Europe (EU)
    '8291895': 'WEU',       # Tundra Esports
    '2163': 'WEU',          # Liquid    
    '8599101': 'WEU',       # Gaimin Gladiators    
    '9498970': 'WEU',       # AVULUS    
    '1838315': 'WEU',       # Team Secret   
    '8291895': 'WEU',       # Tundra Esports    

    # Commonwealth of Independent States (CIS)
    '7119388': 'CIS',       # Team Spirit
    '9824702': 'CIS',       # PARIVISION
    '9131584': 'CIS',       # BB Team
    '9467224': 'CIS',       # Aurora Gaming
    '36': 'CIS',            # Natus Vincere
    '9823272': 'CIS',       # Team Yandex
    '8724984': 'CIS',       # Virtus.pro
    

    # China (CN)
    '9640842': 'CN',        # Team Tidebound
    '8261500': 'CN',        # Xtreme Gaming

    # Southeast Asia (SEA)
    '8597976': 'SEA',       # Talon
    '7732977': 'SEA',       # BOOM Esports
    '9691969': 'SEA',       # Team Nemesis


    # North America (NA)
    '39': 'NA',             # Shopify Rebellion

    # South America (SA)
    '9303484': 'SA',        # HEROIC

    # Middle East and North Africa (MENA)
    '9247354': 'MENA',      # Team Falcons
    '7554697': 'MENA',      # Nigma Galaxy
}

class Team:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ingest_teams(self):
        print('Fetching from Opendota API')
        try:
            response = requests.get('https://api.opendota.com/api/teams')
            response.raise_for_status()
            teams_data = response.json()
            print(f'fetched {len(teams_data)}')
        except requests.exceptions.RequestException as e:
            print(e)
            return    
        
        query = """
        UNWIND $teams AS team_data
        
        WITH team_data WHERE toString(team_data.team_id) IN keys($team_map)

        MATCH (r:Region {name: $team_map[toString(team_data.team_id)]})

        MERGE (t:Team {team_id: team_data.team_id})
        SET
            t.name = team_data.name,
            t.tag = team_data.tag,
            t.wins = team_data.wins,
            t.losses = team_data.losses
            
        MERGE (t)-[:FROM_REGION]->(r)
        
        RETURN count(t) AS teams_processed
        """

        with self.driver.session(database='neo4j') as session:
            result = session.run(query, teams=teams_data, team_map=TEAM_REGION_MAP)

            summary = result.single() 
            print(summary['teams_processed'])  

if __name__ == '__main__':
    ingestor = Team(URI, USERNAME, PASSWORD)
    ingestor.ingest_teams()
    ingestor.close()

    print('Done')            