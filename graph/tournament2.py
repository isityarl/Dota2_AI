import requests
from neo4j import GraphDatabase
import time
import pandas as pd

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'


class Participants:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def team_to_tour(self):
        with self.driver.session(database="neo4j") as session:
            tournament_ids_result = session.run("MATCH (t:Tournament) RETURN t.league_id AS leagueId")
            league_ids = [record["leagueId"] for record in tournament_ids_result]
            
            if not league_ids:
                print('no tours')
                return

            print(f'found {len(league_ids)}')

            for league_id in league_ids:
                try:
                    teams_api_url = f"https://api.opendota.com/api/leagues/{league_id}/teams"
                    response = requests.get(teams_api_url)
                    response.raise_for_status()
                    teams_data = response.json()
                    
                    if not teams_data:
                        print(f'no participating teams found for tournament {league_id}.')
                        continue

                    print(f'Found {len(teams_data)}')
                    
                    query = """
                    // Find the specific tournament we are processing
                    MATCH (tourn:Tournament {league_id: $league_id})
                    // Iterate through the list of teams we pass in
                    UNWIND $teams AS team_data
                    // Find the corresponding team node in our database
                    MATCH (team:Team {team_id: team_data.team_id})
                    // Create the relationship between them
                    MERGE (team)-[:PARTICIPATED_IN]->(tourn)
                    """
                    session.run(query, teams=teams_data, league_id=league_id)

                except requests.exceptions.RequestException as e:
                    print(f"tournament {league_id}: {e}")
                
                time.sleep(5)    


if __name__ == '__main__':
    ingestor = Participants(URI, USERNAME, PASSWORD)
    ingestor.team_to_tour()
    ingestor.close()

    print('done')                        