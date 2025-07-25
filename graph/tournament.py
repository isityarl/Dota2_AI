import requests
from neo4j import GraphDatabase
import time
import pandas as pd

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'


class Tournament:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def tours(self, filename='tours_id.csv'):
        data = pd.read_csv(filename)
        ids = data['leagueid'].tolist()
        return set(ids)

    def ingest_tournament(self):
        to_ingest = self.tours()
        try:
            response = requests.get('https://api.opendota.com/api/leagues')        
            response.raise_for_status()
            all_tours = response.json()
            print(f'len: {len(all_tours)}')
        except requests.exceptions.RequestException as e:
            print(e)
            return

        tier1_tours = [t for t in all_tours if t.get('leagueid') in to_ingest]

        query = """
        UNWIND $tournaments AS tournament_data
        MERGE (t:Tournament {league_id: tournament_data.leagueid})
        SET
            t.name = tournament_data.name,
            t.tier = tournament_data.tier,
            t.region = tournament_data.region
        RETURN count(t) AS tournaments_processed
        """

        with self.driver.session(database='neo4j') as session:
            result = session.run(query, tournaments = tier1_tours)
            summary = result.single()
            if summary:
                print(f'processed: {summary['tournaments_processed']}')

if __name__ == '__main__':
    ingestor = Tournament(URI, USERNAME, PASSWORD)
    ingestor.ingest_tournament()
    ingestor.close()

    print('Done')          