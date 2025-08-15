import requests
from neo4j import GraphDatabase
import time

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'

class Item:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ingest_heroes(self):
        try:
            response = requests.get('https://api.opendota.com/api/heroes')
            response.raise_for_status()
            heroes_data = response.json()
            print(f'got {len(heroes_data)} data')   
        except requests.exceptions.RequestException as e:
            print(e)

        query = """
        UNWIND $heroes AS hero_data
        // Use the hero's unique ID to create the node
        MERGE (h:Hero {id: hero_data.id})
        SET
            // The API provides a 'localized_name' which is the proper name
            h.name = hero_data.localized_name,
            h.primary_attribute = hero_data.primary_attr,
            h.attack_type = hero_data.attack_type
        RETURN count(h) AS heroes_processed
        """

        with self.driver.session(database='neo4j') as session:
            result = session.run(query, heroes=heroes_data)

if __name__=='__main__':
    ingestor = Item(URI, USERNAME, PASSWORD)
    ingestor.ingest_heroes()
    ingestor.close()
    print('Done')