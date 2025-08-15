from neo4j import GraphDatabase

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'

class Region:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_regions(self):
        query = """
        UNWIND ['EU', 'CIS', 'NA', 'SA', 'CN', 'SEA', 'MENA'] AS region_name
        MERGE (r:Region {name: region_name})
        RETURN r.name AS name
        """

        with self.driver.session(database='neo4j') as session:
            result = session.run(query)

            created_regions = [record['name'] for record in result]
            print('Creation done')

if __name__== '__main__':
    print('Connecting to db')
    creator = Region(URI, USERNAME, PASSWORD)
    
    creator.create_regions()
    creator.close()
    
