import requests
from neo4j import GraphDatabase
import time

URI = 'neo4j+s://9fa0005b.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '2OGzHvL34RYo6zDxtqKxsWl6ktfkjVpvjV5_Hod-Mq8'

class Match:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()    

    def ingest_matches(self):
        with self.driver.session(database='neo4j') as session:
            leagues_ids = self.get_tour_id(session)
            if not leagues_ids:
                return
            
            for league_id in leagues_ids:
                match_ids = self.get_match_id(league_id)
                if not match_ids:
                    print('no matches')
                    continue
                
                for match_id in match_ids:
                    if self.get_match_existance(session, match_id):
                        print(f'match {match_id} already exists in db')
                        continue

                    match_details = self.get_match_details(match_id)
                    if match_details:
                        self.get_match_data(session, match_details, league_id)
                        print(f'succes with match {match_id}')

                    time.sleep(2)    


    def get_tour_id(self, session):
        result = session.run("MATCH (t:Tournament) RETURN t.league_id AS leagueId")
        ids = [record['leagueId'] for record in result]
        if not ids:
            print('no tournament')
        return ids
    
    def get_match_id(self, league_id):
        try:
            response = requests.get(f'https://api.opendota.com/api/leagues/{league_id}/matches')
            response.raise_for_status()
            matches = response.json()
            return [m['match_id'] for m in matches]
        except requests.exceptions.RequestException as e:
            print(f'for league_id: {e}')
            return []
        
    def get_match_existance(self, session, match_id):
        result = session.run('MATCH (m:Match {match_id: $id}) RETURN m', id=match_id)
        return result.single() is not None
    
    def get_match_details(self, match_id):
        try:
            response = requests.get(f'https://api.opendota.com/api/matches/{match_id}')
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f'for match_id: {e}')
            return None
        
    def get_match_data(self, session, match_details, league_id):
        params = {
            'league_id': league_id,
            'match_id': match_details.get('match_id'),
            'duration': match_details.get('duration', 0),
            'start_time': match_details.get('start_time', 0),
            'radiant_win': match_details.get('radiant_win', False),
            'radiant_team_id': match_details.get('radiant_team_id'),
            'dire_team_id': match_details.get('dire_team_id'),
            'sanitized_players': []
        }

        if match_details.get('players'):
            for player_perf in match_details['players']:
                if not player_perf.get('account_id') or not player_perf.get('hero_id'):
                    continue
                
                params['sanitized_players'].append({
                    'account_id': player_perf.get('account_id'),
                    'hero_id': player_perf.get('hero_id'),
                    'kills': player_perf.get('kills', 0),
                    'deaths': player_perf.get('deaths', 0),
                    'assists': player_perf.get('assists', 0),
                    'gpm': player_perf.get('gold_per_min', 0),
                    'xpm': player_perf.get('xp_per_min', 0),
                    'net_worth': player_perf.get('net_worth', 0),
                    'hero_damage': player_perf.get('hero_damage', 0),
                    'tower_damage': player_perf.get('tower_damage', 0)
                })

        if not params['sanitized_players']:
            print("No valid player performance data found in this match.")
            return

        query = """
        MATCH (tourn:Tournament {league_id: $params.league_id})
        MERGE (m:Match {match_id: $params.match_id})
        SET
            m.duration_seconds = $params.duration,
            m.start_time = datetime({epochSeconds: $params.start_time}),
            m.radiant_win = $params.radiant_win
        MERGE (m)-[:PART_OF]->(tourn)

        WITH m, $params AS prms
        OPTIONAL MATCH (radiant_team:Team {team_id: prms.radiant_team_id})
        OPTIONAL MATCH (dire_team:Team {team_id: prms.dire_team_id})
        FOREACH (ignoreMe IN CASE WHEN radiant_team IS NOT NULL THEN [1] ELSE [] END |
            MERGE (radiant_team)-[:PLAYED_IN]->(m)
        )
        FOREACH (ignoreMe IN CASE WHEN dire_team IS NOT NULL THEN [1] ELSE [] END |
            MERGE (dire_team)-[:PLAYED_IN]->(m)
        )

        WITH m, prms.sanitized_players AS players_perf
        UNWIND players_perf AS player_perf
        
        MATCH (p:Player {account_id: player_perf.account_id})
        MATCH (h:Hero {id: player_perf.hero_id})

        CREATE (perf:Performance {
            kills: player_perf.kills,
            deaths: player_perf.deaths,
            assists: player_perf.assists,
            gpm: player_perf.gpm,
            xpm: player_perf.xpm,
            net_worth: player_perf.net_worth,
            hero_damage: player_perf.hero_damage,
            tower_damage: player_perf.tower_damage
        })

        CREATE (p)-[:HAD_PERFORMANCE]->(perf)
        CREATE (perf)-[:IN_MATCH]->(m)
        CREATE (perf)-[:AS_HERO]->(h)
        """
        session.run(query, params=params)


if __name__=='__main__':
    ingestor = Match(URI, USERNAME, PASSWORD)
    ingestor.ingest_matches()
    ingestor.close()

    print('Done')