import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date
from neo4j import GraphDatabase
import time
import os
import re
import pandas as pd

URI = os.environ.get('URI')
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')


class RosterScraper:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def scrap(self):
        headers = {'User-Agent': 'Dota2AI-Scraper/1.0'}
        all = []
        data = pd.read_csv('graph/roster_history.csv')
        print(f'Starting scrape for {len(data)} players...')

        for index, player_info in data.iterrows():
            player_id = player_info['account_id']
            liquipedia_name = player_info['name']
            url = f"https://liquipedia.net/dota2/{liquipedia_name}" 

            print(f"for: {liquipedia_name} ID: {player_id} ")
            
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 404:
                    print(f"  404 Not Found at {url}. Skipping.")
                    continue
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'html.parser')

                history_header = soup.find('div', class_='infobox-header', string='History')
                if not history_header:
                    print(f"no 'History' section for {liquipedia_name}.")
                    continue
                
                infobox_center = history_header.find_next('div', class_='infobox-center')
                if not infobox_center:
                    print(f"no infobox-center div for {liquipedia_name}.")
                    continue
                
                history_table = infobox_center.find('table')
                if not history_table:
                    print(f"no history table for {liquipedia_name}.")
                    continue

                player_rows = history_table.find_all('tr')
                
                for row in player_rows:
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        dates_td = cols[0]
                        team_td = cols[1]
                        
                        date_text = dates_td.text.strip()
                        date_parts = [d.strip() for d in date_text.split('—')]
                        team_name = team_td.text.strip()
                        
                        if len(date_parts) == 2:
                            start_date_str, end_date_str = date_parts
                            try:
                                start_date = parse_date(start_date_str).strftime('%Y-%m-%d')
                                end_date = "Present" if "Present" in end_date_str else parse_date(end_date_str).strftime('%Y-%m-%d')
                                                                
                                print(f"found Played for '{team_name}' from {start_date} to {end_date}")
                                all.append({
                                    'account_id': player_id,
                                    'name': liquipedia_name,
                                    'team_name': team_name,
                                    'start_date': start_date,
                                    'end_date': end_date
                                })

                            except Exception as e:
                                print(f"    Could not parse dates '{date_text}' for team {team_name}: {e}")

            except requests.exceptions.RequestException as e:
                print(f"  Error fetching Liquipedia page: {e}")
            
            time.sleep(2)

        data2 = pd.DataFrame(all)
        data2.to_csv('graph/roster_history.csv', index=False)
        return data

    def update_graph(self, filename='graph/roster_history.csv'):
        data = pd.read_csv(filename)
        data.dropna(subset=['team_name'], inplace=True)
        with self.driver.session(database='neo4j') as session:
            roster_list = data.to_dict('records')
            query = """
            UNWIND $rosters AS roster_row
            MATCH (p:Player {account_id: roster_row.account_id})
            MERGE (t:Team {name: roster_row.team_name})
            MERGE (p)-[r:PLAYS_FOR {start_date: date(roster_row.start_date)}]->(t)
            SET r.end_date = CASE WHEN roster_row.end_date = 'Present' THEN null ELSE date(roster_row.end_date) END
            """
            session.run(query, rosters=roster_list)
        
        print("Graph update complete.")

        

if __name__=='__main__':
    scraper = RosterScraper(URI, USERNAME, PASSWORD)
    scraper.update_graph()
    scraper.close()
    print('Done')
