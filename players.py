import requests
from neo4j import GraphDatabase
import time

URI = 'neo4j+s://de9f0fd0.databases.neo4j.io'
USERNAME = 'neo4j'
PASSWORD = '3xRqM8a5BtXMRp-SlrheVsU0lz3sd5JPpxJJqxsjRbM'

MANUAL_PLAYER_MAP = {
    #CIS
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
    '9467224': [ # Aurora Gaming
        {'name': 'TORONTOTOKYO', 'account_id': 431770905},
        {'name': 'panto', 'account_id': 108958769},
        {'name': 'kiyotaka', 'account_id': 858106446},
        {'name': 'Nightfall', 'account_id': 124801257},
        {'name': 'Mira', 'account_id': 256156323},
    ],
    '9131584': [ # BB Team
        {'name': 'gpk~', 'account_id': 480412663},
        {'name': 'Save-', 'account_id': 317880638},
        {'name': 'Pure', 'account_id': 331855530},
        {'name': 'MieRo', 'account_id': 165564598},
        {'name': 'Kataomi`', 'account_id': 196878136},
    ],
    '9824702': [ # PARIVISION
        {'name': 'Satanic', 'account_id': 1044002267},
        {'name': 'DM', 'account_id': 56351509},
        {'name': 'Dukalis', 'account_id': 73401082},
        {'name': '9Class', 'account_id': 164199202},
        {'name': 'No[o]ne-', 'account_id': 106573901},
    ],
    '7119388': [ # Team Spirit
        {'name': 'Yatoro', 'account_id': 321580662},
        {'name': 'Miposhka', 'account_id': 113331514},
        {'name': 'Collapse', 'account_id': 302214028},
        {'name': 'Larl', 'account_id': 106305042},
        {'name': 'rue', 'account_id': 847565596},
    ],

    #EU
    '8291895': [ # Tundra Esports
        {'name': '33', 'account_id': 86698277},
        {'name': 'Saksa', 'account_id': 103735745},
        {'name': 'Whitemon', 'account_id': 136829091},
        {'name': 'Topson', 'account_id': 94054712},
        {'name': 'bzm', 'account_id': 93618577},
    ],
    '2163': [ # Liquid
        {'name': 'Insania', 'account_id': 54580962},
        {'name': 'Boxi', 'account_id': 77490514},
        {'name': 'm1CKe', 'account_id': 152962063},
        {'name': 'Nisha', 'account_id': 201358612},
        {'name': 'Saberlight', 'account_id': 126212866},
    ],
    '8599101': [ # Gaimin Gladiators
        {'name': 'watson`', 'account_id': 171262902},
        {'name': 'Malady', 'account_id': 93817671},
        {'name': 'Ace', 'account_id': 97590558},
        {'name': 'tOfu', 'account_id': 16497807},
        {'name': 'Quinn', 'account_id': 221666230},
    ],
    '9498970': [ # AVULUS
        {'name': 'Fly', 'account_id': 94155156},
        {'name': 'Worick', 'account_id': 72393079},
        {'name': 'Smiling Knight', 'account_id': 155162307},
        {'name': 'dEsire', 'account_id': 115464954},
        {'name': 'Xibbe', 'account_id': 50580004},
    ],

    #CN
    '9640842': [ # Team Tidebound
        {'name': 'shiro', 'account_id': 320252024},
        {'name': 'NothingToSay', 'account_id': 173978074},
        {'name': 'Bach', 'account_id': 118134220},
        {'name': 'planet', 'account_id': 150961567},
        {'name': 'y`', 'account_id': 111114687},
    ],
    '8261500': [ # Xtreme Gaming
        {'name': 'Ame', 'account_id': 898754153},
        {'name': 'Xm', 'account_id': 137129583},
        {'name': 'Xxs', 'account_id': 129958758},
        {'name': 'XinQ', 'account_id': 157475523},
        {'name': 'poloson', 'account_id': 76904792},
    ],

    #SEA
    '8597976': [ # Talon
        {'name': 'Kuku', 'account_id': 184950344},
        {'name': 'Mikoto', 'account_id': 301750126},
        {'name': 'Jhocam', 'account_id': 152859296},
        {'name': 'Ws', 'account_id': 126842529},
        {'name': '23savage', 'account_id': 375507918},
    ],
    '7732977': [ # BOOM Esports
        {'name': 'JaCkky', 'account_id': 392565237},
        {'name': 'TIMS', 'account_id': 155494381},
        {'name': 'Jabz', 'account_id': 100471531},
        {'name': 'Armel', 'account_id': 164532005},
        {'name': 'Jaunuel', 'account_id': 148526973},
    ],
    '9691969': [ # Team Nemesis
        {'name': 'Erice`', 'account_id': 100598959},
        {'name': 'Mac', 'account_id': 104512126},
        {'name': 'Jîng', 'account_id': 219755398},
        {'name': 'raven', 'account_id': 132309493},
        {'name': 'Akashi', 'account_id': 330534326},
    ],

    #NA
    '39': [ # Shopify Rebellion
        {'name': 'Davai Lama', 'account_id': 138880576},
        {'name': 'Yopaj-', 'account_id': 324277900},
        {'name': 'diabloii', 'account_id': 97658618},
        {'name': 'Hellscream', 'account_id': 241884166},
        {'name': 'skem', 'account_id': 100594231},
    ],

    #SA
    '9303484': [ # HEROIC
        {'name': 'Yuma', 'account_id': 177203952},
        {'name': '4nalog', 'account_id': 131303632},
        {'name': 'Wisper', 'account_id': 292921272},
        {'name': 'Scofield', 'account_id': 157989498},
        {'name': 'KingJungles', 'account_id': 81306398},
    ],

    #MENA
    '9247354': [ # Team Falcons
        {'name': 'ATF', 'account_id': 183719386},
        {'name': 'Cr1t-', 'account_id': 25907144},
        {'name': 'skiter', 'account_id': 100058342},
        {'name': 'Sneyking', 'account_id': 10366616},
        {'name': 'Malr1ne', 'account_id': 898455820},
    ],
    '7554697': [ # Nigma Galaxy
        {'name': 'SumaiL', 'account_id': 111620041},
        {'name': 'No!ob', 'account_id': 140297552},
        {'name': 'OmaR', 'account_id': 152168157},
        {'name': 'GH', 'account_id': 101356886},
        {'name': 'KuroKy', 'account_id': 206642367},
    ],
}

class Player:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ingest_players_manual(self):
        with self.driver.session(database='neo4j') as session:
            for team_id_str, players in MANUAL_PLAYER_MAP.items():
                team_id = int(team_id_str)
                print(f'Team{team_id} ({len(players)} players)')

                self.run_query(session, players, team_id)
                
    def run_query(self, session, players, team_id):
        """Helper function to run the Cypher query for a list of players."""
        query = """
        // Find the team node first. This will fail gracefully if the team doesn't exist.
        MATCH (t:Team {team_id: $team_id})
        // Now, process the players for this team
        UNWIND $players AS player_data
        MERGE (p:Player {account_id: player_data.account_id})
        SET p.name = player_data.name
        // Create the relationship
        MERGE (p)-[r:PLAYS_FOR]->(t)
        """
        session.run(query, players=players, team_id=team_id)

if __name__ == '__main__':
    ingestor = Player(URI, USERNAME, PASSWORD)
    ingestor.ingest_players_manual()
    ingestor.close()

    print('Done') 