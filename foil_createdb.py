import sqlite3
import requests
import json
import os
import time
import pandas as pd
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# requête de création de la table mesure
create_table_mesure = """
    CREATE TABLE IF NOT EXISTS mesure(    
        id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
        date date,
        succes booleen,
        wind_heading float,
        wind_speed_avg float,
        wind_speed_max float,
        wind_speed_min float,
        status text,
        id_station int)   
    """

# requête d'insertion dans la table station
inserer_mesure = """
    INSERT INTO mesure 
    (date, succes, wind_heading, wind_speed_avg, wind_speed_max, wind_speed_min, status, id_station)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""
# requête pour calculer le nombre d'enregistrement dans la table mesure
sql_query1 = """SELECT COUNT(rowid)
               FROM mesure;"""

sql_query2 = """DELETE
                FROM mesure
                WHERE rowid < 10;"""

last_mesure_113 = '''
SELECT *
FROM mesure
WHERE id_station=113
ORDER BY id DESC 
LIMIT 10
'''

last_mesure_307 = '''
SELECT *
FROM mesure
WHERE id_station=307
ORDER BY id DESC
LIMIT 10
'''


last_mesure_308 = '''
SELECT *
FROM mesure
WHERE id_station=308
ORDER BY id DESC
LIMIT 10
'''


# connection (voir création) à la db
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)

    except Error as e:
        print(e)

    return conn


# Créer une table avec SQLite
def create_table(conn, sql_create):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_create)    
        conn.commit()

    except sqlite3.OperationalError:
        print('Erreur la table existe déjà')
    except Exception as e:
        print("Erreur")
        conn.rollback()
        # raise e
    finally:
        cursor.close
        

# fonction pour insérer les données dans la table mesure

def inserer_donnees_mesure(conn, inserer_mesure, liste_station):
    try:
        cursor = conn.cursor()
        # initialisation liste_station
        # elle va receuillir les éléments du dict. json
        # requête API
        for i in liste_station:
            q= str(i)
            requete= "http://api.pioupiou.fr/v1/live/"+q
            response = requests.get(requete)
            #movies.append(response.json())
            objet_mesures = response.json()

            cursor.execute(inserer_mesure, (objet_mesures['data']['measurements']['date'], 
                                                objet_mesures['data']['location']['success'], 
                                                objet_mesures['data']['measurements']['wind_heading'],
                                                objet_mesures['data']['measurements']['wind_speed_avg'],
                                                objet_mesures['data']['measurements']['wind_speed_max'],
                                                objet_mesures['data']['measurements']['wind_speed_min'],
                                                objet_mesures['data']['status']['state'],
                                                objet_mesures['data']['id']))

        conn.commit()

   
    except sqlite3.Error as e:
        print("Erreur lors de l'insertion des données")
        print(e)
        return
    cursor.close()
    print("Les données ont été insérées avec succès")


def backupdb(status, remaining, total, db_file):
    try:
        # existing DB
        sqliteCon = sqlite3.connect(db_file)
        # copy into this DB
        backupCon = sqlite3.connect('data/Sqlite_backup.db')
        with backupCon:
            sqliteCon.backup(backupCon, pages=3, progress=progress)
        print("backup successful")
    except sqlite3.Error as error:
        print("Error while taking backup: ", error)
    finally:
        if backupCon:
            backupCon.close()
        

def query_count(conn, sql_query):
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        count = cursor.fetchall()
        return (count[0][0])

    except sqlite3.Error as e:
        print("Erreur dans le compte des données")
        print(e)
    

def main():
    # connection/création de la BDD Foil.db
    liste_station = [113, 308, 307]
    i=0
    conn = create_connection('data/foil.db')

    # création table mesure
    create_table(conn, create_table_mesure)

    while True:
        # requete pour insérer données dans la table mesure
        inserer_donnees_mesure(conn, inserer_mesure, liste_station)
        i +=1
        
        variable = query_count(conn, sql_query1) 
        
        if variable > 5:
            backupdb(status, remaining, total, db_file)

        else:
            continue

        
        last_mesures_113 = pd.read_sql_query(last_mesure_113, conn)
        list_last_mesures_113 = last_mesures_113.values.tolist()

        last_mesures_307 = pd.read_sql_query(last_mesure_307, conn)
        list_last_mesures_307 = last_mesures_307.values.tolist()

        last_mesures_308 = pd.read_sql_query(last_mesure_308, conn)
        list_last_mesures_308 = last_mesures_308.values.tolist()
        #Création d'un tableau de bord
        file_loader = FileSystemLoader('templates')
        env = Environment(loader=file_loader, autoescape=True)

        template = env.get_template('index.html')

        filename = "html/index.html"
        with open(filename, 'w') as dashboard:
            dashboard.write(template.render(data_mesures_113=list_last_mesures_113, data_mesures_307=list_last_mesures_307, data_mesures_308=list_last_mesures_308))
        time.sleep(15)



if __name__ == '__main__':
    main()