#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
import pymysql
import numpy as np

# Chemin vers votre fichier CSV contenant les données des passagers
csv_file_path = 'train.csv'

# Se connecter à la base de données
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="20030701Ae.moon"
)

print(mydb)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE workshoptitanic")


# In[4]:


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="20030701Ae.moon",
  database="workshoptitanic"
)

mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE Passengers (PassengerId INT AUTO_INCREMENT PRIMARY KEY,Survived TINYINT(1) NOT NULL CHECK (survived IN (0, 1)) DEFAULT 0,Pclass TINYINT(1) NOT NULL CHECK (Pclass IN (1, 2, 3)) DEFAULT 1,Name VARCHAR(255),Sex VARCHAR(10),Age INT,SibSp INT,Parch INT,Ticket VARCHAR(255),Fare FLOAT,Cabin VARCHAR(255) DEFAULT NULL,Embarked VARCHAR(255))")


# In[5]:


# Se connecter à la base de données
mydb = pymysql.connect(host='localhost',
                       user='root',
                       password='20030701Ae.moon',
                       database='workshoptitanic')

# Créer un curseur pour exécuter des requêtes SQL
cursor = mydb.cursor()

# Chemin vers votre fichier CSV contenant les données des passagers
csv_file_path = 'train.csv'

try:
    # Lecture du fichier CSV avec pandas
    data = pd.read_csv(csv_file_path)

    # Remplacer les valeurs NaN par des valeurs nulles ou des chaînes vides
    data = data.replace({np.nan: None})  # Remplacer les NaN par des valeurs None

    # Insertion des données dans la table 'passengers'
    for index, row in data.iterrows():
        sql = """INSERT INTO passengers (Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        values = (row['Survived'], row['Pclass'], row['Name'], row['Sex'], row['Age'], row['SibSp'], row['Parch'],
                  row['Ticket'], row['Fare'], row['Cabin'], row['Embarked'])

        cursor.execute(sql, values)

    # Valider la transaction
    mydb.commit()
    print("Données insérées avec succès dans la table 'passengers'.")
except FileNotFoundError:
    print("Le fichier CSV spécifié est introuvable.")
except pd.errors.EmptyDataError:
    print("Le fichier CSV spécifié est vide.")
except pymysql.Error as e:
    # En cas d'erreur, annuler la transaction
    mydb.rollback()
    print(f"Erreur lors de l'insertion des données : {e}")

# Fermer le curseur et la connexion à la base de données
cursor.close()
mydb.close()


# In[ ]:




