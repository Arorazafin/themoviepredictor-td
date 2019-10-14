#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
TheMoviePredictor script
Author: Arnaud de Mouhy <arnaud@admds.net>
Modified by Aro Razafindrakola <aro.razafindrakola@gmail.com>
"""

import mysql.connector
import sys
import argparse
import csv
from datetime import date

def connectToDatabase():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')

def disconnectDatabase(cnx):
    cnx.close()

def createCursor(cnx):
    return cnx.cursor(dictionary=True)

def closeCursor(cursor):    
    cursor.close()

def findQuery(table, id):
    return ("SELECT * FROM {} WHERE id = {}".format(table, id))

def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def find(table, id):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = findQuery(table, id)
    cursor.execute(query)
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def findAll(table):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(findAllQuery(table))
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def insertPeople (firstname, lastname):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query  = ("INSERT INTO people(id, firstname, lastname) VALUES (%s, %s, %s)")
  
    id = cursor.lastrowid
    data_people = (id, firstname, lastname)
    cursor.execute(query, data_people)
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)

def insertMovie (title,original_title,synopsis,duration,rating,production_budget,marketing_budget,release_date,is3d):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = ("INSERT INTO `movies`"
               "(id,title,original_title,synopsis,duration,rating,production_budget,marketing_budget,release_date,is3d)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
  
    id = cursor.lastrowid
    if release_date == None:
        release_date = date(9999,1,1)
    if is3d == None:
       is3d = 0 
   # data_movie = (id,'Iron Man','Iron Man',140000000,None,126,'TP', date(2008, 4, 30),0,'')

    """data_movie = {
    'id': id,
    'title': title,
    'original_title' : original_title, 
    'synopsis': synopsis,
    'duration' : duration,
    'rating' : rating,
    'production_budget': production_budget,
    'marketing_budget' : marketing_budget,
    'release_date' : release_date,
    'is3d' : is3d,
    }
    """
    data_movie = (id,title,original_title, synopsis,duration,rating,production_budget,marketing_budget,release_date,is3d)
    #print (data_movie)
    cursor.execute(query, data_movie)
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)



def printPerson(person):
    print("#{}: {} {}".format(person['id'], person['firstname'], person['lastname']))

def printMovie(movie):
    print("#{}: {} released on {}".format(movie['id'], movie['title'], movie['release_date']))

parser = argparse.ArgumentParser(description='Process MoviePredictor data')

parser.add_argument('context', choices=['people', 'movies'], help='Le contexte dans lequel nous allons travailler')

action_subparser = parser.add_subparsers(title='action', dest='action')

list_parser = action_subparser.add_parser('list', help='Liste les entitÃ©es du contexte')
list_parser.add_argument('--export' , help='Chemin du fichier exportÃ©')

find_parser = action_subparser.add_parser('find', help='Trouve une entitÃ© selon un paramÃ¨tre')
find_parser.add_argument('id' , help='Identifant Ã  rechercher')

insert_parser = action_subparser.add_parser('insert', help='Insere des personnes ou des films')
insert_parser.add_argument('--firstname' , help='Prenom de l auteur')
insert_parser.add_argument('--lastname' , help='Nom de l auteur')
#movies
insert_parser.add_argument('--title' , help='Le titre du film')
insert_parser.add_argument('--original_title' , help='Le titre origianl du film')
insert_parser.add_argument('--synopsis' , help='Le synopsis du film')
insert_parser.add_argument('--duration' , help='La durée du film')
insert_parser.add_argument('--rating' , help='Le rating du film')
insert_parser.add_argument('--production_budget' , help='Le montant de la production du film')
insert_parser.add_argument('--marketing_budget' , help='Le montant du marketing du film')
insert_parser.add_argument('--release_date' , help='La date de sortie du film')
insert_parser.add_argument('--is3d' , help=' un film  3D')

import_parser = action_subparser.add_parser('import', help='Liste les entitÃ©es du contexte')
import_parser.add_argument('--file' , help='Chemin du fichier exportÃ©')



args = parser.parse_args()
print (args)
#exit()

if args.context == "people":
    if args.action == "list":
        people = findAll("people")
        if args.export:
            with open(args.export, 'w', encoding='utf-8', newline='\n') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
        else:
            for person in people:
                printPerson(person)
    if args.action == "find":
        peopleId = args.id
        people = find("people", peopleId)
        for person in people:
            printPerson(person)
    if args.action == "insert":
        #print (args.firstname, args.lastname)
        insertPeople (args.firstname, args.lastname)



if args.context == "movies":
    if args.action == "list":  
        movies = findAll("movies")
        for movie in movies:
            printMovie(movie)
    if args.action == "find":  
        movieId = args.id
        movies = find("movies", movieId)
        for movie in movies:
            printMovie(movie)
    if args.action == "insert":
        insertMovie (args.title, args.original_title, args.synopsis, args.duration, args.rating, args.production_budget, args.marketing_budget, args.release_date, args.is3d)  
    if args.action == "import": 
        with open(args.file, 'r', encoding='utf-8', newline='\n') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:  
                #print("#{}".format(row['title'])) 
                #print(row)
                insertMovie (row['title'], row['original_title'], None, row['duration'], row['rating'], None, None, row['release_date'], 0)

"""                
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
                    insertMovie (title, original_title, None, duration, rating, None, None, release_date, 0)
"""   
