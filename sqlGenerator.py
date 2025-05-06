import random
import string
from enum import Enum
from datetime import datetime, timedelta
import uuid
import decimal
from utilities import *
Queries = []
# {Table_name:{columns_name: [value]}}
#Tables = {}
#TestTable
c1 = {"c1":'NULL'}
# List of functions that add a query
QueryFunctions = []


def orderByClause(columns):
  clause = "ORDER BY"
  for c in columns:
    clause += c
    r = random.randint(0,3)
    if(r ==2):
      clause +='DESC'
    elif(r == 1):
      clause +='ASC'
    return clause
  #TODO
def whereClause():
  clause = "WHERE"
  clause += " "
  return clause
def createTable():
  global Queries
  global Tables
  return "CREATE TABLE t1(c1, c2, c3, c4, PRIMARY KEY (c4, c3));"
QueryFunctions.append(createTable)

def selectDisticnt(Tables:dict):
  query = "SELECT DISTINCT "
  table = random.choice(Tables.keys)
  all_columns = Tables[table]
  num_values = random.randint(1,len(all_columns)+1)
  column = random.choices(all_columns.keys, k = num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query += (columns + " FROM" + table)
  return query 
QueryFunctions.append(selectDisticnt)

def insertIntoColumns(Tables:dict):
  query = ""
  #table to insert into 
  table = random.choice(Tables.keys)
  all_columns = Tables[table]
  num_values = random.randint(1,len(all_columns)+1)
  column = random.choices(all_columns.keys, k = num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query = (query + "INSERT INTO " + table + "("  + columns+ ")" + "VALUES" )
  values = ""
  #TODO: Change into random data
  for i in range(num_values):
    values += "(0)"
    if(i!=num_values):
      values += ", "
  query += values
  #                "(0), (0), (0), (0), (0), (0), (0), (0), (0), (0), (NULL), (1), (0);")
  return query
QueryFunctions.append(insertIntoColumns)

def select(Tables:dict):
  table = random.choice(Tables.keys)
  all_columns = Tables[table]
  num_values = random.randint(1,len(all_columns)+1)
  column = random.choices(all_columns.keys, k = num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query = ("SELECT " + columns+" " )
  query = ""
  return query
QueryFunctions.append(select)

def update(Tables:dict):
  query = "UPDATE"
  table = random.choice(Tables.keys)
  all_columns = Tables[table]
  num_values = random.randint(1,len(all_columns)+1)
  column = random.choices(all_columns.keys, k = num_values)
  columns = column[0]
  query +=(table+"SET")
  for i in range(num_values-1):
    query+=(column[i] + "=" +"NULL" +"," )#TODO change NULL to random value
  query+=(column[num_values-1] + "=" +"NULL" )
  query+= whereClause()
  return query
QueryFunctions.append(update)

def delete(Tables:dict):
  query = "DELETE FROM"
  table = random.choice(Tables.keys)
  query +=  table
  query += (whereClause() +";")

def selectFunction(Tables:dict):
  table = random.choice(Tables.keys)
  all_columns = Tables[table]
  column = random.choice(all_columns.keys)
  query = ("SELECT " + random.choice(functions) + +"("+ column +")" +"FROM" + table)
  query = ""
  return query
QueryFunctions.append(select)
with open("queries.txt", "w") as f:
  for q in Queries:
    f.write(q)
    f.write('\n')



