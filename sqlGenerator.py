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

def getRandomValue(type:str):
  if type == "INTEGER":
    value=(random.randchoice([-(2**63 - 1) - 1, 2**63 - 1,0]))
  elif type == "TEXT":
      value=(''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
  elif type == "REAL":
      value=(random.uniform(-1e307, 1e307))
  elif type == "BLOB":
      value=(bytearray(random.getrandbits(8) for _ in range(256)))
  else:
      i = random.choice([0, 1])
      if i == 0:
          value=(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
      else:
          value=(random.uniform(-1e307, 1e307))
  if(random.randint(0,6)==5):
     value = "NULL"
  return str(value)

def whereClause(columns,table,t):
  clause = "WHERE"
  column = random.choice(columns)
  clause += generatePredicate(column)
  return clause

def generatePredicate(columns,table,t):
  operator = random.choice(operators)
  column = random.choice(columns)
  c2 = []
  for c in columns:
     if(table[column][t] == table[c][t]):
        c2.append(c)
  if(random.randint(0,10)):
    value = getRandomValue(table[column][t])
  else:
     value = random.choice(c2)
  return  column +" " + operator + " " + value 
  
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
  values = "'" + getRandomValue(Tables[table][column[0]]) +" ' "
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
     values = values + " ," + "'" + getRandomValue(Tables[table][column[i]]) +"'"
  query = (query + "INSERT INTO " + table + "("  + columns+ ")" + "VALUES(" + values +");" )

  #                "(0), (0), (0), (0), (0), (0), (0), (0), (0), (0), (NULL), (1), (0);")
  return query
QueryFunctions.append(insertIntoColumns)

def select(Tables:dict):
  t = random.choice(Tables.keys)
  all_columns = Tables[t]
  num_values = random.randint(1,len(all_columns)+1)
  column = random.choices(all_columns.keys, k = num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query = ("SELECT " + columns+" FROM" + t + whereClause(columns,Tables,t))

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
    query+=(column[i] + "= " +getRandomValue(Tables[table][column[i]]) +"," )
  query+=(column[num_values-1] + "= " + getRandomValue(Tables[table][column[num_values-1]] ))
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



