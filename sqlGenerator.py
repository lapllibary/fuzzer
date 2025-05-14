import random
import string
import binascii
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
JOIN = [" INNER JOIN " ,"RIGHT JOIN ","CROSS JOIN ","SELF JOIN ", "UNION"]

def joinClause(columns,table2,condition):
   clause = random.choice(JOIN)
   return "INNER JOIN " + table2 +" "+ clause + " " +condition

def orderByClause(columns):
  clause = "ORDER BY"
  for c in columns:
    clause += c
    r = random.randint(0,2)
    if(r ==2):
      clause +='DESC'
    elif(r == 1):
      clause +='ASC'
    return clause

def getRandomValue(type:str):
  if type == "INTEGER":
    value=(random.choice([-(2**63 - 1) - 1, 2**63 - 1,0]))
  elif type == "TEXT":
      value=(''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
  elif type == "REAL":
      value=(random.uniform(-1e307, 1e307))
  elif type == "BLOB":
    random_bytes = bytes(random.getrandbits(8) for _ in range(256))
    
    # Convert to hex without \x prefixes
    hex_str = binascii.hexlify(random_bytes).decode('ascii')
    
    # Format for SQL (X'hexhexhex')
    return f"X'{hex_str}'"
  else:
      i = random.choice([0, 1])
      if i == 0:
          value=(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
      else:
          value=(random.uniform(-1e307, 1e307))
  if(random.randint(0,5)==5):
     value = "NULL"
  return str(value)

def whereClause(columns,table,t):
  clause = "WHERE "
  column = random.choice(columns)
  clause += generatePredicate(columns,table,t)
  
  return clause


def generatePredicate(columns,table,t):
  operator = random.choice(operators)
  column = random.choice(columns)
  c2 = []
  for c in columns:
     if(table[t][column] == table[t][c]):
        c2.append(c)
  if(random.randint(0,4)):
    value = getRandomValue(table[t][column])
  else:
    value = random.choice(c2)
  if(table[t][column]== "TEXT"):
      value = "\' "+value +" \'"
  return  column +" " + operator + " " + value 


# def createTable():
#   global Queries
#   global Tables
#   return "CREATE TABLE t1(c1, c2, c3, c4, PRIMARY KEY (c4, c3));"
# QueryFunctions.append(createTable)

def selectDisticnt(Tables:dict):
  query = "SELECT DISTINCT "
  table = random.choice(list(Tables.keys()))
  all_columns = list(Tables[table].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns,  num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query += (columns + " FROM " + table)
  return query 
QueryFunctions.append(selectDisticnt)

def insertIntoColumns(Tables:dict):
  query = ""
  #table to insert into 
  table = random.choice(list(Tables.keys()))
  all_columns = list(Tables[table].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns, num_values)
  columns = column[0]
  values = "'" + getRandomValue(Tables[table][column[0]]) +" ' "
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
     values = values + " ,"  + getRandomValue(Tables[table][column[i]])
  query = (query + "INSERT INTO " + table + "("  + columns+ ") " + "VALUES (" + values +")" )

  #                "(0), (0), (0), (0), (0), (0), (0), (0), (0), (0), (NULL), (1), (0);")
  return query
QueryFunctions.append(insertIntoColumns)

def select(Tables:dict):
  t = random.choice(list(Tables.keys()))
  all_columns = list(Tables[t].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns, num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query = ("SELECT " + columns+" FROM " + t +" " + whereClause(column,Tables,t) )
  return query
QueryFunctions.append(select)

def update(Tables:dict):
  query = "UPDATE "
  table = random.choice(list(Tables.keys()))
  all_columns = list(Tables[table].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns,  num_values)
  columns = column[0]
  query +=(table+" SET ")
  for i in range(num_values-1):
    query+=(column[i] + "= " +getRandomValue(Tables[table][column[i]]) +"," )
  query+=(column[num_values-1] + " = " + getRandomValue(Tables[table][column[num_values-1]] ))
  query+= whereClause(column,Tables,table)
  return query
QueryFunctions.append(update)

def delete(Tables:dict):
  query = "DELETE FROM"
  table = random.choice(list(Tables.keys()))
  query +=  table
  query += (whereClause(list(table.keys()),Tables,table) )

def selectFunction(Tables:dict):
  table = random.choice(list(Tables.keys()))
  all_columns = list(Tables[table].keys())
  column = random.choice(all_columns)
  query = ("SELECT " + random.choice(functions)  +"("+ column +")" +" FROM " + table)
  return query
QueryFunctions.append(select)

  
def whereTLPClause(columns,table,t):
    
    predicate = "NULL"
    if(random.randint(1,10)==1):
      predicate = random.choice(["TRUE","FALSE"])
    else:
       predicate = generatePredicate(columns,table,t)
    clauses =[" WHERE "+ predicate," WHERE NOT " + predicate," WHERE " +predicate + " IS NULL"]
    return clauses
def whereExTLPClause(columns,table,t):
    
    predicate = "NULL"
    if(random.randint(1,10)==1):
      predicate = random.choice(["TRUE","FALSE"])
    else:
       predicate = generatePredicate(columns,table,t)
    clauses =[" AND "+ predicate," AND NOT" + predicate," AND " +predicate + " IS NULL"]
    return clauses

def selectDistinctTLP(Tables):
  query = "SELECT DISTINCT "
  table = random.choice(list(Tables.keys()))
  all_columns = list(Tables[table].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns,  num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query += (columns + " FROM " + table)
  clauses = whereTLP(all_columns,Tables,table)
  query0 = query
  query1 = query + clauses[0]
  query2 = query + clauses[1]
  query3 = query + clauses[2]
  return [query0,query1 +" UNION " + query2 + " UNION " + query3]

def whereTLP(Tables:dict):
  t = random.choice(list(Tables.keys()))
  all_columns = list(Tables[t].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns, num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query = ("SELECT " + columns+" FROM " + t +" "  )
  clauses = whereTLPClause(all_columns,Tables,t)
  query0 = query
  query1 = query + clauses[0]
  query2 = query + clauses[1]
  query3 = query + clauses[2]
  return [query0,query1 +" UNION ALL " + query2 + " UNION ALL " + query3]

def whereExtendedTLP(Tables: dict):
  t = random.choice(list(Tables.keys()))
  all_columns = list(Tables[t].keys())
  num_values = random.randint(1,len(all_columns))
  column = random.sample(all_columns, num_values)
  columns = column[0]
  for i in range(1,num_values-1):
     columns = columns +", " + column[i] 
  query = ("SELECT " + columns+" FROM " + t +" " + whereClause(column,Tables,t)  )
  clauses = whereExTLPClause(all_columns,Tables,t)
  query0 = query
  query1 = query + clauses[0]
  query2 = query + clauses[1]
  query3 = query + clauses[2]
  return [query0,query1 +" UNION ALL " + query2 + " UNION ALL " + query3]

def aggregateTLP(Tables:dict):
  table = random.choice(list(Tables.keys()))
  all_columns = list(Tables[table].keys())
  column = random.choice(all_columns)
  query = ("SELECT " + random.choice(functions)  +"("+ column +")" +" FROM " + table)
  query = ("SELECT " + column+" FROM " + table  )
  clauses = whereTLPClause(all_columns,Tables,table)
  query0 = query
  query1 = query + clauses[0]
  query2 = query + clauses[1]
  query3 = query + clauses[2]
  return [query0,query1 +" UNION ALL " + query2 + " UNION ALL " + query3]


with open("queries.txt", "w") as f:
  for q in Queries:
    f.write(q)
    f.write('\n')



