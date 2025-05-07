import random
import string
import sys
from sqlGenerator import *
BINARY_COMPARISON_EXPRESSIONS = ["=", "==", "<", "<=", ">", ">=", "!=", "IN", "NOT IN", "BETWEEN", "IS", "IS NOT"]

def random_data_type():
    data_types = ['INTEGER', 'TEXT', 'REAL', 'BLOB', 'NUMERIC']
    return random.choice(data_types)

def add_random_row(t):
    t_name = next(iter(t))
    comm = f"INSERT INTO {t_name} ("
    random_values = []
    col = 1
    for type in t[t_name]:
        if type == "INTEGER":
            random_values.append(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
        elif type == "TEXT":
            random_values.append(''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
        elif type == "REAL":
            random_values.append(random.uniform(-1e307, 1e307))
        elif type == "BLOB":
            random_values.append(bytearray(random.getrandbits(8) for _ in range(256)))
        else:
            i = random.choice([0, 1])
            if i == 0:
                random_values.append(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
            else:
                random_values.append(random.uniform(-1e307, 1e307))
        comm += f"c{col}" + ", "
        col += 1
    comm = comm.strip(", ") + ") VALUES ("
    for value in random_values:
        if isinstance(value, bytearray):
            comm += f"X'{value.hex()}', "
        else:
            comm += f"'{value}', " if isinstance(value, str) else str(value) + ", "
    comm = comm.strip(", ") + ");"
    #print(comm)
    return comm

def add_random_constraints(col_type):
    primary_key = random.choice([True, False])
    constraints = ""
    
    if random.choice([True, False]):
        constraints += "UNIQUE "

    if random.choice([True, False]):
        constraints += "NOT NULL "
    
    #CHECK
    #DEFAULT

    #if primary_key and col_type == "INTEGER" and random.choice([True, False]):
    #    constraints += "AUTOINCREMENT "

    return constraints.strip(), primary_key

def create_random_table(ind):
    table = {
        f"t{ind}": {}
    }
    comm = f"CREATE TABLE t{ind} ("
    primaryKey = []
    col_num = random.randint(1, 10)
    for i in range(1, col_num + 1):
        col_type  = random_data_type()
        constraints, hasPrimary = add_random_constraints(col_type)
        if hasPrimary:
            primaryKey.append(f"c{i}")
        table[f"t{ind}"][f"c{i}"] = f"{col_type}"
        comm += f"c{i} {col_type} {constraints}, "
    if not primaryKey:
        comm += f"c{col_num + 1} INTEGER PRIMARY KEY"
        table.add_column("INTEGER")
    else:
        comm += "PRIMARY KEY ("
        comm += ", ".join(primaryKey)
        comm += ")"
    comm += ");"
    
    print(table)
    for t in table:
        print(t)
    return table, comm

table, _ = create_random_table(1)
c = add_random_row(table)
print(c)