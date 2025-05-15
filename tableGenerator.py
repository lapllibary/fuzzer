import random
import string
import sys
from sqlGenerator import *

OPERATORS = ["=", "<", ">", "<=", ">=", "IS NULL", "IS NOT NULL"]

def random_data_type():
    data_types = ['INTEGER', 'TEXT', 'REAL', 'BLOB', 'NUMERIC']
    return random.choice(data_types)

def add_random_row(t):
    t_name = next(iter(t))
    comm = f"INSERT INTO {t_name} ("
    random_values = []
    n_col = 1
    for col in t[t_name]:
        if t[t_name][col] == "INTEGER":
            random_values.append(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
        elif t[t_name][col] == "TEXT":
            random_values.append(''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
        elif t[t_name][col] == "REAL":
            random_values.append(random.uniform(-1e307, 1e307))
        elif t[t_name][col] == "BLOB":
            random_values.append((''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32)))).encode())
        else:
            i = random.choice([0, 1])
            if i == 0:
                random_values.append(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
            else:
                random_values.append(random.uniform(-1e307, 1e307))
        comm += f"c{n_col}" + ", "
        n_col += 1
    comm = comm.strip(", ") + ") VALUES ("
    for value in random_values:
        if isinstance(value, bytes):
            comm += f"X'{value.hex()}', "
        else:
            comm += f"'{value}', " if isinstance(value, str) else str(value) + ", "
    comm = comm.strip(", ") + ");"
    #print(comm)
    return comm

def add_random_prob_row(t):
    t_name = next(iter(t))
    comm = f"INSERT INTO {t_name} ("
    random_values = []
    
    for col in t[t_name]:
        if random.choice([True, False]) or col == f"c{len(t[t_name])}":
            if t[t_name][col] == "INTEGER":
                random_values.append(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
            elif t[t_name][col] == "TEXT":
                random_values.append(''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
            elif t[t_name][col] == "REAL":
                random_values.append(random.uniform(-1e307, 1e307))
            elif t[t_name][col] == "BLOB":
                random_values.append((''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32)))).encode())
            else:
                i = random.choice([0, 1])
                if i == 0:
                    random_values.append(random.randint(-(2**63 - 1) - 1, 2**63 - 1))
                else:
                    random_values.append(random.uniform(-1e307, 1e307))
            comm += f"{col}" + ", "
    comm = comm.strip(", ") + ") VALUES ("
    for value in random_values:
        if isinstance(value, bytes):
            comm += f"X'{value.hex()}', "
        else:
            comm += f"'{value}', " if isinstance(value, str) else str(value) + ", "
    comm = comm.strip(", ") + ");"
    #print(comm)
    return comm

def update_table(t):
    t_name = next(iter(t))
    comm = f"UPDATE {t_name} "
    random_col = random.choice(list(t[t_name].keys()))
    if t[t_name][random_col] == "INTEGER":
        rand_val = (random.randint(-(2**63 - 1) - 1, 2**63 - 1))
    elif t[t_name][random_col] == "TEXT":
        rand_val = (''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
    elif t[t_name][random_col] == "REAL":
        rand_val = (random.uniform(-1e307, 1e307))
    elif t[t_name][random_col] == "BLOB":
        rand_val = ((''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32)))).encode())
    else:
        i = random.choice([0, 1])
        if i == 0:
            rand_val = (random.randint(-(2**63 - 1) - 1, 2**63 - 1))
        else:
            rand_val = (random.uniform(-1e307, 1e307))

    comm += f"SET {random_col} = "
    if isinstance(rand_val, bytes):
        comm += f"X'{rand_val.hex()}' "
    else:
        comm += f"'{rand_val}', " if isinstance(rand_val, str) else str(rand_val) + " "

    random_col = random.choice(list(t[t_name].keys()))
    
    rand_op = random.choice(OPERATORS)
    if rand_op == "IS NULL" or rand_op == "IS NOT NULL":
        comm += f"WHERE {random_col} {rand_op}"
    else:
        if t[t_name][random_col] == "INTEGER":
            rand_val = (random.randint(-(2**63 - 1) - 1, 2**63 - 1))
        elif t[t_name][random_col] == "TEXT":
            rand_val = (''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
        elif t[t_name][random_col] == "REAL":
            rand_val = (random.uniform(-1e307, 1e307))
        elif t[t_name][random_col] == "BLOB":
            rand_val = ((''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32)))).encode())
        else:
            i = random.choice([0, 1])
            if i == 0:
                rand_val = (random.randint(-(2**63 - 1) - 1, 2**63 - 1))
            else:
                rand_val = (random.uniform(-1e307, 1e307))
        comm += f"WHERE {random_col} {rand_op} "
        if isinstance(rand_val, bytes):
            comm += f"X'{rand_val.hex()}' "
        else:
            comm += f"'{rand_val}'" if isinstance(rand_val, str) else str(rand_val) + " "
    comm += ";"
    return comm

def del_row(t):
    t_name = next(iter(t))
    comm = f"DELETE FROM {t_name} "
    random_col = random.choice(list(t[t_name].keys()))
    
    rand_op = random.choice(OPERATORS)
    if rand_op == "IS NULL" or rand_op == "IS NOT NULL":
        comm += f"WHERE {random_col} {rand_op}"
    else:
        if t[t_name][random_col] == "INTEGER":
            rand_val = (random.randint(-(2**63 - 1) - 1, 2**63 - 1))
        elif t[t_name][random_col] == "TEXT":
            rand_val = (''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32))))
        elif t[t_name][random_col] == "REAL":
            rand_val = (random.uniform(-1e307, 1e307))
        elif t[t_name][random_col] == "BLOB":
            rand_val = ((''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(0, 32)))).encode())
        else:
            i = random.choice([0, 1])
            if i == 0:
                rand_val = (random.randint(-(2**63 - 1) - 1, 2**63 - 1))
            else:
                rand_val = (random.uniform(-1e307, 1e307))
        comm += f"WHERE {random_col} {rand_op} "
        if isinstance(rand_val, bytes):
            comm += f"X'{rand_val.hex()}' "
        else:
            comm += f"'{rand_val}'" if isinstance(rand_val, str) else str(rand_val) + " "
    comm += ";"
    return comm

def add_random_constraints(col_type):
    primary_key = random.choice([True, False])
    constraints = ""
    #TODO
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
        table[f"t{ind}"][f"c{col_num + 1}"] = "INTEGER"
    else:
        comm += "PRIMARY KEY ("
        comm += ", ".join(primaryKey)
        comm += ")"
    comm += ");"
    
    #print(comm)
    return table, comm

