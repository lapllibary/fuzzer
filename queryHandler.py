import subprocess
import sys
import os
import shutil
from typing import List, Tuple

from collections import deque

from tableGenerator import *
from mutateQuery import * 

ORACLE = "sqlite3"
ENGINE1 = "bld/sqlite3"
ENGINE2 = "sqlite3-3.39.4"

def execute_sql_queries(queries: List[str], engine: str, db):
    process = subprocess.Popen(
        [
            f"./{engine}",
            f"./{db}"
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    results = []
    error_output = ""

    try:

        for query in queries:
            process.stdin.write(f"{query}\n")
            process.stdin.flush()


        process.stdin.write(".quit\n")
        process.stdin.flush()
        output = []
        while True:
            line = process.stdout.readline().strip()
            if not line:
                break
            output.append(line)

            results = output

            err_output = process.stderr.read()
            if err_output:
                error_output = err_output.strip()
    except Exception as e:
        error_output = f"Execution error: {str(e)}"
    finally:
        if process.poll() is None:
            process.terminate()
        _, stderr = process.communicate()
        if stderr:
            error_output += "\n" + stderr if error_output else stderr

        return (results, error_output)

def main():
    bugs = 0

    tables = {}
    queries = []

    for i in range(5):
        t, q = create_random_table(i)
        tables[f"t{i}"] = t[f"t{i}"]
        queries += [q]
        for _ in range(50):
            q = add_random_row(t)
            to_save = q + '\n'
            queries += [q]

    oracle_db = "db_oracle123"
    db1 = f"db{bugs + 1}_3.26.0123"
    db2 = f"db{bugs + 1}_3.39.4123"

    _, error = execute_sql_queries(queries, ORACLE, oracle_db)
    if error:
        print(f"Errors encountered:\n{error}", file=sys.stderr)

    _, error = execute_sql_queries(queries, ENGINE1, db1)
    if error:
        print(f"Errors encountered:\n{error} in {ENGINE1}", file=sys.stderr)

    _, error = execute_sql_queries(queries, ENGINE2, db2)
    if error:
        print(f"Errors encountered:\n{error} in {ENGINE2}", file=sys.stderr)

    bugs = 0
    cov = 0.0

    mutation_queue = deque()

    for i in range(5):
        mutation_queue.append(f"SELECT t{i}.c1 FROM t{i};")

    epoch = 0
    try:
        while mutation_queue:
            q = mutation_queue.popleft()
            print(f"Running query {epoch}: {q}")
            
            results, error = execute_sql_queries([q], ORACLE, oracle_db)

            if error:
                print(f"Errors encountered:\n{error}", file=sys.stderr)
            
            res1, error = execute_sql_queries([q], ENGINE1, db1)

            if error:
                print(f"Errors encountered:\n{error} in {ENGINE1}", file=sys.stderr)

            if len(results) != len(res1):
                to_save += (q + '\n' + f"Correct res: {results}" + '\n' + f"Wrogn in {ENGINE1} res: {res1}" + '\n')
            else:
                for ind in range(results):
                    if "e+" not in results[ind] and "e-" not in results[ind] and not results[ind] == res1[ind]:
                        to_save += (q + '\n' + f"Correct res: {results}" + '\n' + f"Wrogn in {ENGINE1} res: {res1}" + '\n')
            
            res2, error = execute_sql_queries([q], ENGINE2, db2)

            if error:
                print(f"Errors encountered:\n{error} in {ENGINE2}", file=sys.stderr)
            
            if len(results) != len(res2):
                to_save += (q + '\n' + f"Correct res: {results}" + '\n' + f"Wrogn in {ENGINE2} res: {res2}" + '\n')
            else:
                for ind in range(results):
                    if "e+" not in results[ind] and "e-" not in results[ind] and not results[ind] == res2[ind]:
                        to_save += (q + '\n' + f"Correct res: {results}" + '\n' + f"Wrogn in {ENGINE2} res: {res1}" + '\n')
            mutations = mutate_select(q, tables)
            
            for m in mutations:
                mutation_queue.append(m)
            print("")
            epoch += 1

        with open("queries.txt", "w") as f:
                f.write(to_save) 
    except KeyboardInterrupt:
        original_directory = os.getcwd()

        os.chdir("./bld")
        gcov_command = ["gcov", "sqlite3-sqlite3.c"]
        result = subprocess.run(gcov_command, capture_output=True, text=True)

        lines = result.stdout.splitlines()
        print(lines[-1])
        percentage = float(lines[-1].split()[1].strip("executed:").strip('%'))
            
        os.chdir(original_directory)

        print(f"Coverage: {percentage}")

        with open("queries.txt", "w") as f:
            f.write(to_save) 

if __name__ == "__main__":
    main()
