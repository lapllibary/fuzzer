import subprocess
import sys
import os
import shutil
from typing import List, Tuple
import argparse

from collections import deque

from tableGenerator import *
from mutateQuery import * 

ORACLE = "sqlite3"
ENGINE1 = "bld-sqlite3-3.26.0/sqlite3"
ENGINE2 = "bld-sqlite3-3.39.4/sqite3"

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

        return (results, error_output, process.returncode)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n_muts', help="Testcasses number you want to do", type=int, default=20000)
    parser.add_argument('-v', help="Engine to use 3.26.0 or 3.39.4", default="3.26.0")
    args = parser.parse_args()

    print(f"Fuzzing sqlite{args.v} with {args.n_muts} testcases")
    if args.v == "3.26.0":
        engine = ENGINE1
        dir = "bld-sqlite3-3.26.0"
    elif args.v == "3.39.4":
        engine = ENGINE2
        dir = "bld-sqlite3-3.39.4"
    else:
        print("not avail. vers")
        return
    bugs = 0
    cov = 0
    tables = {}
    queries = []
    to_save = ""
    for i in range(5):
        t, q = create_random_table(i)
        tables[f"t{i}"] = t[f"t{i}"]
        to_save += q + '\n'
        queries += [q]
        for _ in range(10):
            q = add_random_row(t)
            to_save += q + '\n'
            queries += [q]
    
    oracle_db = "db_oracle"
    db1 = f"db{bugs + 1}_test"

    _, error, ret_code = execute_sql_queries(queries, ORACLE, oracle_db)
    if error:
        print(f"Errors encountered:\n{error}", file=sys.stderr)

    _, error, ret_code = execute_sql_queries(queries, engine, db1)
    if error:
        print(f"Errors encountered:\n{error} in {engine}", file=sys.stderr)
    
    mutation_queue = deque()

    for i in range(5):
        mutation_queue.append(f"SELECT t{i}.c1 FROM t{i};")
    batch = []
    epoch = 0
    try:
        while mutation_queue and epoch < args.n_muts:
            q = mutation_queue.popleft()
            batch += [q]
            print(f"Running query {epoch}: {q}")
            
            results, error, ret_code = execute_sql_queries([q], ORACLE, oracle_db)

            if error:
                print(f"Errors encountered:\n{error}", file=sys.stderr)
            #print(results)
            res1, error, ret_code = execute_sql_queries([q], engine, db1)

            #print(res1)
            if error:
                print(f"Errors encountered:\n{error} in {engine}", file=sys.stderr)

            if ret_code < 0:
                print(f"Detected possible crash, return code {ret_code}")
        
            if not len(results) == len(res1):
                to_save += (q + '\n' + f"Correct res: {results}" + '\n' + f"Wrogn in {engine} res: {res1}" + '\n')
                shutil.copy(db1, f"db{bugs + 2}_test")
                bugs +=1
                db1 = f"db{bugs + 1}_test"
            else:
                for ind in range(len(results)):
                    if "e+" not in results[ind] and "e-" not in results[ind] and not results[ind] == res1[ind]:
                        to_save += (q + '\n' + f"Correct res: {results}" + '\n' + f"Wrogn in {engine} res: {res1}" + '\n')
                        break

            epoch += 1

            if epoch % 50 == 0 or epoch == 5 or epoch == 40:
                original_directory = os.getcwd()

                os.chdir(f"./{dir}")
                print("Calculating coverage ...")
                gcov_command = ["gcov", "sqlite3-sqlite3.c"]
                result = subprocess.run(gcov_command, capture_output=True, text=True)

                lines = result.stdout.splitlines()
                print(lines[-1])
                percentage = float(lines[-1].split()[1].strip("executed:").strip('%'))
                    
                os.chdir(original_directory)

                if cov < percentage:
                    print(f"New Best Coverage: {percentage}")
                    cov = percentage
                    random.shuffle(batch)
                    for b in batch:    
                        mutations = mutate_select(b, tables)
                        
                        for m in mutations:
                            mutation_queue.append(m)
                
                batch = []

            print("")

            queries = []
            if epoch % 100 == 0:
                for t in tables:
                    for _ in range(5):
                        q = add_random_prob_row({t: tables[t]})
                        to_save += q + '\n'
                        queries += [q]
                        q = del_row({t: tables[t]})
                        to_save += q + '\n'
                        queries += [q]
                        q = f"ANALYZE {t};"
                        to_save += q + '\n'
                        queries += [q]

                _, error, ret_code = execute_sql_queries(queries, ORACLE, oracle_db)
                if error:
                    print(f"Errors encountered:\n{error}", file=sys.stderr)

                _, error, ret_code = execute_sql_queries(queries, engine, db1)
                if error:
                    print(f"Errors encountered:\n{error} in {engine}", file=sys.stderr)        

        with open("queries.txt", "w") as f:
                f.write(to_save)
    except KeyboardInterrupt:
        print("Saving queries ...")
        with open("queries.txt", "w") as f:
            f.write(to_save) 

        print("Finished saving")

if __name__ == "__main__":
    main()
