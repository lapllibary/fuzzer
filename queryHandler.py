import subprocess
import sys
from typing import List, Tuple

from collections import deque

from tableGenerator import *
from mutateQuery import * 

DIR = "/mnt/c/Users/franc/Desktop/ASL/fuzzer/"
ENGINE = "sqlite3-3.26.0"

def execute_sql_queries(queries: List[str], db: str) -> Tuple[List[str], str]:
    """
    Execute SQL queries against SQLite3 database via WSL
    Returns (results, error_message)
    """
    process = subprocess.Popen(
        [
            "wsl", 
            DIR + ENGINE,
            DIR + "db"
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # Line-buffered for better interaction
    )

    results = []
    error_output = ""

    try:

        for query in queries:
            print(f"Running query: {query}")
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
    db_path = "/db"

    tables = {}
    queries = []

    for i in range(5):
        t, q = create_random_table(i)
        tables[f"t{i}"] = t[f"t{i}"]
        queries += [q]
        for _ in range(2):
            q = add_random_row(t)
            queries += [q]

    results, error = execute_sql_queries(queries, db_path)

    mutation_queue = deque()
    mutation_queue.append("SELECT t1.c1 FROM t1;")
    while mutation_queue:
        q = mutation_queue.popleft()
        mutations = mutate_select(q, tables)
        for m in mutations:
            mutation_queue.append(m)
        results, error = execute_sql_queries([q], db_path)
        # Print all results
        for i, result in enumerate(results, 1):
            print(f"Result {i}:\n{result}\n")

        if error:
            print(f"Errors encountered:\n{error}", file=sys.stderr)

    

if __name__ == "__main__":
    main()
