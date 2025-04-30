import subprocess
import sys
from typing import List, Tuple

DIR = "/mnt/c/Users/franc/Desktop/sqlite_files/"
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
    db_path = "/mnt/c/Users/franc/Desktop/sqlite_files/db2"
    queries = [
        "CREATE TABLE t1(c1, c2, c3, c4, PRIMARY KEY (c4, c3));",
        "INSERT INTO t1(c3) VALUES (0), (0), (0), (0), (0), (0), (0), (0), (0), (0), (NULL), (1), (0);",
        "UPDATE t1 SET c2 = 0;",
        "INSERT INTO t1(c1) VALUES (0), (0), (NULL), (0), (0);",
        "ANALYZE t1;",
        "UPDATE t1 SET c3 = 1;",
        "SELECT DISTINCT * FROM t1 WHERE t1.c3 = 1;"
    ]

    results, error = execute_sql_queries(queries, db_path)

    # Print all results
    for i, result in enumerate(results, 1):
        print(f"Result {i}:\n{result}\n")

    if error:
        print(f"Errors encountered:\n{error}", file=sys.stderr)

if __name__ == "__main__":
    main()
