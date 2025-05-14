import subprocess
import sys
from typing import List, Tuple
from tableGenerator import *
DIR = "/home/lapllibrary/Fuzzer/"
ENGINE = "sqlite3-3.26.0"
allTables = []
num_cycles = 1000
num_cycles2 = 100
def equal_ignore_order(a, b):
    """ Use only when elements are neither hashable nor sortable! """
    unmatched = list(b)
    for element in a:
        try:
            unmatched.remove(element)
        except ValueError:
            return False
    return True

def execute_sql_queries(queries: List[str], db: str) -> Tuple[List[str], str]:
    """
    Execute SQL queries against SQLite3 database via WSL
    Returns (results, error_message)
    """
    process = subprocess.Popen(
        [
            "wsl", 
            DIR + ENGINE,
           
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,

        text=False,
        
    )

    results = []
    error_output = ""

    try:
        # Write all queries (encode as UTF-8)
        for query in queries:
            # print(f"Running query: {query}")
            process.stdin.write(f"{query}\n".encode('utf-8'))
            process.stdin.flush()


        process.stdin.write(".quit\n".encode('utf-8'))
        process.stdin.flush()

        while True:
            line = process.stdout.readline()
            if not line:
                break
            try:
                # Try to decode as UTF-8 text
                decoded_line = line.decode('utf-8').strip()
                results.append(decoded_line)
            except UnicodeDecodeError:
                # If decoding fails, treat as binary data
                results.append(f"[{line.hex()}]")

        # Read error output
        err_output = process.stderr.read()
        if err_output:
            try:
                error_output = err_output.decode('utf-8').strip()
            except UnicodeDecodeError:
                error_output = f"Binary error output: {err_output.hex()}"

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
    # c = add_random_row(table)
    #0 for equal
    bugquery = []
    bugquery2 = []
    queries = [
    ]
    tables ={}

    queries2 = []
    counter  = 0
    num_result = []
    bquery  =  []
    
    for i in range(num_cycles):
        n1= len(queries)
        if(i%6 == 0):
            if(i):
                bugquery.append(bquery)
            bquery = []
            for t in tables.keys():
                queries.append(deleteTable(t)+";")
                queries2.append(deleteTable(t)+";")
                
            tables = {}
            t,q= create_random_table(i)
            tables[f"t{i}"] = t[f"t{i}"]
            queries += [q]
            queries2 +=[q]
            bquery += [q]
            f_update = update(tables)
            queries.append(f_update)
            bugquery.append(f_update)
            f_delete = delete(tables)
            queries.append(f_delete)
            bugquery.append(f_delete)
            for _ in range(2):
                q= add_random_row(t)
                queries += [q]
                queries2 += [q] 
                bquery +=[q]
        f = random.choice([whereTLP,aggregateTLP])
        query  = f(tables)
        num_result.append(2)
        queries.append(query[0]+";")
        queries2.append(query[1]+";")
        bquery.append(query[0]+";")
        bquery.append(query[1]+";")
        n2 = len(queries)
    if(num_cycles%6):
        bugquery.append(bquery)
    
    for t in tables.keys():
                queries.append(deleteTable(t)+";")
    queries.extend(queries2)
    for t in tables.keys():
                queries.append(deleteTable(t)+";")
    tables = {}
    queries2 = []
    for i in range(num_cycles2):
        if(i%6 == 0):                
            for t in tables.keys():
                queries.append(deleteTable(t)+";")
                queries2.append(deleteTable(t)+";")
                bugquery2.append(deleteTable(t)+";")
            tables = {}
            t,q= create_random_table(i)
            tables[f"t{i}"] = t[f"t{i}"]
            queries += [q]
            queries2 +=[q]
            bugquery2.append(q)
            f_update = update(tables)
            queries.append(f_update)
            bugquery2.append(f_update)
            f_delete = delete(tables)
            queries.append(f_delete)
            bugquery2.append(f_delete)
            for _ in range(2):
                q= add_random_row(t)
                queries += [q]
                queries2 += [q] 
                bugquery2 +=[q]
        f = random.choice([whereExtendedTLP,selectDistinctTLP])
        query  = f(tables)
        queries.append(query[0]+";")
        queries2.append(query[1]+";")
        bugquery2.append(query[0]+";")
        bugquery2.append(query[1]+";")


    for t in tables.keys():
                queries.append(deleteTable(t)+";")
    queries.extend(queries2)
    results, error = execute_sql_queries(queries, db_path)

    # Print all results
    # for i, result in enumerate(results, 1):
    #     print(f"Result {i}:\n{result}\n")
    bug = 0
    prev = 0

    l = 0
    for n in num_result:
        l += n
    list3 = results[2*l:]
    for i, n in enumerate(num_result,1):
        list1 = results[prev:prev+n]
        list2 = results[prev+l:prev+n+l]
        
        prev = prev + n
        if(equal_ignore_order(list1,list2)==False):
            print(f"BUG: {bug}\n")
            bug = i
    if(bug):
        with open("queries.txt", "w") as f:
            # n1 = allTables[bug][0]
            # n2 = allTables[bug][1]
            n1 = 0
            n2 = len(queries)
            n = bug // 6
            x = bugquery[n]
            for q in x:
                f.write(q)
                f.write("\n")
            f.write("\n")
            for n in num_result:
                f.write(str(n))
                f.write("\n")
    else:
        list4 = list3[:len(list3)//2]
        list5 = list3[len(list3)//2:]
        if(equal_ignore_order(list4,list5)==False):
            print(f"BUG2: {bug}\n")
            with open("queries.txt", "w") as f:
                for q in bugquery2:
                    f.write(q)
                    f.write("\n")
                f.write("\n")
           

    if error:
        print(f"Errors encountered:\n{error}", file=sys.stderr)

if __name__ == "__main__":
    main()
