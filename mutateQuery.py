import sqlglot
import random

from tableGenerator import *
from sqlglot import expressions as exp

BORDER_CASE_VALUES = [0, -0, "1e999", "-1e999", -(2**63 - 1) - 1, 2**63 - 1, "\'\'"]
OPERATORS = ["=", "<", ">", "<=", ">="]
BIN_BOOL_EXP = ["and", "or"]
UN_BOOL_EXP = ["NOT", ""]
JOIN_TYPES = ["INNER", "LEFT", "CROSS", "NATURAL"]
AGG_FUNC = ["Sum", "Avg", "Min", "Max", "GroupConcat", "Count"]

def add_column(parsed_query, t_name, tables):
    select = parsed_query.find(exp.Select)
    select_items = select.expressions
    available_cols = list(tables[t_name].keys())

    if len(select_items) == 1 and isinstance(select_items[0], exp.Star):
        select.set(
            'expressions',
            [exp.Column(this=exp.Identifier(this=col)) for col in available_cols]
        )
        extra_col = random.choice(available_cols)
        new_column = exp.Column(this=exp.Identifier(this=extra_col))
        select.args['expressions'].append(new_column)
        return [parsed_query]
    
    selected_col_names = set()
    for item in select_items:
        if isinstance(item, exp.Column):
            col_name = item.alias_or_name
            selected_col_names.add(col_name)

    if len(selected_col_names) == len(available_cols) - 1:
        select.set('expressions', [exp.Star()])
        return [parsed_query]

    missing_cols = [col for col in available_cols if col not in selected_col_names]

    if missing_cols:
        to_add = f"{t_name}." + random.choice(missing_cols)
        new_column = exp.Column(this=exp.Identifier(this=to_add))
        select.args['expressions'].append(new_column)

    return [parsed_query]

def remove_column(parsed_query, t_name, tables):
    select = parsed_query.find(exp.Select)
    select_items = [item for item in select.expressions if isinstance(item, exp.Column)]
    available_cols = list(tables[t_name].keys())

    if len(select_items) == 1 and isinstance(select_items[0], exp.Star):
        select.set(
            'expressions',
            [exp.Column(this=exp.Identifier(this=col)) for col in available_cols]
        )
        to_remove = random.choice(select.expressions)
        select.args['expressions'].remove(to_remove)
        return [parsed_query]
    
    if len(select_items) == 1:
        return [parsed_query] 
    
    if select_items:
        to_remove = random.choice(select_items)
        select.args['expressions'].remove(to_remove)
    
    return [parsed_query]

def modify_columns(parsed_query, t_name, tables):
    select = parsed_query.find(exp.Select)
    select_items = [item for item in select.expressions if isinstance(item, exp.Column)]
    available_cols = list(tables[t_name].keys())

    if len(select_items) == 1 and isinstance(select_items[0], exp.Star):
        return [parsed_query]

    if select_items and len(available_cols) > 1:
        to_modify = random.choice(select_items)
        available_cols.remove(to_modify.sql().split('.')[1])
        col = random.choice(available_cols)
        to_modify.set('this', exp.Identifier(this=col))
    
    return [parsed_query]

def create_where_cond(parsed_query, new_cond, b_op):
    select = parsed_query.find(exp.Select)
    if select.args.get('where'):
        exist_conds = select.args['where']
        if b_op == "and":
            select.args['where'] = exist_conds.and_(new_cond)
        elif b_op == "or":
            select.args['where'] = exist_conds.or_(new_cond)
    else:
        select = select.where(new_cond)
    return select

def add_where_cond(parsed_query, t_name, tables):
    available_cols = list(tables[t_name].keys())
    
    mutations = []

    for b_op in BIN_BOOL_EXP:
        for u_op in UN_BOOL_EXP:
            for op in OPERATORS:
                col = f"{t_name}." + random.choice(available_cols)
                copy = parsed_query.copy()
                val = random.choice(BORDER_CASE_VALUES)
                new_cond = f"{u_op} {col} {op} {val}"
                select = create_where_cond(copy, new_cond, b_op)
                mutations += [select]

                if len(available_cols) >= 2:
                    col1 = f"{t_name}." + random.choice(available_cols)
                    col2 = f"{t_name}." + random.choice(available_cols)
                    copy = parsed_query.copy()
                    new_cond = f"{u_op} {col1} {op} {col2}"
                    select = create_where_cond(copy, new_cond, b_op)
                    mutations += [select]

            col = f"{t_name}." + random.choice(available_cols)
            copy = parsed_query.copy()
            new_cond = f"{col} IS {u_op} NULL"
            select = create_where_cond(copy, new_cond, b_op)
            mutations += [select]

            col = f"{t_name}." + random.choice(available_cols)
            val1 = random.choice(BORDER_CASE_VALUES)
            val2 = random.choice(BORDER_CASE_VALUES)
            copy = parsed_query.copy()
            new_cond = f"{col} {u_op} BETWEEN {val1} AND {val2}"
            select = create_where_cond(copy, new_cond, b_op)
            mutations += [select]

    return mutations

def rem_where_cond(parsed_query, t_name, tables):
    select = parsed_query.find(exp.Select)
    if not select.args.get('where'):
        return [parsed_query]
    
    where_condition = select.args['where'].this
    
    conditions = []
    
    def collect_conditions(condition):
        if isinstance(condition, (exp.And, exp.Or)):
            collect_conditions(condition.left)
            collect_conditions(condition.right)
        else:
            conditions.append(condition)

    collect_conditions(where_condition)

    if len(conditions) == 1:
        select.args.pop('where')
        return [parsed_query]
    
    to_remove = random.choice(conditions)
    
    def rem_cond(condition, father = None, is_left = None):
        if is_left == None and condition.left == to_remove:
            select.args['where'].set('this', condition.right)
        elif is_left == None and condition.right == to_remove:
            select.args['where'].set('this', condition.left)
        elif is_left and condition.left == to_remove:
            condition.set('this', condition.right)
        elif is_left and condition.right == to_remove:
            condition.set('this', condition.left)
        elif not is_left and condition.left == to_remove:
            condition.set('expression', condition.right)
        elif not is_left and condition.left == to_remove:
            condition.set('expression', condition.left)
        else:
            if isinstance(condition, (exp.And, exp.Or)):
                rem_cond(condition.left, condition, True)
                rem_cond(condition.right, condition, False)

    rem_cond(where_condition)

    return [parsed_query]

def add_inj(parsed_query, t_name, tables):
    val = random.choice(BORDER_CASE_VALUES)
    inj = f"{val} = {val}"
    select = create_where_cond(parsed_query, inj, "or")

    return [select]

def add_distinc(parsed_query, t_name, tables):
    select = parsed_query.find(exp.Select)
    select.set('distinct', exp.Distinct())

    return [select]

def add_join(parsed_query, t_name, tables):
    select = parsed_query.find(exp.Select)

    all_tables = list(tables.keys())
    all_tables.remove(t_name)

    matching_tables = {}
    types = set(tables[t_name].values())

    for t in all_tables:
        common_types = types.intersection(set(tables[t].values()))
        if common_types:
            matching_tables[t] = common_types

    if not matching_tables:

        return [parsed_query]
    

    rand_t = random.choice(list(matching_tables.keys()))
    rand_type = random.choice(list(matching_tables[rand_t]))

    i = len(select.args.get('joins', []))
    alias = f"j{i}"

    join_type = random.choice(JOIN_TYPES)
    cond = None

    if join_type == "INNER" or join_type == "LEFT":
        col_origin_t = random.choice([c for c, typ in tables[t_name].items() if typ == rand_type])
        col_join_t = random.choice([c for c, typ in tables[rand_t].items() if typ == rand_type])

        cond = f"{t_name}.{col_origin_t} = {alias}.{col_join_t}"

        join_expr = exp.Join(
            this=exp.Table(
                this=exp.Identifier(this=rand_t, quoted=False),
                alias=exp.Identifier(this=alias, quoted=False)),
            on=exp.condition(cond),
            kind=join_type
        )
    else:
        join_expr = exp.Join(
            this=exp.Table(
                this=exp.Identifier(this=rand_t, quoted=False),
                alias=exp.Identifier(this=alias, quoted=False)),
            kind=join_type
        )
    
    select.set('joins', select.args.get('joins', []) + [join_expr])
    return [parsed_query]

def add_agg_func(parsed_query, t_name, tables):
    available_cols = list(tables[t_name].keys())

    col = random.choice(available_cols)
    group_by_cols = [item for item in parsed_query.find(exp.Select).expressions if isinstance(item, exp.Column)]
    group_exprs = [exp.Column(this=col) for col in group_by_cols]

    mutations = []

    for f in AGG_FUNC:
        copy = parsed_query.copy()
        select = copy.find(exp.Select)

        agg_class = getattr(exp, f)

        if (tables[t_name][col] == "NUMERIC" or tables[t_name][col] == "INTEGER" or tables[t_name][col] == "REAL") and not f == "Count":
            agg_expr = agg_class(this=exp.Column(this=f"{t_name}." + col))
            select.args['expressions'].append(agg_expr)
        if f == "Count":
            col = random.choice(["*"] + [col])
            agg_this = exp.Star() if col == '*' else exp.Column(this=f"{t_name}." + col)
            agg_expr = agg_class(this=agg_this)
            select.args['expressions'].append(agg_expr)

        select.set('group', exp.Group(expressions=group_exprs))
        
        mutations += [select]


    return mutations

def create_having_cond(parsed_query, new_cond, b_op):
    select = parsed_query.find(exp.Select)
    if select.args.get('having'):
        exist_conds = select.args['having']
        if b_op == "and":
            select.args['having'] = exist_conds.and_(new_cond)
        elif b_op == "or":
            select.args['having'] = exist_conds.or_(new_cond)
    else:
        select = select.having(new_cond)
    return select

def add_having_cond(parsed_query, t_name, tables):
    available_cols = list(tables[t_name].keys())
    
    select_items = [item for item in parsed_query.find(exp.Select).expressions if not isinstance(item, exp.Column)]
    mutations = []
    if parsed_query.find(exp.Select).args.get('group'):
        f = random.choice(AGG_FUNC)
        agg_class = getattr(exp, f)
        col = random.choice(available_cols)
        if (tables[t_name][col] == "NUMERIC" or tables[t_name][col] == "INTEGER" or tables[t_name][col] == "REAL") and not f == "Count":
            select_items += [agg_class(this=exp.Column(this=f"{t_name}." + col))]
        if f == "Count":
            col = random.choice(["*"] + [col])
            agg_this = exp.Star() if col == '*' else exp.Column(this=f"{t_name}." + col)
            select_items += [agg_class(this=agg_this)]
    if select_items:
        for b_op in BIN_BOOL_EXP:
            for u_op in UN_BOOL_EXP:
                for op in OPERATORS:
                    agg = random.choice(select_items)
                    copy = parsed_query.copy()
                    val = random.choice(BORDER_CASE_VALUES)
                    new_cond = f"{u_op} {agg} {op} {val}"
                    select = create_having_cond(copy, new_cond, b_op)
                    mutations += [select]

                    col = f"{t_name}." + random.choice(available_cols)
                    copy = parsed_query.copy()
                    new_cond = f"{u_op} {agg} {op} {col}"
                    select = create_having_cond(copy, new_cond, b_op)
                    mutations += [select]

                    if len(select_items) >= 2:
                        agg2 = random.choice(select_items).sql()
                        copy = parsed_query.copy()
                        new_cond = f"{u_op} {agg} {op} {agg2}"
                        select = create_having_cond(copy, new_cond, b_op)
                        mutations += [select]

                agg = random.choice(select_items).sql()
                copy = parsed_query.copy()
                new_cond = f"{agg} IS {u_op} NULL"
                select = create_having_cond(copy, new_cond, b_op)
                mutations += [select]

                agg = random.choice(select_items).sql()
                val1 = random.choice(BORDER_CASE_VALUES)
                val2 = random.choice(BORDER_CASE_VALUES)
                copy = parsed_query.copy()
                new_cond = f"{agg} {u_op} BETWEEN {val1} AND {val2}"
                select = create_having_cond(copy, new_cond, b_op)
                mutations += [select]

    


    return mutations


MUTATIONS = [modify_columns, add_column, add_where_cond, add_inj, add_distinc, add_join, add_agg_func, add_having_cond]

def mutate_select(query, tables):
    parsed = sqlglot.parse_one(query)
    t = parsed.find(exp.From).this.sql()
    mutated_queries = []
    for mutation in MUTATIONS:
        parsed_copy = parsed.copy()
        mutations_queries = (mutation(parsed_copy, t, tables))
        for m in mutations_queries:
            if not m.sql() == parsed.sql():
                #print(m.sql())
                mutated_queries.append(m.sql() + ";")

    return mutated_queries

