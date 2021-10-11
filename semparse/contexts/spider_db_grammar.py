# pylint: disable=anomalous-backslash-in-string
"""
A ``Text2SqlTableContext`` represents the SQL context in which an utterance appears
for the any of the text2sql datasets, with the grammar and the valid actions.
"""
from typing import List, Dict

from dataset_readers.dataset_util.spider_utils import Table

GRAMMAR_DICTIONARY = {"statement": ['(query ws iue ws query)', '(query ws)'],
                      "iue": ['"intersect"', '"except"', '"union"'],
                      "query": ['(ws select_core ws groupby_clause ws orderby_clause ws limit)',
                                '(ws select_core ws groupby_clause ws orderby_clause)',
                                '(ws select_core ws groupby_clause ws limit)',
                                '(ws select_core ws orderby_clause ws limit)',
                                '(ws select_core ws groupby_clause)',
                                '(ws select_core ws orderby_clause)',
                                '(ws select_core)'],
                      "select_core": ['(select_with_distinct ws select_results ws from_clause ws where_clause)',
                                      '(select_with_distinct ws select_results ws from_clause)',
                                      '(select_with_distinct ws select_results ws where_clause)',
                                      '(select_with_distinct ws select_results)'],
                      "select_with_distinct": ['(ws "select" ws "distinct")', '(ws "select")'],
                      "select_results": ['(ws select_result ws "," ws select_results)', '(ws select_result)'],
                      "select_result": ['"*"', '(table_source ws ".*")',
                                        'expr', 'col_ref'],
                      "from_clause": ['(ws "from" ws table_source ws join_clauses)',
                                      '(ws "from" ws source)'],
                      "join_clauses": ['(join_clause ws join_clauses)', 'join_clause'],
                      "join_clause": ['"join" ws table_source ws "on" ws join_condition_clause'],
                      "join_condition_clause": ['(join_condition ws "and" ws join_condition_clause)', 'join_condition'],
                      "join_condition": ['ws col_ref ws "=" ws col_ref'],
                      "source": ['(ws single_source ws "," ws source)', '(ws single_source)'],
                      "single_source": ['table_source', 'source_subq'], "source_subq": ['("(" ws query ws ")")'],
                      "limit": ['("limit" ws non_literal_number)'],
                      "where_clause": ['(ws "where" wsp expr ws where_conj)', '(ws "where" wsp expr)'],
                      "where_conj": ['(ws "and" wsp expr ws where_conj)', '(ws "and" wsp expr)'],
                      "groupby_clause": ['(ws "group" ws "by" ws group_clause ws "having" ws expr)',
                                         '(ws "group" ws "by" ws group_clause)'],
                      "group_clause": ['(ws expr ws "," ws group_clause)', '(ws expr)'],
                      "orderby_clause": ['ws "order" ws "by" ws order_clause'],
                      "order_clause": ['(ordering_term ws "," ws order_clause)', 'ordering_term'],
                      "ordering_term": ['(ws expr ws ordering)', '(ws expr)'],
                      "ordering": ['(ws "asc")', '(ws "desc")'],
                      "col_ref": ['(table_name ws "." ws column_name)', 'column_name'],
                      "table_source": ['(table_name ws "as" ws table_alias)', 'table_name'],
                      "table_name": ["table_alias"], "table_alias": ['"t1"', '"t2"', '"t3"', '"t4"'], "column_name": [],
                      "ws": ['~"\s*"i'], 'wsp': ['~"\s+"i'],
                      "expr": ['in_expr',
                               # Like expressions.
                               '(value wsp "like" wsp string)',
                               # Between expressions.
                               '(value ws "between" wsp value ws "and" wsp value)',
                               # Binary expressions.
                               '(value ws binaryop wsp expr)',
                               # Unary expressions.
                               '(unaryop ws expr)',
                               'source_subq',
                               'value'],
                      "in_expr": ['(value wsp "not" wsp "in" wsp string_set)',
                                  '(value wsp "in" wsp string_set)',
                                  '(value wsp "not" wsp "in" wsp expr)',
                                  '(value wsp "in" wsp expr)'],
                      "value": ['parenval', '"YEAR(CURDATE())"', 'number', 'boolean',
                                'function', 'col_ref', 'string'], "parenval": ['"(" ws expr ws ")"'],
                      "function": ['(fname ws "(" ws "distinct" ws arg_list_or_star ws ")")',
                                   '(fname ws "(" ws arg_list_or_star ws ")")'],
                      "arg_list_or_star": ['arg_list', '"*"'], "arg_list": ['(expr ws "," ws arg_list)', 'expr'],
                      "non_literal_number": ['"1"', '"2"', '"3"', '"4"'], "number": ['ws "value" ws'],
                      "string_set": ['ws "(" ws string_set_vals ws ")"'],
                      "string_set_vals": ['(string ws "," ws string_set_vals)', 'string'],
                      "string": ['"\'" ws "value" ws "\'"'],
                      "fname": ['"count"', '"sum"', '"max"', '"min"', '"avg"', '"all"'],
                      "boolean": ['"true"', '"false"'], "binaryop": ['"+"', '"-"', '"*"', '"/"', '"="', '"!="', '"<>"',
                                                                     '">="', '"<="', '">"', '"<"', '"and"', '"or"',
                                                                     '"like"'],
                      "unaryop": ['"+"', '"-"', '"not"', '"not"']}


# GRAMMAR_DICTIONARY["source_subq"] = ['("(" ws query ws ")" ws "as" ws name)', '("(" ws query ws ")")']

# TODO(MARK): Massive hack, remove and modify the grammar accordingly
# GRAMMAR_DICTIONARY["number"] = ['~"\d*\.?\d+"i', "'3'", "'4'"]
# GRAMMAR_DICTIONARY["string"] = ['~"\'.*?\'"i']

# TODO(MARK): This is not tight enough. AND/OR are strictly boolean value operators.


def update_grammar_with_tables(grammar_dictionary: Dict[str, List[str]],
                               schema: Dict[str, Table]) -> None:
    table_names = sorted([f'"{table.lower()}"' for table in
                          list(schema.keys())], reverse=True)
    grammar_dictionary['table_name'] += table_names

    all_columns = set()
    for table in schema.values():
        all_columns.update(
            [f'"{table.name.lower()}@{column.name.lower()}"' for column in table.columns if column.name != '*'])
    sorted_columns = sorted([column for column in all_columns], reverse=True)
    grammar_dictionary['column_name'] += sorted_columns


def update_grammar_to_be_table_names_free(grammar_dictionary: Dict[str, List[str]]):
    """
    Remove table names from column names, remove aliases
    """

    grammar_dictionary["column_name"] = []
    grammar_dictionary["table_name"] = []
    grammar_dictionary["col_ref"] = ['column_name']
    grammar_dictionary["table_source"] = ['table_name']

    del grammar_dictionary["table_alias"]


def update_grammar_flip_joins(grammar_dictionary: Dict[str, List[str]]):
    """
    Remove table names from column names, remove aliases
    """

    # using a simple rule such as join_clauses-> [(join_clauses ws join_clause), join_clause]
    # resulted in a max recursion error, so for now just using a predefined max
    # number of joins
    grammar_dictionary["join_clauses"] = ['(join_clauses_1 ws join_clause)', 'join_clause']
    grammar_dictionary["join_clauses_1"] = ['(join_clauses_2 ws join_clause)', 'join_clause']
    grammar_dictionary["join_clauses_2"] = ['(join_clause ws join_clause)', 'join_clause']
