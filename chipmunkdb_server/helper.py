import os
import sys
import importlib
import subprocess
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from sql_metadata import Parser

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                yield from extract_from_part(item)
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            # when its virtual_table we search for the name inside of the normalized sql
            if item.get_name() == "virtual_table":
                yield extract_tables(item.normalized)
            else:
                yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value


def extract_tables(sql):
    parser = Parser(sql)
    list = parser.tables
    return list

    #stream = extract_from_part(sqlparse.parse(sql)[0])
    #return list(extract_table_identifiers(stream))


currentfile = sys.argv[0]


is_in_ipynb = False

if not 'workbookDir' in globals():
    workbookDir = os.getcwd()


def getEnvironment():
    if (is_in_ipynb):
        return "dev"
    else:
        return "live"
