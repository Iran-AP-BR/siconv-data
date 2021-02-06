# coding: utf-8
"""Filtering.
   """

from functools import reduce
from app.graphql.commands import Command


def resolver_eq(field, argument, negation=False):
    neg = 'not' if negation else ''
    if argument == "''":
        return f"{neg} {field}.isna()".strip()
    else:
        return f"{neg} {field}=={argument}".strip()
    
def resolver_gt(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}>{argument}".strip()

def resolver_lt(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}<{argument}".strip()

def resolver_ct(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.str.contains({argument}, na=False)".strip()

def resolver_sw(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.str.startswith({argument}, na=False)".strip()

def resolver_ew(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.str.endswith({argument}, na=False)".strip()

def resolver_in(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.isin({argument})".strip()

def resolver_bt(field, argument, negation=False):
    neg = 'not' if negation else ''
    inf = f"'{argument[0]}'" if type(argument[0]) == str else argument[0]
    sup = f"'{argument[1]}'" if type(argument[1]) == str else argument[1]
    return f"{neg} {field}.between({inf}, {sup})".strip()


def filter_constructor(parameters, dtypes=None, parse_dates=[]):
    eq = Command(name='eq', data_type='*', split=False, split_length=0, resolver=resolver_eq)
    ct = Command(name='ct', data_type='str', split=False, split_length=0, resolver=resolver_ct, default='eq')
    sw = Command(name='sw', data_type='str', split=False, split_length=0, resolver=resolver_sw, default='eq')
    ew = Command(name='ew', data_type='str', split=False, split_length=0, resolver=resolver_ew, default='eq')
    gt = Command(name='gt', data_type='*', split=False, split_length=0, resolver=resolver_gt, default='eq')
    lt = Command(name='lt', data_type='*', split=False, split_length=0, resolver=resolver_lt, default='eq')
    _in = Command(name='in', data_type='*', split=True, split_length=0, resolver=resolver_in, default='eq')
    bt = Command(name='bt', data_type='*', split=True, split_length=2, resolver=resolver_bt, default='eq')
    
    commands = [eq, ct, sw, ew, gt, lt, _in, bt]

    conditions = []
    for key in parameters.keys():
        cmd_found = False
        for cmd in commands:
            condition = cmd.get_condition(field=key, line=parameters.get(key),dtypes=dtypes, parse_dates=parse_dates)
            if condition is not None:
                conditions += [condition]
                cmd_found = True
                break        

        if not cmd_found:
            raise Exception(f'Comando desconhecido: "{parameters.get(key)}".')
        
    
    if len(conditions) == 0:
        return None

    return reduce(lambda x, y: f'{x} and {y}', conditions)
