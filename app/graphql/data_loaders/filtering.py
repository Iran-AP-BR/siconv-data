# coding: utf-8
"""Filtering.
   """

from .operators import Operator, parser
import re

PARENTHESES_BLOCK_TOKEN_PREFIX = '_#_b'
PARENTHESES_BLOCK_TOKEN_SUFFIX = '_#_'
PARENTHESES_BLOCK_TOKEN_PATTERN = f'^{PARENTHESES_BLOCK_TOKEN_PREFIX}\d{{1,2}}{PARENTHESES_BLOCK_TOKEN_SUFFIX}$'

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



operators = {
    'eq': Operator(name='eq', data_type='*', split=False, split_length=0, resolver=resolver_eq), 
    'ct': Operator(name='ct', data_type='str', split=False, split_length=0, resolver=resolver_ct, default='eq'),
    'sw': Operator(name='sw', data_type='str', split=False, split_length=0, resolver=resolver_sw, default='eq'),
    'ew': Operator(name='ew', data_type='str', split=False, split_length=0, resolver=resolver_ew, default='eq'),
    'gt': Operator(name='gt', data_type='*', split=False, split_length=0, resolver=resolver_gt, default='eq'),
    'lt': Operator(name='lt', data_type='*', split=False, split_length=0, resolver=resolver_lt, default='eq'),
    'in': Operator(name='in', data_type='*', split=True, split_length=0, resolver=resolver_in, default='eq'),
    'bt': Operator(name='bt', data_type='*', split=True, split_length=2, resolver=resolver_bt, default='eq')
    }


connectors = {
    '&&': 'and',
    '||': 'or'
    }


def tokenize_parentheses(expression):

    blocks = {}
    count = 1
    
    while True:
        groups = re.findall('\([^\(\)]*\)', expression)
        if not groups:
            block_id = f'{PARENTHESES_BLOCK_TOKEN_PREFIX}0{PARENTHESES_BLOCK_TOKEN_SUFFIX}'
            blocks[block_id] = expression
            expression = block_id
            break
            
        for gr in groups:
            block_id = f'{PARENTHESES_BLOCK_TOKEN_PREFIX}{count}{PARENTHESES_BLOCK_TOKEN_SUFFIX}'
            blocks[block_id] = gr
            expression = expression.replace(gr, block_id)
            count += 1
    
    return blocks
    

def translate(par, field, dtypes, parse_dates=[]):

    for p in par:
        if par[p][0] == '(' and par[p][-1] == ')':
            open_parenteses = par[p][0]
            close_parenteses = par[p][-1]
            par[p] = par[p][1:-1]
        else:
            open_parenteses = ''
            close_parenteses = ''
        
        logical_operators_pattern = '&&|\|\|'
        lines = re.split(logical_operators_pattern, par[p])
        connections = re.findall(logical_operators_pattern, par[p]) 

        condition = ''
        
        for line in lines:
            line = line.strip()
            if not re.match(PARENTHESES_BLOCK_TOKEN_PATTERN, line.strip()):
                operator, _, _ = parser(line, 'eq')
                
                if operator not in operators.keys():
                    raise Exception(f'Operador desconhecido: "{operator}".')

                cond = operators[operator].get_condition(field=field, line=line, dtypes=dtypes, parse_dates=parse_dates)
            else:
                cond = line.strip()

            if connections:
                condition += f'{cond} {connectors[connections[0]]} '
                connections.pop(0)
            else:
                condition += cond
        
        par[p] = f'{open_parenteses}{condition}{close_parenteses}'
        
    
    return par


def reconstruct_expression(par):
    
    par_list = list(par.keys())
    i = 0
    expr = f'{PARENTHESES_BLOCK_TOKEN_PREFIX}0{PARENTHESES_BLOCK_TOKEN_SUFFIX}'
    while par_list:
        bl = par_list[i]
        if bl in expr:
            expr = expr.replace(bl, par[bl])
            par_list.pop(i)
            i = 0
        else:
            i += 1
            if i >= len(par_list):
                i = 0

    return expr

def filter_constructor(parameters, dtypes=None, parse_dates=[]):
   
    if not parameters:
        return None

    conditions = ''
    for key in parameters.keys():
        par = tokenize_parentheses(parameters.get(key).strip())
       
        par = translate(par=par, field=key, dtypes=dtypes, parse_dates=parse_dates)

        condition = reconstruct_expression(par)
        
        conditions += f' and ({condition})' if conditions else f'({condition})'  
   
    return conditions






















'''
def filter_constructor(parameters, dtypes=None, parse_dates=[]):
    if not parameters:
        return None

    eq = Operator(name='eq', data_type='*', split=False, split_length=0, resolver=resolver_eq)
    ct = Operator(name='ct', data_type='str', split=False, split_length=0, resolver=resolver_ct, default='eq')
    sw = Operator(name='sw', data_type='str', split=False, split_length=0, resolver=resolver_sw, default='eq')
    ew = Operator(name='ew', data_type='str', split=False, split_length=0, resolver=resolver_ew, default='eq')
    gt = Operator(name='gt', data_type='*', split=False, split_length=0, resolver=resolver_gt, default='eq')
    lt = Operator(name='lt', data_type='*', split=False, split_length=0, resolver=resolver_lt, default='eq')
    _in = Operator(name='in', data_type='*', split=True, split_length=0, resolver=resolver_in, default='eq')
    bt = Operator(name='bt', data_type='*', split=True, split_length=2, resolver=resolver_bt, default='eq')
    

    operators = {'eq': eq, 'ct': ct, 'sw': sw, 'ew': ew, 'gt': gt, 'lt': lt, 'in': _in, 'bt': bt}
    connectors = {'&&': 'and', '||': 'or'}

    conditions = ''
    for key in parameters.keys():
        logical_operators_pattern = '&&|\|\|'
        lines = re.split(logical_operators_pattern, parameters.get(key))
        connections = re.findall(logical_operators_pattern, parameters.get(key)) 

        condition = ''
        for line in lines:
            operator, _, _ = parser(line, 'eq')

            if operator not in operators.keys():
                raise Exception(f'Operador desconhecido: "{operator}".')

            cond = operators[operator].get_condition(field=key, line=line, dtypes=dtypes, parse_dates=parse_dates)

            if connections:
                condition += f'{cond} {connectors[connections[0]]} '
                connections.pop(0)
            else:
                condition += cond
            
        conditions += f' and ({condition})' if conditions else f'({condition})'      

    return conditions
'''