# coding: utf-8
"""Filtering.
   """

from .commands_resolvers import *
from .commands import Command, parser
import re
from uuid import uuid4

LOGICAL_CONNECTORS = {
    '&&': 'and',
    '||': 'or'
    }

PARENTHESES_TOKEN_PATTERN = '^[0-9a-f]{32}$'

commands = {
    'eq': Command(name='eq', data_type='*', split=False, split_length=0, resolver=resolver_eq), 
    'ct': Command(name='ct', data_type='str', split=False, split_length=0, resolver=resolver_ct, default='eq'),
    'ctx': Command(name='ctx', data_type='str', split=False, split_length=0, resolver=resolver_ctx, default='eq'),
    'sw': Command(name='sw', data_type='str', split=False, split_length=0, resolver=resolver_sw, default='eq'),
    'swx': Command(name='swx', data_type='str', split=False, split_length=0, resolver=resolver_swx, default='eq'),
    'ew': Command(name='ew', data_type='str', split=False, split_length=0, resolver=resolver_ew, default='eq'),
    'ewx': Command(name='ewx', data_type='str', split=False, split_length=0, resolver=resolver_ewx, default='eq'),
    'gt': Command(name='gt', data_type='*', split=False, split_length=0, resolver=resolver_gt, default='eq'),
    'gte': Command(name='gte', data_type='*', split=False, split_length=0, resolver=resolver_gte, default='eq'),
    'lt': Command(name='lt', data_type='*', split=False, split_length=0, resolver=resolver_lt, default='eq'),
    'lte': Command(name='lte', data_type='*', split=False, split_length=0, resolver=resolver_lte, default='eq'),
    'in': Command(name='in', data_type='*', split=True, split_length=0, resolver=resolver_in, default='eq'),
    'bt': Command(name='bt', data_type='*', split=True, split_length=2, resolver=resolver_bt, default='eq')
    }

def translate(tree, field, dtypes, parse_dates=[]):    
    for t in tree:
        text = tree[t]['tokenized'] if tree[t].get('tokenized') else tree[t]['text']
        text = text[1:-1] #descards parentheses
        logical_operators_pattern = '|'.join([''.join([f'\{c}' for c in connector]) for connector in LOGICAL_CONNECTORS])
        lines = re.split(logical_operators_pattern, text)
        connections = re.findall(logical_operators_pattern, text) 

        condition = ''
        
        for line in lines:
            line = line.strip()
            if not re.match(PARENTHESES_TOKEN_PATTERN, line):
                command, _, _ = parser(line, 'eq')
                
                if command not in commands.keys():
                    raise Exception(f'Comando desconhecido: "{command}".')

                cond = commands[command].get_condition(field=field, line=line, dtypes=dtypes, parse_dates=parse_dates)
            else:
                cond = line
                
            if connections:
                condition += f'{cond} {LOGICAL_CONNECTORS[connections[0]]} '
                connections.pop(0)
            else:
                condition += cond
        
        condition = f'({condition})' #restores parentheses
        
        if tree[t].get('tokenized'):
            tree[t]['tokenized'] = condition
        else:
            tree[t]['text'] = condition
        
    return tree


def set_tree(expression, marks=None, tree=None, id=None):
    def new_token():
        return uuid4().hex

    if not marks:
        marks = [(m.group(), m.start()) for m in re.compile('\(|\)').finditer(expression)]
        
    mark = marks.pop(0)
    complete = False
    if mark[0] == ')':
        tree[id]['end'] = mark[1]
        tree[id]['text'] = expression[tree[id]['start']:tree[id]['end']+1]
        
        if not tree[id]['parent']:
            complete = True
            
        id = tree[id]['parent']
    else:
        parent_id = id
        id = new_token()
        if not tree:
            tree = {}
        
        tree[id] = {'parent': parent_id, 'start': mark[1]} 
            

    if len(marks) > 0 and not complete:
        return set_tree(expression, marks, tree, id)
    
    if len(marks) == 0 and not complete:
        raise Exception('Parentheses not closed.')
    
    if len(marks) > 0 and complete:
        raise Exception('Too many Parentheses.')
        
    #tokenize
    for t in tree:
        p = tree[t]['parent']
        if p:
            text = tree[p]['tokenized'] if tree[p].get('tokenized') else tree[p]['text']
            tree[p]['tokenized'] = text.replace(tree[t]['text'], t)
        
    return tree

def reconstruction(tree):
    if len(tree) > 1:
        leaves = set(list(tree.keys())) - set([tree[t]['parent'] for t in tree])
        for lv in leaves:
            p = tree[lv]['parent']
            text = tree[lv]['tokenized'] if tree[lv].get('tokenized') else tree[lv]['text']
            tree[p]['tokenized'] = tree[p]['tokenized'].replace(lv, text)
            tree.pop(lv)

        return reconstruction(tree)
    
    last_node = list(tree.values())[0]
    return last_node['tokenized'] if last_node.get('tokenized') else last_node['text']

def translate_commands(expression, field, dtypes, parse_dates=[]):
    expression = expression.strip()
    expression = f'({expression})' #extra parentheses to make sure that there'll be one root node in the tree
        
    tree = set_tree(expression)
    
    tree = translate(tree, field=field, dtypes=dtypes, parse_dates=parse_dates)
    
    expression = reconstruction(tree)
    return expression[1:-1] #discards extra parentheses and return expression translated

def filter_constructor(parameters, dtypes=None, parse_dates=[]):
   
    if not parameters:
        return None

    conditions = ''
    for key in parameters.keys():
        condition = translate_commands(parameters.get(key).strip(), field=key, dtypes=dtypes, parse_dates=parse_dates)
       
        conditions += f' and ({condition})' if conditions else f'({condition})'  
   
    return conditions
