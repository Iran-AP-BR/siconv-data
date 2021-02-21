# coding: utf-8
"""Filtering.
   """

from .translators import *

translators = {
    'eq': translator_eq,
    'ct': translator_ct,
    'ctx': translator_ctx,
    'sw': translator_sw,
    'swx': translator_swx,
    'ew': translator_ew,
    'ewx': translator_ewx,
    'gt': translator_gt,
    'gte': translator_gte,
    'lt': translator_lt,
    'lte': translator_lte,
    'in': translator_in,
    'bt': translator_bt
    }

def filter_constructor(filters):
    def transl(field, cmds):
        expr = ''
        op = ''
        for cmd in cmds:
            translated = translators[cmd](field, cmds[cmd])
            expr += f' {op} {translated}'
            op = 'and'
        
        return f'{expr.strip()}'

    if not filters:
        return None

    expr = ''
    opr = ''
    for f in filters:
        if f in ['OR', 'AND']:
            expr2 = ''
            opr2 = ''
            for d in filters[f]:
                expr2 += f' {opr2} ({filter_constructor(d)})'
                expr2 = expr2.strip()
                opr2 = 'and'
                
            opr3 = f.lower() if expr else ''
            expr += f' {opr3} {expr2}'
        elif f == 'NOT':
            expr += f' {opr} not ({filter_constructor(filters[f])})'
        else:
            expr += f' {opr} {transl(f, filters[f])}'
        
        expr = expr.strip()
        opr = 'and'

    return f'{expr}'

