# coding: utf-8
"""Commands settings.
   """
import datetime

def __prepare__(field, argument):
    if type(argument) is list:
        dtype = type(argument[0]) if argument != [] else int
    
    elif type(argument) is dict:
        dtype = type(argument.get(list(argument.keys())[0]))
    
    else:
        dtype = type(argument)


    if dtype is str:
        if type(argument) is list:
            argument = list(map(lambda x: x.lower(), argument))
            
        elif type(argument) is dict:
            argument = { arg[0].lower(): arg[1] for arg in list(map(lambda x: (x, argument[x].lower()), argument))} 
        
        else:
            argument = argument.lower()
    
        field = f'lower({field})'
    
    if dtype == datetime.datetime:
        if type(argument) is list:
            pass
        elif type(argument) is dict:
            argument = { arg[0].lower(): arg[1] for arg in list(map(lambda x: (x, argument[x]), argument))} 
    
    return field, argument, dtype

def translator_eq(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"{quotes}{argument}{quotes}"
    return f"{field} = {argument}"

def translator_diff(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"{quotes}{argument}{quotes}"
    return f"{field} != {argument}"

def translator_gt(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"{quotes}{argument}{quotes}"
    return f"{field} > {argument}"

def translator_gte(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"{quotes}{argument}{quotes}"
    return f"{field} >= {argument}"

def translator_lt(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"{quotes}{argument}{quotes}"
    return f"{field} < {argument}"

def translator_lte(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"{quotes}{argument}{quotes}"
    return f"{field} <= {argument}"

def translator_ct(field, argument):
    field, argument, _ = __prepare__(field, argument)
    argument = f"'%{argument}%'"
    return f"{field} like {argument}"

def translator_ctx(field, argument):
    field, argument, _ = __prepare__(field, argument)
    argument2 = f"'% {argument} %'"
    return f"({field} like {argument2} or {field} = '{argument}')"

def translator_sw(field, argument):
    field, argument, _ = __prepare__(field, argument)
    argument = f"'{argument}%'"
    return f"{field} like {argument}"

def translator_swx(field, argument):
    field, argument, _ = __prepare__(field, argument)
    argument2 = f"'{argument} %'"
    return f"({field} like {argument2} or {field} = '{argument}')"

def translator_ew(field, argument):
    field, argument, _ = __prepare__(field, argument)
    argument = f"'%{argument}'"
    return f"{field} like {argument}"

def translator_ewx(field, argument):
    field, argument, _ = __prepare__(field, argument)
    argument2 = f"'% {argument}'"
    return f"({field} like {argument2} or {field} = '{argument}')"

def translator_in(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    argument = f"({quotes}" + f"{quotes},{quotes}".join(map(lambda x: str(x), argument)) + f"{quotes})"
    return f"{field} in {argument}"

def translator_bt(field, argument):
    field, argument, dtype = __prepare__(field, argument)
    quotes = "'" if dtype in [str, datetime.datetime] else ""
    inf = f"{quotes}{argument.get('min')}{quotes}"
    sup = f"{quotes}{argument.get('max')}{quotes}"
    return f"({field} between {inf} and {sup})"
