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
            argument = list(map(lambda x: f"'{x.lower()}'", argument))
            
        elif type(argument) is dict:
            argument = { arg[0].lower(): f"'{arg[1]}'" for arg in list(map(lambda x: (x, argument[x].lower()), argument))} 
        
        else:
            argument = f"'{argument.lower()}'"
    
        field = f'lower({field})'
    
    if dtype == datetime.datetime:
        if type(argument) is list:
            pass
        elif type(argument) is dict:
            argument = { arg[0].lower(): f"'{arg[1]}'" for arg in list(map(lambda x: (x, argument[x]), argument))} 
        else:
            argument = f"'{argument}'"

    return field, argument


def translator_eq(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}=={argument}".strip()

def translator_diff(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}!={argument}".strip()


def translator_gt(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}>{argument}".strip()


def translator_gte(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}>={argument}".strip()


def translator_lt(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}<{argument}".strip()


def translator_lte(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}<={argument}".strip()


def translator_ct(field, argument):
    field, argument = __prepare__(field, argument)
    argument = f"{argument[0]}%{argument[1:-1]}%{argument[-1]}"
    return f"{field} like {argument}".strip()


def translator_ctx(field, argument):
    field, argument = __prepare__(field, argument)
    argument2 = f"{argument[0]}% {argument[1:-1]} %{argument[-1]}"
    return f"({field} like {argument2} or {field} == {argument})".strip()


def translator_sw(field, argument):
    field, argument = __prepare__(field, argument)
    argument = f"{argument[:-1]}%{argument[-1]}"
    return f"{field} like {argument}".strip()


def translator_swx(field, argument):
    field, argument = __prepare__(field, argument)
    argument2 = f"{argument[:-1]} %{argument[-1]}"
    return f"({field} like {argument2} or {field} == {argument})".strip()

def translator_ew(field, argument):
    field, argument = __prepare__(field, argument)
    argument = f"{argument[0]}%{argument[1:]}"
    return f"{field} like {argument}".strip()

def translator_ewx(field, argument):
    field, argument = __prepare__(field, argument)
    argument2 = f"{argument[0]}% {argument[1:]}"
    return f"({field} like {argument2} or {field} == {argument})".strip()

def translator_in(field, argument):
    field, argument = __prepare__(field, argument)
    argument = f"({','.join(argument)})"
    return f"{field} in {argument}".strip()

def translator_bt(field, argument):
    field, argument = __prepare__(field, argument)
    inf = argument.get('min')
    sup = argument.get('max')
    return f"({field} >= {inf} and {field} <= {sup})".strip()
