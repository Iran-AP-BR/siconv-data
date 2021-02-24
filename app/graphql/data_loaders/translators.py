# coding: utf-8
"""Commands settings.
   """
import datetime

def __prepare__(field, argument):
    if type(argument) is list:
        dtype = type(argument[0])
    elif type(argument) is dict:
        dtype = type(argument.get(list(argument.keys())[0]))
    else:
        dtype = type(argument)


    if dtype is str:
        if type(argument) is list:
            argument = list(map(lambda x: x.lower(), argument))
        elif type(argument) is dict:
            argument = { arg[0].lower(): f"'{arg[1]}'" for arg in list(map(lambda x: (x, argument[x].lower()), argument))} 
        else:
            argument = f"'{argument.lower()}'"
    
        field = f'{field}.str.lower()'
    
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
    return f"{field}.str.contains({argument}, na=False)".strip()


def translator_ctx(field, argument):
    argument = "'\\b" + argument + "\\b'"
    field, _ = __prepare__(field, '')
    return f"{field}.str.contains(r{argument}, na=False, regex=True)".strip()


def translator_sw(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}.str.startswith({argument}, na=False)".strip()


def translator_swx(field, argument):
    argument = "'^" + argument + "\\b.*'"
    field, _ = __prepare__(field, argument)
    return f"{field}.str.match(r{argument}, na=False)".strip()


def translator_ew(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}.str.endswith({argument}, na=False)".strip()


def translator_ewx(field, argument):
    argument = "'.*\\b" + argument + "$'"
    field, _ = __prepare__(field, argument)
    return f"{field}.str.match(r{argument}, na=False)".strip()


def translator_in(field, argument):
    field, argument = __prepare__(field, argument)
    return f"{field}.isin({argument})".strip()


def translator_bt(field, argument):
    field, argument = __prepare__(field, argument)
    inf = argument.get('min')
    sup = argument.get('max')
    return f"{field}.between({inf}, {sup})".strip()
