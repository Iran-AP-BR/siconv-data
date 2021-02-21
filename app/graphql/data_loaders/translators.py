# coding: utf-8
"""Commands settings.
   """
import datetime

def __prepare__(field, argument):
    if type(argument) is list:
        dtype = type(argument[0])
    else:
        dtype = type(argument)
    
    if dtype is str:
        if type(argument) is list:
            argument = list(map(lambda x: x.lower(), argument))
        else:
            argument = f"'{argument.lower()}'"
    
        field = f'{field}.str.lower()'
    
    if dtype == datetime.datetime:
        if type(argument) is not list:
            argument = f"'{argument}'"

    return field, argument


def translator_eq(field, argument):
    field, argument = __prepare__(field, argument)

    if argument == "''":
        return f"{field}.isna()".strip()
    else:
        return f"{field}=={argument}".strip()


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
    inf = f"'{argument[0]}'" if type(argument[0]) == str else argument[0]
    sup = f"'{argument[1]}'" if type(argument[1]) == str else argument[1]
    field, _ = __prepare__(field, argument)
    return f"{field}.between({inf}, {sup})".strip()
