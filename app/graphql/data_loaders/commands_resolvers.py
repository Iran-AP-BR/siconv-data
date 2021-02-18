# coding: utf-8
"""Commands settings.
   """


def resolver_eq(field, argument, negation=False):
    neg = 'not' if negation else ''
    if argument == "''":
        return f"{neg} {field}.isna()".strip()
    else:
        return f"{neg} {field}=={argument}".strip()


def resolver_gt(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}>{argument}".strip()


def resolver_gte(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}>={argument}".strip()


def resolver_lt(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}<{argument}".strip()


def resolver_lte(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}<={argument}".strip()


def resolver_ct(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.str.contains({argument}, na=False)".strip()


def resolver_ctx(field, argument, negation=False):
    neg = 'not' if negation else ''
    argument = "'\\b" + argument[1:-1] + "\\b'"
    return f"{neg} {field}.str.contains(r{argument}, na=False, regex=True)".strip()


def resolver_sw(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.str.startswith({argument}, na=False)".strip()


def resolver_swx(field, argument, negation=False):
    neg = 'not' if negation else ''
    argument = "'^" + argument[1:-1] + "\\b.*'"
    return f"{neg} {field}.str.match(r{argument}, na=False)".strip()


def resolver_ew(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.str.endswith({argument}, na=False)".strip()


def resolver_ewx(field, argument, negation=False):
    neg = 'not' if negation else ''
    argument = "'.*\\b" + argument[1:-1] + "$'"
    return f"{neg} {field}.str.match(r{argument}, na=False)".strip()


def resolver_in(field, argument, negation=False):
    neg = 'not' if negation else ''
    return f"{neg} {field}.isin({argument})".strip()


def resolver_bt(field, argument, negation=False):
    neg = 'not' if negation else ''
    inf = f"'{argument[0]}'" if type(argument[0]) == str else argument[0]
    sup = f"'{argument[1]}'" if type(argument[1]) == str else argument[1]
    return f"{neg} {field}.between({inf}, {sup})".strip()
