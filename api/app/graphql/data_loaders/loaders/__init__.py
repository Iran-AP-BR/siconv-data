# coding: utf-8
"""loaders.
    """

from tkinter.messagebox import NO
from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
from math import ceil
from app import db
from sqlalchemy import text

 
def make_where_clause(conditions='', where_preffix=True):
    '''Returns a string representing a SQL WHERE clause based on a conditions list. The conditions 
       list elements are concatenated together into a single string, conncected by "and" operators.
       The word WHERE may be ommited by passing where_preffix parameter as False.

    '''
    assert type(conditions) in (list, tuple, str), 'assertion exception: make_where_clause()'

    conditions_str = ' and '.join(conditions) if type(conditions) in (list, tuple) else conditions

    where_clause = f"{'where ' if where_preffix else ''}({conditions_str})" if conditions_str else ''
    
    return where_clause

def set_period_filter(field_name=None, data_inicial=None, data_final=None, filter_list=None):
    '''Returns a string representing a SQL WHERE clause (without the word WHERE) to check whether a 
       date, or a period, belongs to a date range. A period belongs to a date range if at least 
       a portion of it is in date range, in other words, if there is an intersection between period 
       and data range.
    '''

    assert type(filter_list) in (list, tuple, type(None)), 'assertion exception 1: set_period_filter()'
    assert type(field_name) in (list, tuple, str), 'assertion exception 2: set_period_filter()'

    date_filter = ""
    
    if type(field_name) in (list, tuple):
        date_filter_list = set_period_filter(field_name=field_name[0], data_final=data_final, 
                                        filter_list=[])
        date_filter_list = set_period_filter(field_name=field_name[1], data_inicial=data_inicial,
                                        filter_list=date_filter_list)
        if date_filter_list:
            date_filter = f"({' and '.join(date_filter_list)})"

    elif data_inicial is not None or data_final is not None:
        date_conditions = []
        if data_inicial is not None:
            date_conditions += [f"{field_name} >= '{data_inicial.strftime('%Y-%m-%d')}'"]
        
        if data_final is not None:
            date_conditions += [f"{field_name} <= '{data_final.strftime('%Y-%m-%d')}'"]

        date_filter = f"({' and '.join(date_conditions)})"
        
    if filter_list is None:
        result = date_filter
    elif date_filter:
        filter_list += [f"{date_filter}"]
        result = filter_list
    else:
        result = filter_list

    return result
    
def sort_constructor(sort):
    if sort is not None:
        return 'order by ' + ','.join([f"{field} {sort.get('order')[p]}" 
                                       for p, field in enumerate(sort.get('fields'))])

    return ''

def pagination_constructor(conditions=None, page_specs=None, items_count=None):

    page = page_specs.get(
        'page') if page_specs and page_specs.get('page') else 1
    page_length = page_specs.get('page_length') if page_specs and page_specs.get(
        'page_length') else app.config.get('GRAPHQL_DEFAULT_PAGE_LENGTH')
    
    page_count = ceil(items_count / page_length)

    if page > page_count:
        page = page_count

    if page == 0:
        page_length = 0

    offset = (page - 1) * page_length if page > 0 else 0

    page_specs = {'page': page, 'page_length': page_length}

    pagination = {
        "page": page,
        "page_length": page_length,
        "page_count": page_count,
        "items_count": items_count
    }

    return pagination, offset, page_specs


def load_data(table_expression=None, selected_fields=None, filters=None, sort=None,
               page_specs=None, use_pagination=True, distinct_clause=False):
    assert selected_fields is not None, 'No selected field provided'
    assert table_expression is not None, 'No table expression provided'

    pagination = None
    distinct = 'distinct' if distinct_clause else ''
    limit = ''

    where = f"where {filter_constructor(filters=filters)}" if filters else ''
    order_by = sort_constructor(sort)
    for key in selected_fields:
        where = where.replace(key, selected_fields[key])
    
    sql = f"select {distinct} \
           {', '.join([f'{selected_fields[key]} as {key}' for key in selected_fields])} \
            from {table_expression} {where}"

    if use_pagination:
        items_count = db.engine.execute(text(f"select count(*) from ({sql}) aa")).scalar()
        pagination, offset, page_specs = pagination_constructor(page_specs=page_specs, items_count=items_count)
        limit = f"limit {offset}, {page_specs.get('page_length')}"
    
    sql = f"{sql} {order_by} {limit}"
    result = db.engine.execute(text(sql))

    data = [dict(row) for row in result]

    return data, pagination
