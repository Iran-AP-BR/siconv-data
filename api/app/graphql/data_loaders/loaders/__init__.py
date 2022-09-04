# coding: utf-8
"""loaders.
    """

from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
from math import ceil
from app import db
from sqlalchemy import text


def set_period_filter(field_name, data_inicial, data_final):
    date_filter = ""
    if data_inicial is not None or data_final is not None:
        date_conditions = []
        if data_inicial is not None:
            date_conditions += [f"{field_name} >= '{data_inicial.strftime('%Y-%m-%d')}'"]
        
        if data_final is not None:
            date_conditions += [f"{field_name} <= '{data_final.strftime('%Y-%m-%d')}'"]

        date_filter = f"({' and '.join(date_conditions)})"
    
    return date_filter
    
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
    assert selected_fields is not None
    assert table_expression is not None

    pagination = None
    distinct = 'distinct' if distinct_clause else ''
    limit = ''

    where = f"where {filter_constructor(filters=filters)}" if filters else ''
    
    order_by = sort_constructor(sort)

    for k in selected_fields:
        where = where.replace(k, selected_fields[k])
        order_by = order_by.replace(k, selected_fields[k])


    sql = f"select {distinct} {', '.join(selected_fields.values())} from {table_expression} {where}"
    if use_pagination:
        items_count = db.engine.execute(text(f"select count(*) from ({sql}) aa")).scalar()
        pagination, offset, page_specs = pagination_constructor(page_specs=page_specs, items_count=items_count)
        limit = f"limit {offset}, {page_specs.get('page_length')}"
    
    sql = f"{sql} {order_by} {limit}"
    result = db.engine.execute(text(sql))
    
    data = [{list(selected_fields.keys())[p]: r for p, r in enumerate(row)} for row in result]

    return data, pagination
