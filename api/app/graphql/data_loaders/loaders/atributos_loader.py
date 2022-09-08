# coding: utf-8
"""atributos_loader.
    """

from app import db
from sqlalchemy import text

def load_atributos(**kwargs):

    data_atual = db.engine.execute(text("select DATA_ATUAL from data_atual")).scalar()
    result = db.engine.execute(text("select ATRIBUTO, VALOR from atributos order by ATRIBUTO, VALOR"))
    
    data = {}
    data['DATA_ATUAL'] = data_atual
    for row in result:
        if data.get(row['ATRIBUTO']) is None:
            data[row['ATRIBUTO']] = []
        data[row['ATRIBUTO']] += [row['VALOR']]

    return data, None
