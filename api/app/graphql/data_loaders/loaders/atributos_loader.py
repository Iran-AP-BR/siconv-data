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
        if data.get(row[0]) is None:
            data[row[0]] = []
        data[row[0]] += [row[1]]

    return data, None
