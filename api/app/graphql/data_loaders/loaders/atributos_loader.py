# coding: utf-8
"""atributos_loader.
    """

from app import db
from sqlalchemy import text

def load_atributos(**kwargs):
    
    atributos = {
                 'DATA_ATUAL': db.engine.execute(text("select DATA_ATUAL from data_atual")).scalar(),
                 'SIT_CONVENIO': [
                                  valor[0]
                                  for valor in 
                                  db.engine.execute(text("select VALOR from atributos " \
                                                         "where atributo='SIT_CONVENIO'"\
                                                         "order by VALOR"))
                                  ],
                 'NATUREZA_JURIDICA': [
                                       valor[0]
                                       for valor in 
                                       db.engine.execute(text("select VALOR from atributos " \
                                                                "where atributo='NATUREZA_JURIDICA'"\
                                                                "order by VALOR"))

                                       ],
                 'MODALIDADE_TRANSFERENCIA': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='MODALIDADE_TRANSFERENCIA'"\
                                                        "order by VALOR"))
                                ],
                 'TIPO_PARLAMENTAR': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='TIPO_PARLAMENTAR'"\
                                                        "order by VALOR"))                              
                                ],
                 'MODALIDADE_COMPRA': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='MODALIDADE_COMPRA'"\
                                                        "order by VALOR"))                                
                                ],
                 'TIPO_LICITACAO': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='TIPO_LICITACAO'"\
                                                        "order by VALOR"))  
                                ],
                 'FORMA_LICITACAO': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='FORMA_LICITACAO'"\
                                                        "order by VALOR"))  
                                ],
                 'STATUS_LICITACAO': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='STATUS_LICITACAO'"\
                                                        "order by VALOR"))  
                                ]
                 }


    return atributos, None