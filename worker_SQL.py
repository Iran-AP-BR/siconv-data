# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path
from datetime import datetime
from app.logger import app_log

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from app.config import Config

config = Config()

metadata = sa.MetaData()
Base = declarative_base(metadata=metadata)
engine = sa.create_engine(Config.SQLALCHEMY_DATABASE_URI)


parse_dates_convenios = ['DIA_ASSIN_CONV', 'DIA_PUBL_CONV',
                         'DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS']
parse_dates_movimento = ['DATA']

dtypes_convenios = {
    'NR_CONVENIO': 'object',
    'DIA_ASSIN_CONV': 'object',
    'SIT_CONVENIO': 'object',
    'INSTRUMENTO_ATIVO': 'object',
    'DIA_PUBL_CONV': 'object',
    'DIA_INIC_VIGENC_CONV': 'object',
    'DIA_FIM_VIGENC_CONV': 'object',
    'DIA_LIMITE_PREST_CONTAS': 'object',
    'VL_GLOBAL_CONV': 'float64',
    'VL_REPASSE_CONV': 'float64',
    'VL_CONTRAPARTIDA_CONV': 'float64',
    'COD_ORGAO_SUP': 'object',
    'DESC_ORGAO_SUP': 'object',
    'NATUREZA_JURIDICA': 'object',
    'COD_ORGAO': 'object',
    'DESC_ORGAO': 'object',
    'MODALIDADE': 'object',
    'IDENTIF_PROPONENTE': 'object',
    'OBJETO_PROPOSTA': 'object'
}

dtypes_movimento = {
    'NR_CONVENIO': 'object',
    'DATA': 'object',
    'VALOR': 'float64',
    'TIPO': 'object',
    'IDENTIF_FORNECEDOR': 'object',
    'NOME_FORNECEDOR': 'object'
}

dtypes_emendas_convenios = {
    'NR_EMENDA': 'object',
    'NR_CONVENIO': 'object',
    'VALOR_REPASSE_EMENDA': 'float64'
}

dtypes_municipios = {
    'codigo_ibge': 'object',
    'nome_municipio': 'object',
    'codigo_uf': 'object',
    'uf': 'object',
    'estado': 'object',
    'latitude': 'float64',
    'longitude': 'float64'
}

class UpToDateException(Exception):
    """Custom exception to be raised data is up to date"""
    
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

class UnchangedException(Exception):
    """Custom exception to be raised the data was not changed since last download"""
    
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

def datetime_validation(txt):
    try:
        dtm = txt.split(' ')
        dt = dtm[0].split('/')
        tm = dtm[1].split(':')
        if len(dtm) != 2 or len(dt) != 3 or len(tm) != 3:
            raise
        dtime = datetime(int(dt[2]), int(dt[1]), int(dt[0]), int(tm[0]), int(tm[1]), int(tm[2]))
    except Exception as e:
        return None

    return dtime

def feedback(label='', value=''):
    label_length = 30
    value_length = 30
    label = label + ' ' + '-'*label_length
    value = '-'*value_length + ' ' + value
    app_log.info(label[:label_length] + value[-value_length:])

        
def updateSQL():
    def read_data(tbl_name, compression=config.COMPRESSION_METHOD, dtypes=str, parse_dates=[], decimal=','):
        tbl = pd.read_csv(os.path.join(config.DATA_FOLDER, f'{tbl_name}{config.FILE_EXTENTION}'),
            compression=compression, sep=';', decimal=decimal, dayfirst=True, dtype=dtypes,
            parse_dates=parse_dates)
        return tbl

    def get_current_date():
        with open(os.path.join(config.DATA_FOLDER, config.CURRENT_DATE_FILENAME), 'r') as fd:
            return fd.read()

        return ''

    try:      

        app_log.info('[Updating SQL]')

        feedback(label='-> proponentes', value='updating...')
        proponentes = read_data(tbl_name='proponentes')
        proponentes.to_sql('proponentes', con=engine, if_exists='replace', index=False)
        feedback(label='-> proponentes', value='Success!')

        feedback(label='-> convenios', value='updating...')
        convenios = read_data(tbl_name='convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
        convenios.to_sql('convenios', con=engine, if_exists='replace', index=False)
        feedback(label='-> convenios', value='Success!')

        feedback(label='-> situacoes', value='updating...')
        situacoes = convenios[['SIT_CONVENIO']].drop_duplicates().fillna('#indefinido')
        situacoes.to_sql('situacoes', con=engine, if_exists='replace', index=False)
        feedback(label='-> situacoes', value='Success!')

        feedback(label='-> naturezas', value='updating...')
        naturezas = convenios[['NATUREZA_JURIDICA']].drop_duplicates().fillna('#indefinido')
        naturezas.to_sql('naturezas', con=engine, if_exists='replace', index=False)
        feedback(label='-> naturezas', value='Success!')

        feedback(label='-> modalidades', value='updating...')
        modalidades = convenios[['MODALIDADE']].drop_duplicates().fillna('#indefinido')
        modalidades.to_sql('modalidades', con=engine, if_exists='replace', index=False)
        feedback(label='-> modalidades', value='Success!')
        
        feedback(label='-> emendas', value='updating...')
        emendas = read_data(tbl_name='emendas')
        emendas.to_sql('emendas', con=engine, if_exists='replace', index=False)
        feedback(label='-> emendas', value='Success!')

        feedback(label='-> emendas_convenios', value='updating...')
        emendas_convenios = read_data(tbl_name='emendas_convenios', dtypes=dtypes_emendas_convenios)
        emendas_convenios.to_sql('convenios_emendas_association', con=engine, if_exists='replace', index=False)
        feedback(label='-> emendas_convenios', value='Success!')

        feedback(label='-> movimento', value='updating...')
        movimento = read_data(tbl_name='movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)
        movimento.to_sql('movimento', con=engine, if_exists='replace', index=True, index_label='MOV_ID')
        feedback(label='-> movimento', value='Success!')

        feedback(label='-> municipios', value='updating...')
        municipios = read_data(tbl_name='municipios', dtypes=dtypes_municipios, decimal='.')
        municipios.to_sql('municipios', con=engine, if_exists='replace', index=False)
        feedback(label='-> municipios', value='Success!')

        feedback(label='-> data atual', value='updating...')
        data_atual = pd.DataFrame({'data_atual': [datetime_validation(get_current_date())]}).astype('datetime64[ns]')
        data_atual.to_sql('data_atual', con=engine, if_exists='replace', index=False)
        feedback(label='-> data atual', value='Success!')

        app_log.info('Processo finalizado com sucesso!')

    except UpToDateException as e:
        app_log.info(e.message)
        return True
    except UnchangedException as e:
        app_log.info(e.message)
        return False
    except Exception as e:
        app_log.info(repr(e))
        app_log.info('Processo falhou!')
        return False
    
    return True


if __name__ == '__main__':
    updateSQL()