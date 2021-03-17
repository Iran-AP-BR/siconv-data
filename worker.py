# -*- coding: utf-8 -*-

import pandas as pd
import requests
import os
from pathlib import Path
from datetime import datetime
from app.logger import app_log


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
    'MOV_ID': 'object',
    'NR_CONVENIO': 'object',
    'DATA': 'object',
    'VALOR': 'float64',
    'TIPO': 'object',
    'IDENTIF_FORNECEDOR': 'object',
    'NOME_FORNECEDOR': 'object'
}

dtypes_emendas_convenios = {
    'NR_EMENDA': 'object',
    'NR_CONVENIO': 'object'
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

#columns definitions
proponentes_cols = ["IDENTIF_PROPONENTE", "NM_PROPONENTE"]
proponentes_final_cols = ["IDENTIF_PROPONENTE", "NM_PROPONENTE", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE"]
proponentes_drop = ["NM_PROPONENTE", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE"]

propostas_cols = ["ID_PROPOSTA", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE", "COD_ORGAO_SUP", "DESC_ORGAO_SUP", "NATUREZA_JURIDICA", "COD_ORGAO", "DESC_ORGAO", "MODALIDADE", "IDENTIF_PROPONENTE", "OBJETO_PROPOSTA"]
convenios_cols = ["NR_CONVENIO", "ID_PROPOSTA", "DIA_ASSIN_CONV", "SIT_CONVENIO", "INSTRUMENTO_ATIVO", "DIA_PUBL_CONV", "DIA_INIC_VIGENC_CONV", "DIA_FIM_VIGENC_CONV", "DIA_LIMITE_PREST_CONTAS", "VL_GLOBAL_CONV", "VL_REPASSE_CONV", "VL_CONTRAPARTIDA_CONV"]

emendas_cols = ['ID_PROPOSTA', 'NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR']
emendas_drop_cols = ['ID_PROPOSTA', 'NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR']
emendas_final_cols = ['NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR']

emendas_convenios_cols = ['NR_EMENDA', 'NR_CONVENIO']

desembolsos_cols = ["ID_DESEMBOLSO", "NR_CONVENIO", "DATA_DESEMBOLSO", "VL_DESEMBOLSADO"]
contrapartidas_cols = ["NR_CONVENIO", "DT_INGRESSO_CONTRAPARTIDA", "VL_INGRESSO_CONTRAPARTIDA"]
pagamentos_cols = ["NR_MOV_FIN", "NR_CONVENIO", "IDENTIF_FORNECEDOR", "NOME_FORNECEDOR", "DATA_PAG", "VL_PAGO"]

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

def getCurrentDate():
    url = 'http://plataformamaisbrasil.gov.br/download-de-dados'
    response = requests.get(url, stream=True)

    p = response.text.find('dos dados: <strong>﻿')
    dt = response.text[p+20:p+39].strip()
    if datetime_validation(dt) is None:
        raise Exception(f'Invalid datetime: {dt}')
    return datetime_validation(dt)

def feedback(label='', value=''):
    label_length = 30
    value_length = 30
    label = label + ' ' + '-'*label_length
    value = '-'*value_length + ' ' + value
    app_log.info(label[:label_length] + value[-value_length:])

def get_last_date():
    try:
        last_date = pd.read_csv(os.path.join(config.DATA_FOLDER, 'data_atual.txt'), sep=';', dtype=str)
        return datetime_validation(last_date.columns[0])
    except:
        return None

def fetch_data():
    #url = 'http://plataformamaisbrasil.gov.br/images/docs/CGSIS/csv'
    url = 'C:\\Users\\jrans\\Desktop\\git\\siconv\\downloads'

    Path(config.DATA_FOLDER).mkdir(parents=True, exist_ok=True)

    app_log.info('[Getting current date]')

    feedback(label='-> data atual', value='connecting...')    
    
    last_date = get_last_date()
    today = datetime.now()
    today = datetime(today.year, today.month, today.day)
    
    current_date = getCurrentDate()
    current_date_str = current_date.strftime("%d/%m/%Y %H:%M:%S")

    data_atual = pd.DataFrame(data={current_date_str: []})

    feedback(label='-> data atual', value=current_date_str)
    if last_date:
        if  last_date >= today:
            raise UpToDateException('', 'Dados já estão atualizados.')

        if last_date >= current_date:
            raise UnchangedException('', 'Dados inalterados na origem.')


    app_log.info('[Fetching data]')
    
    feedback(label='-> proponentes', value='connecting...')
    proponentes = pd.read_csv(f'{url}/siconv_proponentes.csv.zip', compression='zip', sep=';', dtype=str, usecols=proponentes_cols)
    feedback(label='-> Proponentes', value=f'{len(proponentes)}')

    feedback(label='-> propostas', value='connecting...')
    propostas = pd.read_csv(f'{url}/siconv_proposta.csv.zip', compression='zip', sep=';', dtype=str, usecols=propostas_cols)
    feedback(label='-> Propostas', value=f'{len(propostas)}')

    feedback(label='-> convenios', value='connecting...')
    convenios = pd.read_csv(f'{url}/siconv_convenio.csv.zip', compression='zip', sep=';', dtype=str, usecols=convenios_cols)
    convenios = convenios[convenios['DIA_ASSIN_CONV'].notna()]
    feedback(label='-> convenios', value=f'{len(convenios)}')

    feedback(label='-> emendas', value='connecting...')
    emendas = pd.read_csv(f'{url}/siconv_emenda.csv.zip', compression='zip', sep=';', dtype=str, usecols=emendas_cols)
    feedback(label='-> emendas', value=f'{len(emendas)}')

    feedback(label='-> desembolsos', value='connecting...')
    desembolsos = pd.read_csv(f'{url}/siconv_desembolso.csv.zip', compression='zip', sep=';', dtype=str, usecols=desembolsos_cols)
    feedback(label='-> desembolsos', value=f'{len(desembolsos)}')

    feedback(label='-> contrapartidas', value='connecting...')
    contrapartidas = pd.read_csv(f'{url}/siconv_ingresso_contrapartida.csv.zip', compression='zip', sep=';', dtype=str, usecols=contrapartidas_cols)
    feedback(label='-> contrapartidas', value=f'{len(contrapartidas)}')

    feedback(label='-> pagamentos', value='connecting...')
    pagamentos = pd.read_csv(f'{url}/siconv_pagamento.csv.zip', compression='zip', sep=';', dtype=str, usecols=pagamentos_cols)
    feedback(label='-> pagamentos', value=f'{len(pagamentos)}')

    app_log.info('[Transforming data]')

    feedback(label='-> convenios', value='transforming...')
    propostas_proponentes = pd.merge(propostas, proponentes, how='inner', on='IDENTIF_PROPONENTE', left_index=False, right_index=False)
    convenios = pd.merge(convenios, propostas_proponentes, how='inner', on='ID_PROPOSTA', left_index=False, right_index=False)
    feedback(label='-> convenios', value='Success!')

    feedback(label='-> proponentes', value='transforming...')
    proponentes = convenios.filter(proponentes_final_cols).drop_duplicates()
    convenios = convenios.drop(columns=proponentes_drop).drop_duplicates()
    feedback(label='-> proponentes', value='Success!')

    feedback(label='-> emendas', value='transforming...')
    emendas = emendas[emendas['NR_EMENDA'].notna()]
    emendas_convenios = pd.merge(emendas, convenios, how='inner', on='ID_PROPOSTA', left_index=False, right_index=False)
    emendas = emendas_convenios.filter(emendas_final_cols).drop_duplicates()
    feedback(label='-> emendas', value='Success!')

    feedback(label='-> emendas_convenios', value='transforming...')
    emendas_convenios = emendas_convenios.filter(emendas_convenios_cols).drop_duplicates()
    convenios = convenios.drop(columns=['ID_PROPOSTA']).drop_duplicates()
    feedback(label='-> emendas_convenios', value='Success!')

    feedback(label='-> movimento', value='transforming...')
    convs = convenios['NR_CONVENIO'].unique()
    desembolsos = desembolsos[desembolsos['NR_CONVENIO'].isin(convs) & desembolsos['DATA_DESEMBOLSO'].notna()]
    desembolsos.columns = ['MOV_ID', 'NR_CONVENIO', 'DATA', 'VALOR']
    desembolsos['MOV_ID'] = 'D' + desembolsos['MOV_ID']
    desembolsos['TIPO'] = 'D'
    
    contrapartidas = contrapartidas[contrapartidas['NR_CONVENIO'].isin(convs) & contrapartidas['DT_INGRESSO_CONTRAPARTIDA'].notna()]
    contrapartidas.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
    contrapartidas['MOV_ID'] = 'C' + contrapartidas.index.astype(str)
    contrapartidas['TIPO'] = 'C'

    pagamentos = pagamentos[pagamentos['NR_CONVENIO'].isin(convs) & pagamentos['DATA_PAG'].notna()]
    pagamentos.columns = ['MOV_ID', 'NR_CONVENIO', 'IDENTIF_FORNECEDOR', 'NOME_FORNECEDOR', 'DATA', 'VALOR']
    pagamentos['MOV_ID'] = 'P' + pagamentos['MOV_ID']
    pagamentos['TIPO'] = 'P'

    movimento = pd.concat([desembolsos, contrapartidas, pagamentos], ignore_index=True, sort=False)
    movimento.loc[movimento['IDENTIF_FORNECEDOR'].isna(), 'IDENTIF_FORNECEDOR'] = '#N/D'
    movimento.loc[movimento['NOME_FORNECEDOR'].isna(), 'NOME_FORNECEDOR'] = '#N/D'
    feedback(label='-> movimento', value='Success!')

    return data_atual, proponentes, convenios, emendas, emendas_convenios, movimento

def fix_movimento(movimento):
    fix_list = [
        {'convenio': '774717', 'ref': '24/06/1900', 'valor': '24/06/2014'},
        {'convenio': '756498', 'ref': '03/09/1985', 'valor': '03/09/2012'},
        {'convenio': '704101', 'ref': '30/12/2000', 'valor': '30/12/2009'},
        {'convenio': '703184', 'ref': '13/01/2001', 'valor': '13/01/2010'},
        {'convenio': '731964', 'ref': '19/01/2001', 'valor': '19/01/2011'},
        {'convenio': '702011', 'ref': '14/02/2001', 'valor': '14/02/2011'},
        {'convenio': '726717', 'ref': '21/02/2001', 'valor': '21/02/2011'},
        {'convenio': '720576', 'ref': '07/04/2001', 'valor': '07/04/2010'},
        {'convenio': '721720', 'ref': '01/06/2001', 'valor': '01/06/2011'},
        {'convenio': '752802', 'ref': '19/08/2001', 'valor': '19/08/2011'},
        {'convenio': '702821', 'ref': '01/09/2001', 'valor': '01/09/2011'},
        {'convenio': '751725', 'ref': '18/11/2001', 'valor': '18/11/2011'},
        {'convenio': '733707', 'ref': '31/12/2004', 'valor': '31/12/2010'},
        {'convenio': '716075', 'ref': '13/02/2002', 'valor': '13/02/2012'},
        {'convenio': '703060', 'ref': '14/09/2002', 'valor': '14/09/2012'},
        {'convenio': '723528', 'ref': '16/10/2002', 'valor': '16/10/2012'},
        {'convenio': '719808', 'ref': '05/02/2003', 'valor': '05/02/2013'},
        {'convenio': '702387', 'ref': '31/12/2004', 'valor': '31/12/2010'},
        {'convenio': '772002', 'ref': '30/12/2005', 'valor': '30/12/2014'},
        {'convenio': '717464', 'ref': '22/06/2006', 'valor': '22/06/2010'},
        {'convenio': '704192', 'ref': '24/12/2006', 'valor': '22/06/2010'},
        {'convenio': '732451', 'ref': '17/05/2007', 'valor': '17/05/2010'},
        {'convenio': '701723', 'ref': '31/05/2007', 'valor': '31/05/2011'},
        {'convenio': '715596', 'ref': '16/06/2007', 'valor': '16/06/2010'},
        {'convenio': '705156', 'ref': '06/11/2007', 'valor': '06/11/2009'}
        ]

    app_log.info('[Fixing movimento]')

    for fix in fix_list:
        movimento.loc[(movimento['NR_CONVENIO']==fix['convenio']) & (movimento['DATA'] == fix['ref']), 'DATA'] = fix['valor']
        feedback(label=f"-> {fix['convenio']}", value=f"{fix['ref']} >>> {fix['valor']}")

    return movimento

def update_csv():
    try:
        #fetch data
        data_atual, proponentes, convenios, emendas, emendas_convenios, movimento = fetch_data()
        
        #fix data
        movimento = fix_movimento(movimento)
        
        app_log.info('[Updating csv files]')
       
        feedback(label='-> proponentes', value='updating...')
        proponentes.to_csv(os.path.join(config.DATA_FOLDER, f'proponentes{config.FILE_EXTENTION}'), compression=config.COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> proponentes', value='Success!')

        feedback(label='-> convenios', value='updating...')
        convenios.to_csv(os.path.join(config.DATA_FOLDER, f'convenios{config.FILE_EXTENTION}'), compression=config.COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> convenios', value='Success!')
        
        feedback(label='-> emendas', value='updating...')
        emendas.to_csv(os.path.join(config.DATA_FOLDER, f'emendas{config.FILE_EXTENTION}'), compression=config.COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> emendas', value='Success!')

        feedback(label='-> emendas_convenios', value='updating...')
        emendas_convenios.to_csv(os.path.join(config.DATA_FOLDER, f'emendas_convenios{config.FILE_EXTENTION}'), compression=config.COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> emendas_convenios', value='Success!')

        feedback(label='-> movimento', value='updating...')
        movimento.to_csv(os.path.join(config.DATA_FOLDER, f'movimento{config.FILE_EXTENTION}'), compression=config.COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> movimento', value='Success!')

        feedback(label='-> data atual', value='updating...')
        data_atual.to_csv(os.path.join(config.DATA_FOLDER, 'data_atual.txt'), encoding='utf-8', index=False)
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

def update_database():
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

        app_log.info('[Updating Database]')

        feedback(label='-> proponentes', value='updating...')
        proponentes = read_data(tbl_name='proponentes')
        proponentes.to_sql('proponentes', con=engine, if_exists='replace', index=False, chunksize=chunksize)
        feedback(label='-> proponentes', value='Success!')

        feedback(label='-> convenios', value='updating...')
        convenios = read_data(tbl_name='convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
        convenios.to_sql('convenios', con=engine, if_exists='replace', index=False, chunksize=chunksize)
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
        emendas.to_sql('emendas', con=engine, if_exists='replace', index=False, chunksize=chunksize)
        feedback(label='-> emendas', value='Success!')

        feedback(label='-> emendas_convenios', value='updating...')
        emendas_convenios = read_data(tbl_name='emendas_convenios', dtypes=dtypes_emendas_convenios)
        emendas_convenios.to_sql('convenios_emendas_association', con=engine, if_exists='replace', index=False, chunksize=chunksize)
        feedback(label='-> emendas_convenios', value='Success!')

        feedback(label='-> movimento', value='updating...')
        movimento = read_data(tbl_name='movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)
        movimento.to_sql('movimento', con=engine, if_exists='replace', index=False, chunksize=chunksize)
        feedback(label='-> movimento', value='Success!')

        feedback(label='-> municipios', value='updating...')
        municipios = read_data(tbl_name='municipios', dtypes=dtypes_municipios, decimal='.')
        municipios.to_sql('municipios', con=engine, if_exists='replace', index=False, chunksize=chunksize)
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
    from apscheduler.schedulers.blocking import BlockingScheduler
    from dotenv import load_dotenv
    from sqlalchemy.ext.declarative import declarative_base
    import sqlalchemy as sa

    env_path = '/home/siconvdata/.env'
    if Path(env_path).is_file():
        load_dotenv(dotenv_path=env_path, override=True)
    else:
        load_dotenv(dotenv_path='.env', override=True)

    from app.config import Config

    config = Config()

    metadata = sa.MetaData()
    Base = declarative_base(metadata=metadata)
    engine = sa.create_engine(Config.SQLALCHEMY_DATABASE_URI)
    

    chunksize = 100000

    '''
    sched = BlockingScheduler()

    download_ok = False
    database_ok = False

    @sched.scheduled_job('cron', day_of_week='*', hour='8/1', minute='*/15', max_instances=1)
    def update_job():
        global download_ok
        global database_ok

        if not download_ok:
            download_ok = update_csv()

        if not database_ok:
            database_ok = update_database()
        
        if (download_ok and database_ok) or datetime.utcnow().hour >= 21:
            sched.shutdown(wait=False)

    sched.start()
    '''
    update_csv()
    update_database()

