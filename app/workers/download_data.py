# -*- coding: utf-8 -*-

import pandas as pd
import requests
import os
from datetime import date, time
from app.logger import app_log
from app.config import DATA_FOLDER, COMPRESSION_METHOD, FILE_EXTENTION


#columns definitions
proponentes_cols = ["IDENTIF_PROPONENTE", "NM_PROPONENTE"]
proponentes_final_cols = ["IDENTIF_PROPONENTE", "NM_PROPONENTE", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE"]
proponentes_drop = ["NM_PROPONENTE", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE"]

propostas_cols = ["ID_PROPOSTA", "UF_PROPONENTE", "MUNIC_PROPONENTE", "COD_MUNIC_IBGE", "COD_ORGAO_SUP", "DESC_ORGAO_SUP", "NATUREZA_JURIDICA", "COD_ORGAO", "DESC_ORGAO", "MODALIDADE", "IDENTIF_PROPONENTE", "OBJETO_PROPOSTA"]
convenios_cols = ["NR_CONVENIO", "ID_PROPOSTA", "DIA_ASSIN_CONV", "SIT_CONVENIO", "INSTRUMENTO_ATIVO", "DIA_PUBL_CONV", "DIA_INIC_VIGENC_CONV", "DIA_FIM_VIGENC_CONV", "DIA_LIMITE_PREST_CONTAS", "VL_GLOBAL_CONV", "VL_REPASSE_CONV", "VL_CONTRAPARTIDA_CONV"]

emendas_cols = ['ID_PROPOSTA', 'NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR', 'VALOR_REPASSE_EMENDA']
emendas_drop_cols = ['ID_PROPOSTA', 'NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR', 'VALOR_REPASSE_EMENDA']
emendas_final_cols = ['NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR']
emendas_convenios_cols = ['NR_EMENDA', 'NR_CONVENIO', 'VALOR_REPASSE_EMENDA']

desembolsos_cols = ["NR_CONVENIO", "DATA_DESEMBOLSO", "VL_DESEMBOLSADO"]
contrapartidas_cols = ["NR_CONVENIO", "DT_INGRESSO_CONTRAPARTIDA", "VL_INGRESSO_CONTRAPARTIDA"]
pagamentos_cols = ["NR_CONVENIO", "IDENTIF_FORNECEDOR", "NOME_FORNECEDOR", "DATA_PAG", "VL_PAGO"]

def datetime_validation(txt):
    try:
        dtm = txt.split(' ')
        dt = dtm[0].split('/')
        tm = dtm[1].split(':')
        if len(dtm) != 2 or len(dt) != 3 or len(tm) != 3:
            raise
        date(int(dt[2]), int(dt[1]), int(dt[0]))
        time(int(tm[0]), int(tm[1]), int(tm[2]))
    except:
        return False

    return True

def get_df(df_chunk, label=''):
    df_list = []
    rows = 0
    for chunk in df_chunk:
        df_list += [chunk]
        rows += len(chunk)

    return pd.concat(df_list)

def getCurrentDate():
    url = 'http://plataformamaisbrasil.gov.br/download-de-dados'
    response = requests.get(url, stream=True)

    p = response.text.find('dos dados: <strong>﻿')
    dt = response.text[p+20:p+39].strip()
    if not datetime_validation(dt):
        raise Exception(f'Invalid datetime: {dt}')
    return dt

def feedback(label='', value=''):
    label_length = 30
    value_length = 30
    label = label + ' ' + '-'*label_length
    value = '-'*value_length + ' ' + value
    app_log.info(label[:label_length] + value[-value_length:])

def fetch_data():
    url = 'http://plataformamaisbrasil.gov.br/images/docs/CGSIS/csv'
    chunk_nrows = 1000
    
    app_log.info('[Getting current date]')

    feedback(label='-> data atual', value='connecting...')
    current_date = getCurrentDate()
    df_date = pd.DataFrame(data={current_date: []})
    feedback(label='-> data atual', value=current_date)

    app_log.info('[Fetching data]')
    
    feedback(label='-> proponentes', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_proponentes.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=proponentes_cols)
    proponentes = get_df(df_chunk, '-> Proponentes')
    feedback(label='-> Proponentes', value=f'{len(proponentes)}')

    feedback(label='-> propostas', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_proposta.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=propostas_cols)
    propostas = get_df(df_chunk, '-> Propostas')
    feedback(label='-> Propostas', value=f'{len(propostas)}')

    feedback(label='-> convenios', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_convenio.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=convenios_cols)
    convenios = get_df(df_chunk, '-> convenios')
    convenios = convenios[convenios['DIA_ASSIN_CONV'].notna()]
    feedback(label='-> convenios', value=f'{len(convenios)}')

    feedback(label='-> emendas', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_emenda.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=emendas_cols)
    emendas = get_df(df_chunk, '-> emendas')
    feedback(label='-> emendas', value=f'{len(emendas)}')

    feedback(label='-> desembolsos', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_desembolso.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=desembolsos_cols)
    desembolsos = get_df(df_chunk, '-> desembolsos')
    feedback(label='-> desembolsos', value=f'{len(desembolsos)}')

    feedback(label='-> contrapartidas', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_ingresso_contrapartida.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=contrapartidas_cols)
    contrapartidas = get_df(df_chunk, '-> contrapartidas')
    feedback(label='-> contrapartidas', value=f'{len(contrapartidas)}')

    feedback(label='-> pagamentos', value='connecting...')
    df_chunk = pd.read_csv(f'{url}/siconv_pagamento.csv.zip', compression='zip', chunksize=chunk_nrows, sep=';', dtype=str, usecols=pagamentos_cols)
    pagamentos = get_df(df_chunk, '-> pagamentos')
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
    desembolsos.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
    desembolsos['TIPO'] = 'D'
    contrapartidas = contrapartidas[contrapartidas['NR_CONVENIO'].isin(convs) & contrapartidas['DT_INGRESSO_CONTRAPARTIDA'].notna()]
    contrapartidas.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
    contrapartidas['TIPO'] = 'C'
    pagamentos = pagamentos[pagamentos['NR_CONVENIO'].isin(convs) & pagamentos['DATA_PAG'].notna()]
    pagamentos.columns = ['NR_CONVENIO', 'IDENTIF_FORNECEDOR', 'NOME_FORNECEDOR', 'DATA', 'VALOR']
    pagamentos.loc[pagamentos['IDENTIF_FORNECEDOR'].isna(), 'IDENTIF_FORNECEDOR'] = '#N/D'
    pagamentos.loc[pagamentos['NOME_FORNECEDOR'].isna(), 'NOME_FORNECEDOR'] = '#N/D'
    pagamentos['TIPO'] = 'P'
    movimento = pd.concat([desembolsos, contrapartidas, pagamentos], ignore_index=True, sort=False)
    feedback(label='-> movimento', value='Success!')

    return df_date, proponentes, convenios, emendas, emendas_convenios, movimento

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

def update():
    try:
        #fetching
        df_date, proponentes, convenios, emendas, emendas_convenios, movimento = fetch_data()
        movimento = fix_movimento(movimento)
        

        app_log.info('[Updating]')

        feedback(label='-> data atual', value='updating...')
        df_date.to_csv(os.path.join(DATA_FOLDER, 'data_atual.txt'), encoding='utf-8', index=False)
        feedback(label='-> data atual', value='Success!')
        
        feedback(label='-> proponentes', value='updating...')
        proponentes.to_csv(os.path.join(DATA_FOLDER, f'proponentes{FILE_EXTENTION}'), compression=COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> proponentes', value='Success!')

        feedback(label='-> convenios', value='updating...')
        convenios.to_csv(os.path.join(DATA_FOLDER, f'convenios{FILE_EXTENTION}'), compression=COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> convenios', value='Success!')
        
        feedback(label='-> emendas', value='updating...')
        emendas.to_csv(os.path.join(DATA_FOLDER, f'emendas{FILE_EXTENTION}'), compression=COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> emendas', value='Success!')

        feedback(label='-> emendas_convenios', value='updating...')
        emendas_convenios.to_csv(os.path.join(DATA_FOLDER, f'emendas_convenios{FILE_EXTENTION}'), compression=COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> emendas_convenios', value='Success!')

        feedback(label='-> movimento', value='updating...')
        movimento.to_csv(os.path.join(DATA_FOLDER, f'movimento{FILE_EXTENTION}'), compression=COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)
        feedback(label='-> movimento', value='Success!')

        app_log.info('Processo finalizado com sucesso!')

    except Exception as e:
        app_log.info(repr(e))
        app_log.info('Processo falhou!')

#update()