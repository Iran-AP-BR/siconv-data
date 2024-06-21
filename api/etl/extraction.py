# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
from datetime import datetime
import os
from .data_files_tools import FileTools
from .utils import *
import gc


class Extraction(object):
    def __init__(self, config, logger, current_date) -> None:
        self.config = config
        self.logger = logger
        self.current_date = current_date
        self.file_tools = FileTools(config=config)


    def __already_extracted__(self, table_name, date_verification=True):
        result = False
        file = Path(os.path.join(self.config.STAGE_FOLDER, f'{table_name}{self.config.FILE_EXTENTION}'))
        if file.exists():
            if date_verification:
                creation_date = datetime.fromtimestamp(file.stat().st_mtime).date()
                result = True if creation_date >= self.current_date else False
            else:
                result = True

        return result

    def __get_data__(self, table_name, date_verification=True, force_download=False,
                           compression=None, sep=';', remote_path=None, usecols=None):
        table = None
        if not force_download and self.__already_extracted__(table_name=table_name,
                                        date_verification=date_verification):
            try:
                table = self.file_tools.read_from_stage(tbl_name=table_name)
            except:
                table = None

        if table is None:

            table = pd.read_csv(remote_path, compression=compression, sep=sep,
                    dtype=str, usecols=usecols, skiprows=None, low_memory=True)

            '''
            chunksize = 100000

            for chunk in pd.read_csv(remote_path, compression=compression, sep=sep, dtype=str, usecols=usecols, chunksize=chunksize):
                chunk = chunk.drop_duplicates()
                if table is not None:
                    table = pd.concat([table, chunk]).drop_duplicates()
                else:
                    table = chunk.copy()
            '''
        return table

    '''
    def __get_data2__(self, table_name, date_verification=True, force_download=False,
                           compression=None, sep=';', remote_path=None, usecols=None):
        table = None
        if not force_download and self.__already_extracted__(table_name=table_name,
                                                             date_verification=date_verification):
            try:
                table = self.file_tools.read_from_stage(tbl_name=table_name)
            except:
                table = None

        if table is None:
            chunksize = 100000
            chunks = []

            for chunk in pd.read_csv(remote_path, compression=compression, sep=sep, dtype=str, usecols=usecols, chunksize=chunksize):
                chunk = chunk.drop_duplicates()
                chunks.append(chunk)

            table = pd.concat(chunks).drop_duplicates()

        return table
    '''

    '''
    def __get_data1__(self, table_name, date_verification=True, force_download=False,
                           compression=None, sep=';', remote_path=None, usecols=None):
        table = None
        if not force_download and self.__already_extracted__(table_name=table_name,
                                                             date_verification=date_verification):
            try:
                table = self.file_tools.read_from_stage(tbl_name=table_name)
            except:
                table = None

        if table is None:
            table = pd.read_csv(remote_path, compression=compression, sep=sep, dtype=str,
                                usecols=usecols).drop_duplicates()

        return table

    '''

    def extract(self, force_download=False):

        self.logger.info('[Extracting data]')

        estados_extract_cols = ["codigo_uf", "uf", "nome", "regiao"]
        municipios_extract_cols = ["codigo_ibge", "nome", "latitude", "longitude", "capital", "codigo_uf"]

        proponentes_extract_cols = ["IDENTIF_PROPONENTE", "NM_PROPONENTE"]
        propostas_extract_cols = ["ID_PROPOSTA", "COD_MUNIC_IBGE", "COD_ORGAO_SUP", "DESC_ORGAO_SUP",
                                "NATUREZA_JURIDICA", "COD_ORGAO", "DESC_ORGAO", "MODALIDADE",
                                "IDENTIF_PROPONENTE", "OBJETO_PROPOSTA", 'ANO_PROP', 'UF_PROPONENTE', 'MUNIC_PROPONENTE']

        convenios_extract_cols = ["NR_CONVENIO", "ID_PROPOSTA", "DIA_ASSIN_CONV", "SIT_CONVENIO", "INSTRUMENTO_ATIVO",
                                "DIA_PUBL_CONV", "DIA_INIC_VIGENC_CONV", "DIA_FIM_VIGENC_CONV",
                                "DIA_LIMITE_PREST_CONTAS", "VL_GLOBAL_CONV", "VL_REPASSE_CONV", "VL_CONTRAPARTIDA_CONV"]

        emendas_extract_cols = ['ID_PROPOSTA', 'NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR', 'VALOR_REPASSE_EMENDA']

        desembolsos_extract_cols = ["ID_DESEMBOLSO", "NR_CONVENIO", "DATA_DESEMBOLSO", "VL_DESEMBOLSADO"]
        contrapartidas_extract_cols = ["NR_CONVENIO", "DT_INGRESSO_CONTRAPARTIDA", "VL_INGRESSO_CONTRAPARTIDA"]
        pagamentos_extract_cols = ["NR_MOV_FIN", "NR_CONVENIO", "IDENTIF_FORNECEDOR", "NOME_FORNECEDOR", "DATA_PAG", "VL_PAGO"]

        tributos_extract_cols = ["NR_CONVENIO", "DATA_TRIBUTO", "VL_PAG_TRIBUTOS"]

        obtv_extract_cols = ["NR_MOV_FIN", "IDENTIF_FAVORECIDO_OBTV_CONV", "NM_FAVORECIDO_OBTV_CONV", "TP_AQUISICAO", "VL_PAGO_OBTV_CONV"]

        licitacoes_extract_cols=['ID_LICITACAO', 'NR_CONVENIO', 'MODALIDADE_LICITACAO', 'TP_PROCESSO_COMPRA',
                                 'TIPO_LICITACAO', 'STATUS_LICITACAO', 'VALOR_LICITACAO']

        url = self.config.DOWNLOAD_URI

        feedback(self.logger, label='-> estados', value='connecting...')
        estados = self.__get_data__(table_name='estados',
                                      force_download=force_download,
                                      compression='gzip',
                                      sep=',',
                                      remote_path=f'{self.config.MUNICIPIOS_BACKUP_FOLDER}/estados.csv.gz',
                                      usecols=estados_extract_cols)

        self.file_tools.write_to_stage(table=estados, table_name='estados', current_date=self.current_date)
        feedback(self.logger, label='-> estados', value=f'{len(estados)}')
        del estados
        gc.collect()


        feedback(self.logger, label='-> municipios', value='connecting...')
        municipios = self.__get_data__(table_name='municipios',
                                      force_download=force_download,
                                      compression='gzip',
                                      sep=',',
                                      remote_path=f'{self.config.MUNICIPIOS_BACKUP_FOLDER}/municipios.csv.gz',
                                      usecols=municipios_extract_cols)

        self.file_tools.write_to_stage(table=municipios, table_name='municipios', current_date=self.current_date)
        feedback(self.logger, label='-> municipios', value=f'{len(municipios)}')
        del municipios
        gc.collect()


        feedback(self.logger, label='-> proponentes', value='connecting...')
        proponentes = self.__get_data__(table_name='proponentes',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_proponentes.csv.zip',
                                      usecols=proponentes_extract_cols)

        self.file_tools.write_to_stage(table=proponentes, table_name='proponentes', current_date=self.current_date)
        feedback(self.logger, label='-> proponentes', value=f'{len(proponentes)}')
        del proponentes
        gc.collect()


        feedback(self.logger, label='-> propostas', value='connecting...')
        propostas = self.__get_data__(table_name='propostas',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_proposta.csv.zip',
                                      usecols=propostas_extract_cols)

        self.file_tools.write_to_stage(table=propostas, table_name='propostas', current_date=self.current_date)
        feedback(self.logger, label='-> propostas', value=f'{len(propostas)}')
        del propostas
        gc.collect()


        feedback(self.logger, label='-> convenios', value='connecting...')
        convenios = self.__get_data__(table_name='convenios',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_convenio.csv.zip',
                                      usecols=convenios_extract_cols)

        convenios = convenios[(convenios['DIA_ASSIN_CONV'].notna()) & (convenios['DIA_PUBL_CONV'].notna())]
        convenios.loc[convenios['INSTRUMENTO_ATIVO'].str.upper()=='NÃO', ['INSTRUMENTO_ATIVO']] = 'NAO'
        convenios = convenios.drop_duplicates()

        self.file_tools.write_to_stage(table=convenios, table_name='convenios', current_date=self.current_date)
        feedback(self.logger, label='-> convenios', value=f'{len(convenios)}')
        del convenios
        gc.collect()


        feedback(self.logger, label='-> emendas', value='connecting...')
        emendas = self.__get_data__(table_name='emendas',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_emenda.csv.zip',
                                      usecols=emendas_extract_cols)

        self.file_tools.write_to_stage(table=emendas, table_name='emendas', current_date=self.current_date)
        feedback(self.logger, label='-> emendas', value=f'{len(emendas)}')
        del emendas
        gc.collect()


        feedback(self.logger, label='-> desembolsos', value='connecting...')
        desembolsos = self.__get_data__(table_name='desembolsos',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_desembolso.csv.zip',
                                      usecols=desembolsos_extract_cols)

        self.file_tools.write_to_stage(table=desembolsos, table_name='desembolsos', current_date=self.current_date)
        feedback(self.logger, label='-> desembolsos', value=f'{len(desembolsos)}')
        del desembolsos
        gc.collect()


        feedback(self.logger, label='-> contrapartidas', value='connecting...')
        contrapartidas = self.__get_data__(table_name='contrapartidas',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_ingresso_contrapartida.csv.zip',
                                      usecols=contrapartidas_extract_cols)

        self.file_tools.write_to_stage(table=contrapartidas, table_name='contrapartidas', current_date=self.current_date)
        feedback(self.logger, label='-> contrapartidas', value=f'{len(contrapartidas)}')
        del contrapartidas
        gc.collect()


        feedback(self.logger, label='-> tributos', value='connecting...')
        tributos = self.__get_data__(table_name='tributos',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_pagamento_tributo.csv.zip',
                                      usecols=tributos_extract_cols)

        self.file_tools.write_to_stage(table=tributos, table_name='tributos', current_date=self.current_date)
        feedback(self.logger, label='-> tributos', value=f'{len(tributos)}')
        del tributos
        gc.collect()


        feedback(self.logger, label='-> pagamentos', value='connecting...')
        pagamentos = self.__get_data__(table_name='pagamentos',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_pagamento.csv.zip',
                                      usecols=pagamentos_extract_cols)

        self.file_tools.write_to_stage(table=pagamentos, table_name='pagamentos', current_date=self.current_date)
        feedback(self.logger, label='-> pagamentos', value=f'{len(pagamentos)}')
        del pagamentos
        gc.collect()


        feedback(self.logger, label='-> OBTV', value='connecting...')
        obtv = self.__get_data__(table_name='obtv',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_obtv_convenente.csv.zip',
                                      usecols=obtv_extract_cols)

        obtv = obtv[obtv['IDENTIF_FAVORECIDO_OBTV_CONV'].notna()]
        obtv = obtv.drop_duplicates()

        self.file_tools.write_to_stage(table=obtv, table_name='obtv', current_date=self.current_date)
        feedback(self.logger, label='-> OBTV', value=f'{len(obtv)}')
        del obtv
        gc.collect()


        feedback(self.logger, label='-> licitações', value='connecting...')
        licitacoes = self.__get_data__(table_name='licitacoes',
                                      force_download=force_download,
                                      compression='zip',
                                      remote_path=f'{url}/siconv_licitacao.csv.zip',
                                      usecols=licitacoes_extract_cols)

        self.file_tools.write_to_stage(table=licitacoes, table_name='licitacoes', current_date=self.current_date)
        feedback(self.logger, label='-> licitações', value=f'{len(licitacoes)}')
        del licitacoes
        gc.collect()

        return None

        #return estados, municipios, proponentes, propostas, convenios, emendas, desembolsos, \
        #       contrapartidas, tributos, pagamentos, obtv, licitacoes
