# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path
from datetime import datetime
import os
from .data_files_tools import FileTools
from .utils import *

class Extraction(object):
    def __init__(self, config, logger) -> None:
        self.config = config
        self.logger = logger
        #self.file_tools = CSVTools(config=config)
        self.file_tools = FileTools(config=config)

    def __already_extracted__(self, table_name, current_date, date_verification=True):
        result = False
        file = Path(os.path.join(self.config.STAGE_FOLDER, f'{table_name}{self.config.FILE_EXTENTION}'))
        if file.exists():
            if date_verification:
                creation_date = datetime.fromtimestamp(file.stat().st_mtime).date()
                result = True if creation_date >= current_date else False
            else:
                result = True

        #return file.exists() 
        return result

    def extract(self, current_date, force_download=False):
        
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
        if force_download or not self.__already_extracted__(table_name='estados', current_date=current_date, date_verification=False):
            estados = pd.read_csv(f'{self.config.MUNICIPIOS_BACKUP_FOLDER}/estados.csv.gz', 
                                  compression='gzip', sep=',', dtype=str,
                                  usecols=estados_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=estados, table_name='estados')
        else:
            estados = self.file_tools.read_from_stage(tbl_name='estados')
        feedback(self.logger, label='-> estados', value=f'{len(estados)}')


        feedback(self.logger, label='-> municipios', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='municipios', current_date=current_date, date_verification=False):
            municipios = pd.read_csv(f'{self.config.MUNICIPIOS_BACKUP_FOLDER}/municipios.csv.gz', 
                                     compression='gzip', sep=',', dtype=str,
                                     usecols=municipios_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=municipios, table_name='municipios')
        else:
            municipios = self.file_tools.read_from_stage(tbl_name='municipios')
        feedback(self.logger, label='-> municipios', value=f'{len(municipios)}')
        

        feedback(self.logger, label='-> proponentes', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='proponentes', current_date=current_date):
            proponentes = pd.read_csv(f'{url}/siconv_proponentes.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=proponentes_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=proponentes, table_name='proponentes')
        else:
            proponentes = self.file_tools.read_from_stage(tbl_name='proponentes')
        feedback(self.logger, label='-> Proponentes', value=f'{len(proponentes)}')


        feedback(self.logger, label='-> propostas', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='propostas', current_date=current_date):
            propostas = pd.read_csv(f'{url}/siconv_proposta.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=propostas_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=propostas, table_name='propostas')
        else:
            propostas = self.file_tools.read_from_stage(tbl_name='propostas')
        feedback(self.logger, label='-> propostas', value=f'{len(propostas)}')


        feedback(self.logger, label='-> convenios', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='convenios', current_date=current_date):
            convenios = pd.read_csv(f'{url}/siconv_convenio.csv.zip', compression='zip', sep=';', dtype=str, usecols=convenios_extract_cols)
            convenios = convenios[(convenios['DIA_ASSIN_CONV'].notna()) & (convenios['DIA_PUBL_CONV'].notna())]
            convenios.loc[convenios['INSTRUMENTO_ATIVO'].str.upper()=='NÃO', ['INSTRUMENTO_ATIVO']] = 'NAO'
            convenios = convenios.drop_duplicates()
            self.file_tools.write_to_stage(table=convenios, table_name='convenios')
        else:
            convenios = self.file_tools.read_from_stage(tbl_name='convenios')
        feedback(self.logger, label='-> convenios', value=f'{len(convenios)}')


        feedback(self.logger, label='-> emendas', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='emendas', current_date=current_date):
            emendas = pd.read_csv(f'{url}/siconv_emenda.csv.zip', 
                                compression='zip', sep=';', dtype=str, 
                                usecols=emendas_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=emendas, table_name='emendas')
        else:
            emendas = self.file_tools.read_from_stage(tbl_name='emendas')
        feedback(self.logger, label='-> emendas', value=f'{len(emendas)}')


        feedback(self.logger, label='-> desembolsos', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='desembolsos', current_date=current_date):
            desembolsos = pd.read_csv(f'{url}/siconv_desembolso.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=desembolsos_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=desembolsos, table_name='desembolsos')
        else:
            desembolsos = self.file_tools.read_from_stage(tbl_name='desembolsos')
        feedback(self.logger, label='-> desembolsos', value=f'{len(desembolsos)}')


        feedback(self.logger, label='-> contrapartidas', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='contrapartidas', current_date=current_date):
            contrapartidas = pd.read_csv(f'{url}/siconv_ingresso_contrapartida.csv.zip', 
                                        compression='zip', sep=';', dtype=str,
                                        usecols=contrapartidas_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=contrapartidas, table_name='contrapartidas')
        else:
            contrapartidas = self.file_tools.read_from_stage(tbl_name='contrapartidas')
        feedback(self.logger, label='-> contrapartidas', value=f'{len(contrapartidas)}')


        feedback(self.logger, label='-> tributos', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='tributos', current_date=current_date):
            tributos = pd.read_csv(f'{url}/siconv_pagamento_tributo.csv.zip', 
                                        compression='zip', sep=';', dtype=str,
                                        usecols=tributos_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=tributos, table_name='tributos')
        else:
            tributos = self.file_tools.read_from_stage(tbl_name='tributos')
        feedback(self.logger, label='-> tributos', value=f'{len(tributos)}')


        feedback(self.logger, label='-> pagamentos', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='pagamentos', current_date=current_date):
            pagamentos = pd.read_csv(f'{url}/siconv_pagamento.csv.zip', 
                                    compression='zip', sep=';', dtype=str, 
                                    usecols=pagamentos_extract_cols).drop_duplicates()
            self.file_tools.write_to_stage(table=pagamentos, table_name='pagamentos')
        else:
            pagamentos = self.file_tools.read_from_stage(tbl_name='pagamentos')
        feedback(self.logger, label='-> pagamentos', value=f'{len(pagamentos)}')


        feedback(self.logger, label='-> OBTV', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='obtv', current_date=current_date):
            obtv = pd.read_csv(f'{url}/siconv_obtv_convenente.csv.zip', compression='zip', sep=';', dtype=str, usecols=obtv_extract_cols)
            obtv = obtv[obtv['IDENTIF_FAVORECIDO_OBTV_CONV'].notna()]
            obtv = obtv.drop_duplicates()
            self.file_tools.write_to_stage(table=obtv, table_name='obtv')
        else:
            obtv = self.file_tools.read_from_stage(tbl_name='obtv')
        feedback(self.logger, label='-> OBTV', value=f'{len(pagamentos)}')


        feedback(self.logger, label='-> Licitações', value='connecting...')
        if force_download or not self.__already_extracted__(table_name='licitacoes', current_date=current_date):
            licitacoes = pd.read_csv(f'{url}/siconv_licitacao.csv.zip', compression='zip', sep=';', dtype=str, usecols=licitacoes_extract_cols)
            self.file_tools.write_to_stage(table=licitacoes, table_name='licitacoes')
        else:
            licitacoes = self.file_tools.read_from_stage(tbl_name='licitacoes')
        feedback(self.logger, label='-> Licitações', value=f'{len(licitacoes)}')


        return estados, municipios, proponentes, propostas, convenios, emendas, desembolsos, \
               contrapartidas, tributos, pagamentos, obtv, licitacoes
            