# -*- coding: utf-8 -*-

import pandas as pd
from .csv_types import *
from .utils import *
from .risk_analyzer import RiskAnalyzer


class Transformation(object):
    def __init__(self, logger) -> None:
        self.logger = logger

    def transform(self, estados, municipios, proponentes, propostas, convenios, emendas, desembolsos, 
                        contrapartidas, tributos, pagamentos, obtv, licitacoes, current_date):

        self.logger.info('[Transforming data]')

        municipios = self.__transform_municipios__(municipios, estados, propostas)
        proponentes, convenios = self.__transform_proponentes__(proponentes, propostas, convenios)
        emendas_convenios, emendas = self.__tranform_emendas_convenios__(emendas, convenios, propostas)
        emendas = self.__tansform_emendas__(emendas)
        convenios = self.__transform_convenios__(convenios, emendas_convenios)
        licitacoes = self.__transform_licitacoes__(licitacoes, convenios)
        fornecedores, pagamentos = self.__transform_fornecedores__(pagamentos, convenios, obtv)
        movimento = self.__transform_movimento__(pagamentos, desembolsos, contrapartidas,
                                                 convenios, fornecedores, tributos)
        convenios = self.__risk_analyzer__(convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento)
        calendario = self.__transform_calendario__(movimento, current_date)
        data_atual = self.__transform_data_atual__(current_date)

        return municipios, proponentes, convenios, emendas, emendas_convenios, movimento, \
               fornecedores, calendario, licitacoes, data_atual
        

    def __transform_municipios__(self, municipios, estados, propostas):
        feedback(self.logger, label='-> municipios', value='transforming...')
        
        estados.rename(str.upper, axis='columns', inplace=True)
        municipios.rename(str.upper, axis='columns', inplace=True)
        
        municipios = pd.merge(municipios, estados, how='inner', on='CODIGO_UF', left_index=False, right_index=False)
        municipios.rename(columns={'NOME_x': 'NOME_MUNICIPIO',
                                   'NOME_y': 'NOME_ESTADO'}, inplace=True)
        municipios = municipios.drop(['CODIGO_UF'], axis=1)
        municipios = municipios.apply(lambda x: x.astype(str).str.upper())

        municipios_list = municipios['CODIGO_IBGE'].unique()
        municipios_extra = propostas.loc[~propostas['COD_MUNIC_IBGE'].isin(municipios_list), :].copy()
        municipios_extra.rename(columns={'COD_MUNIC_IBGE': 'CODIGO_IBGE', 
                                        'MUNIC_PROPONENTE': 'NOME_MUNICIPIO', 
                                        'UF_PROPONENTE': 'UF'}, inplace=True)

        municipios_extra.loc[municipios_extra['UF'].isin(['AP', 'PA', 'RR', 'AM', 'RO', 'TO', 'AC']), 'REGIAO'] = 'NORTE'
        municipios_extra.loc[municipios_extra['UF'].isin(['MA', 'SE', 'PI', 'PE', 'RN', 'PB', 'BA', 'CE', 'AL']), 'REGIAO'] = 'NORDESTE'
        municipios_extra.loc[municipios_extra['UF'].isin(['GO', 'MT', 'MS', 'DF']), 'REGIAO'] = 'CENTRO-OESTE'
        municipios_extra.loc[municipios_extra['UF'].isin(['SP', 'RJ', 'ES', 'MG']), 'REGIAO'] = 'SUDESTE'
        municipios_extra.loc[municipios_extra['UF'].isin(['RS', 'PR', 'SC']), 'REGIAO'] = 'SUL'
    
        municipios_extra['CAPITAL'] = False

        uf_nome_estado = municipios[['UF', 'NOME_ESTADO']].drop_duplicates()
        municipios_extra = pd.merge(municipios_extra, uf_nome_estado, how='left', on='UF', left_index=False, right_index=False)
        municipios_extra = municipios_extra.filter(['CODIGO_IBGE', 'NOME_MUNICIPIO', 'UF', 'NOME_ESTADO', 'REGIAO', 'REGIAO_ABREVIADA', 'CAPITAL'])
        
        municipios.loc[municipios['CAPITAL']=='1', 'CAPITAL'] = True
        municipios.loc[municipios['CAPITAL']=='0', 'CAPITAL'] = False
        municipios = pd.concat([municipios, municipios_extra], ignore_index=True, sort=False)
        municipios = municipios.drop_duplicates()
        municipios.loc[municipios['NOME_ESTADO'].isna(), 'NOME_ESTADO'] = '#(NÃO ESPECIFICADO)'
        
        municipios['LATITUDE'] = municipios['LATITUDE'].str.replace(',', '.', regex=False)
        municipios['LONGITUDE'] = municipios['LONGITUDE'].str.replace(',', '.', regex=False)

        municipios.loc[municipios['REGIAO'] == 'NORTE', 'REGIAO_ABREVIADA'] = 'NO'
        municipios.loc[municipios['REGIAO'] == 'NORDESTE', 'REGIAO_ABREVIADA'] = 'NE'
        municipios.loc[municipios['REGIAO'] == 'CENTRO-OESTE', 'REGIAO_ABREVIADA'] = 'CO'
        municipios.loc[municipios['REGIAO'] == 'SUDESTE', 'REGIAO_ABREVIADA'] = 'SE'
        municipios.loc[municipios['REGIAO'] == 'SUL', 'REGIAO_ABREVIADA'] = 'SL'

        municipios = set_types(municipios, csv_municipios_type)
        
        feedback(self.logger, label='-> municipios', value=f'{len(municipios)} linhas')
        
        return municipios

    def __transform_proponentes__(self, proponentes, propostas, convenios):
        feedback(self.logger, label='-> proponentes', value='transforming...')
        propostas_proponentes = pd.merge(propostas, proponentes, how='inner', on='IDENTIF_PROPONENTE', 
                                        left_index=False, right_index=False)
        convenios = pd.merge(convenios, propostas_proponentes, how='inner', on='ID_PROPOSTA', 
                            left_index=False, right_index=False)
        proponentes = convenios.filter(["IDENTIF_PROPONENTE", "NM_PROPONENTE", "COD_MUNIC_IBGE"]).drop_duplicates()
        proponentes.rename(columns={'COD_MUNIC_IBGE': 'CODIGO_IBGE'}, inplace=True)
        convenios = convenios.drop(columns=["NM_PROPONENTE", "COD_MUNIC_IBGE"]).drop_duplicates()
        proponentes = set_types(proponentes, csv_proponentes_type)
        feedback(self.logger, label='-> proponentes', value=f'{len(proponentes)} linhas')

        return proponentes, convenios

    def __tranform_emendas_convenios__(self, emendas, convenios, propostas):
        feedback(self.logger, label='-> emendas_convenios', value='transforming...')
        emendas = emendas[emendas['NR_EMENDA'].notna()].copy()
        propostas_ano = propostas.filter(['ID_PROPOSTA', 'ANO_PROP'])
        emendas = pd.merge(emendas, propostas_ano, how='inner', on='ID_PROPOSTA', left_index=False, right_index=False).drop_duplicates()
        emendas['NR_EMENDA'] = emendas['ANO_PROP'].astype(str) + emendas['NR_EMENDA'].astype(str)    
        emendas_convenios = pd.merge(emendas, convenios, how='inner', on='ID_PROPOSTA', left_index=False, right_index=False)
        emendas_convenios['VALOR_REPASSE_EMENDA'] = emendas_convenios['VALOR_REPASSE_EMENDA'].fillna(0)
        emendas_convenios['VALOR_REPASSE_EMENDA'] = emendas_convenios['VALOR_REPASSE_EMENDA'].str.replace(',', '.', regex=False)

        emendas = emendas_convenios.filter(['NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR', 'VALOR_REPASSE_EMENDA']).drop_duplicates()
        
        emendas_convenios = emendas_convenios.filter(['NR_EMENDA', 'NR_CONVENIO', 'VALOR_REPASSE_EMENDA']).drop_duplicates()
        emendas_convenios = set_types(emendas_convenios, csv_emendas_type_convenios)
        
        feedback(self.logger, label='-> emendas_convenios', value=f'{len(emendas_convenios)} linhas')
        return emendas_convenios, emendas

    def __tansform_emendas__(self, emendas):
        feedback(self.logger, label='-> emendas', value='transforming...')
        emendas.rename(columns={'VALOR_REPASSE_EMENDA': 'VALOR_EMENDA'}, inplace=True)
        emendas.loc[emendas['NOME_PARLAMENTAR'].isna(), 'NOME_PARLAMENTAR'] = '#(NÃO ESPECIFICADO)'
        emendas.loc[emendas['TIPO_PARLAMENTAR'].isna(), 'TIPO_PARLAMENTAR'] = '#(NÃO ESPECIFICADO)'
        emendas['VALOR_EMENDA'] = emendas['VALOR_EMENDA'].astype(float)
        emendas = emendas.groupby(['NR_EMENDA', 'NOME_PARLAMENTAR', 'TIPO_PARLAMENTAR'], as_index=False).sum()
        emendas = set_types(emendas, csv_emendas_type)
        feedback(self.logger, label='-> emendas', value=f'{len(emendas)} linhas')
        
        return emendas

    def __transform_convenios__(self, convenios, emendas_convenios):
        feedback(self.logger, label='-> convenios', value='transforming...')

        convenios_emendas_list = emendas_convenios['NR_CONVENIO'].unique()
        convenios['NR_CONVENIO'] = convenios['NR_CONVENIO'].astype('int64')

        convenios['COM_EMENDAS'] = False
        convenios.loc[convenios['NR_CONVENIO'].isin(convenios_emendas_list), 'COM_EMENDAS'] = True

        convenios['VL_GLOBAL_CONV'] = convenios['VL_GLOBAL_CONV'].str.replace(',', '.', regex=False)
        convenios['VL_REPASSE_CONV'] = convenios['VL_REPASSE_CONV'].str.replace(',', '.', regex=False)
        convenios['VL_CONTRAPARTIDA_CONV'] = convenios['VL_CONTRAPARTIDA_CONV'].str.replace(',', '.', regex=False)

        conv_repasse_emenda = emendas_convenios.groupby('NR_CONVENIO', as_index=False).sum()
        conv_repasse_emenda.rename(columns={'VALOR_REPASSE_EMENDA': 'VALOR_EMENDA_CONVENIO'}, inplace=True)    
        convenios['NR_CONVENIO'] = convenios['NR_CONVENIO'].astype('int64')
        convenios = pd.merge(convenios, conv_repasse_emenda, how='left', on='NR_CONVENIO', left_index=False, right_index=False)
        convenios['VALOR_EMENDA_CONVENIO'] = convenios['VALOR_EMENDA_CONVENIO'].fillna(0)

        convenios.loc[convenios['INSTRUMENTO_ATIVO'].str.upper()=='SIM', 'INSTRUMENTO_ATIVO'] = True
        convenios.loc[convenios['INSTRUMENTO_ATIVO'].str.upper()=='NAO', 'INSTRUMENTO_ATIVO'] = False

        convenios_columns = ['NR_CONVENIO', 'DIA_ASSIN_CONV', 'SIT_CONVENIO', 'INSTRUMENTO_ATIVO', 'DIA_PUBL_CONV',
                            'DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS',
                            'VL_GLOBAL_CONV', 'VL_REPASSE_CONV', 'VL_CONTRAPARTIDA_CONV', 'COD_ORGAO_SUP',
                            'DESC_ORGAO_SUP', 'NATUREZA_JURIDICA', 'COD_ORGAO', 'DESC_ORGAO', 'MODALIDADE',
                            'IDENTIF_PROPONENTE', 'OBJETO_PROPOSTA', 'COM_EMENDAS', 'VALOR_EMENDA_CONVENIO']
        convenios = convenios.filter(convenios_columns).drop_duplicates()

        convenios['INSUCESSO'] = 0
        
        convenios = set_types(convenios, csv_convenios_type)

        feedback(self.logger, label='-> convenios', value=f'{len(convenios)} linhas')

        return convenios

    def __transform_licitacoes__(self, licitacoes, convenios):
        feedback(self.logger, label='-> licitações', value='transforming...')
        convenios_list = convenios['NR_CONVENIO'].unique()
        licitacoes['NR_CONVENIO'] = licitacoes['NR_CONVENIO'].astype('int64')
        licitacoes = licitacoes.loc[licitacoes['NR_CONVENIO'].isin(convenios_list), :].copy()
        licitacoes.rename(columns={'MODALIDADE_LICITACAO': 'MODALIDADE_COMPRA'}, inplace=True)

        licitacoes['MODALIDADE_COMPRA'] = licitacoes['MODALIDADE_COMPRA'].mask(licitacoes['MODALIDADE_COMPRA']=='12', 'PREGÃO')

        licitacoes['MODALIDADE_COMPRA'] = licitacoes['MODALIDADE_COMPRA'].mask(licitacoes['MODALIDADE_COMPRA'].isna() |
                                                                                     licitacoes['MODALIDADE_COMPRA'].astype(str).str.isnumeric(), 
                                                                                     licitacoes['TP_PROCESSO_COMPRA'])
        
        licitacoes['MODALIDADE_COMPRA'] = licitacoes['MODALIDADE_COMPRA'].mask(licitacoes['MODALIDADE_COMPRA'].isna() |
                                                                                     licitacoes['MODALIDADE_COMPRA'].astype(str).str.isnumeric(),
                                                                                     '#(NÃO ESPECIFICADA)')

        licitacoes['TIPO_LICITACAO'] = licitacoes['TIPO_LICITACAO'].mask(licitacoes['TIPO_LICITACAO'].astype(str).str.isnumeric(), None)
        
        licitacoes['VALOR_LICITACAO'] = licitacoes['VALOR_LICITACAO'].str.replace(',', '.', regex=False)


        licitacoes['FORMA_LICITACAO'] = '#(NÃO APLICÁVEL)'
        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.startswith('presencial'), 'FORMA_LICITACAO'] = 'PRESENCIAL'
        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.startswith('eletrônico') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.startswith('eletronico'), 'FORMA_LICITACAO'] = 'ELETRÔNICO'

        licitacoes['REGISTRO_PRECOS'] = False
        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('registro de preço') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('registro de preco'), 'REGISTRO_PRECOS'] = True

        licitacoes['LICITACAO_INTERNACIONAL'] = False
        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.startswith('LICITACAO_INTERNACIONAL'), 'LICITACAO_INTERNACIONAL'] = True

        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('menor preço') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('menor preco') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('menor valor'), 'TIPO_LICITACAO'] = 'MENOR PREÇO'

        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('melhor técnica') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('melhor tecnica'), 'TIPO_LICITACAO'] = 'MELHOR TÉCNICA'

        licitacoes.loc[licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('técnica e preço') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('técnica e preco') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('tecnica e preço') |
                       licitacoes['TIPO_LICITACAO'].astype(str).str.lower().str.contains('tecnica e preco'), 'TIPO_LICITACAO'] = 'TÉCNICA E PREÇO'

        licitacoes.loc[~licitacoes['TIPO_LICITACAO'].astype(str).str.lower().isin(['técnica e preço', 'menor preço', 'melhor técnica']), 
                                                                                 'TIPO_LICITACAO'] = '#(NÃO APLICÁVEL)'

        licitacoes.loc[licitacoes['STATUS_LICITACAO'].isna(), 'STATUS_LICITACAO'] = '#(INDEFINIDO)'
        
        licitacoes = licitacoes.filter(['ID_LICITACAO', 'NR_CONVENIO', 'MODALIDADE_COMPRA',
                                        'FORMA_LICITACAO', 'REGISTRO_PRECOS', 'LICITACAO_INTERNACIONAL', 'TIPO_LICITACAO', 
                                        'STATUS_LICITACAO', 'VALOR_LICITACAO']).drop_duplicates()

        licitacoes['MODALIDADE_COMPRA'] = licitacoes['MODALIDADE_COMPRA'].str.upper()
        licitacoes['STATUS_LICITACAO'] = licitacoes['STATUS_LICITACAO'].str.upper()
        licitacoes['VALOR_LICITACAO'] = licitacoes['VALOR_LICITACAO'].fillna(0)

        licitacoes = set_types(licitacoes, csv_licitacoes_type)
        feedback(self.logger, label='-> licitações', value=f'{len(licitacoes)} linhas')

        return licitacoes

    def __transform_fornecedores__(self, pagamentos, convenios, obtv):
        feedback(self.logger, label='-> fornecedores', value='transforming...')
        convenios_list = convenios['NR_CONVENIO'].unique()
        pagamentos['NR_CONVENIO'] = pagamentos['NR_CONVENIO'].astype('int64')
        pagamentos = pagamentos.loc[pagamentos['NR_CONVENIO'].isin(convenios_list) & 
                                pagamentos['DATA_PAG'].notna(), :]

        pagamentos = pd.merge(pagamentos, obtv, how='left', on='NR_MOV_FIN', left_index=False, right_index=False)


        pagamentos['IDENTIF_FORNECEDOR'] = pagamentos['IDENTIF_FORNECEDOR'].mask(pagamentos['IDENTIF_FAVORECIDO_OBTV_CONV'].notna(), 
                                                        pagamentos['IDENTIF_FAVORECIDO_OBTV_CONV'])
        
        pagamentos['NOME_FORNECEDOR'] = pagamentos['NOME_FORNECEDOR'].mask(pagamentos['IDENTIF_FAVORECIDO_OBTV_CONV'].notna(), 
                                                                           pagamentos['NM_FAVORECIDO_OBTV_CONV'])
        pagamentos['VL_PAGO'] = pagamentos['VL_PAGO'].mask(pagamentos['IDENTIF_FAVORECIDO_OBTV_CONV'].notna(), 
                                                           pagamentos['VL_PAGO_OBTV_CONV'])
        
        pagamentos = pagamentos.loc[pagamentos['VL_PAGO'].notna() & 
                                    pagamentos['VL_PAGO'].str.replace(',', '.', regex=False).astype(float)!=0, :]

        pagamentos.loc[pagamentos['IDENTIF_FORNECEDOR'].isna(), 'IDENTIF_FORNECEDOR'] = '#(NÃO ESPECIFICADA)'
        pagamentos.loc[pagamentos['NOME_FORNECEDOR'].isna(), 'NOME_FORNECEDOR'] = '#(NÃO ESPECIFICADO)'

        fornecedores = pd.DataFrame(pagamentos[['IDENTIF_FORNECEDOR', 'NOME_FORNECEDOR']].drop_duplicates(), dtype=str)
        fornecedores['IDENTIF_FORNECEDOR'] = fornecedores['IDENTIF_FORNECEDOR'].str.upper()
        fornecedores['NOME_FORNECEDOR'] = fornecedores['NOME_FORNECEDOR'].str.upper()
        fornecedores = fornecedores.drop_duplicates()
        fornecedores['FORNECEDOR_ID'] = fornecedores.index + 1

        nao_aplicavel_ = pd.DataFrame([{
                            'FORNECEDOR_ID': -1, 
                            'IDENTIF_FORNECEDOR': 'NAO APLICAVEL', 
                            'NOME_FORNECEDOR': 'NAO APLICAVEL'
                            }])

        fornecedores = pd.concat([fornecedores, nao_aplicavel_], axis=0, sort=False, ignore_index=True)

        fornecedores = set_types(fornecedores, csv_fornecedores_type)
        
        feedback(self.logger, label='-> fornecedores', value=f'{len(fornecedores)} linhas')
        
        return fornecedores, pagamentos

    def __transform_movimento__(self, pagamentos, desembolsos, contrapartidas, convenios, fornecedores, tributos):
        
        def __fix_pagamentos__(pagamentos):
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
                {'convenio': '705156', 'ref': '06/11/2007', 'valor': '06/11/2009'},
                {'convenio': '769286', 'ref': '03/06/2031', 'valor': '03/06/2013'},
                ]

            for fix in fix_list:
                pagamentos.loc[(pagamentos['NR_CONVENIO']==fix['convenio']) & (pagamentos['DATA'] == fix['ref']), 
                            'DATA'] = fix['valor']

            return pagamentos

        def __fix_tributos__(tributos, pagamentos, convenios):
            fix_list1 = {'/0001':'/2001', '/0009':'/2009', '/0010':'/2010', '/0011':'/2011',
                        '/0200':'/2010', '/0201':'/2010', '/0209':'/2009', '/0210':'/2010'}
            
            fix_list2 = [
                        {'convenio': 700427, 'ref': '30/09/2007', 'valor': '30/09/2009'},
                        {'convenio': 700427, 'ref': '01/09/8009', 'valor': '01/09/2009'},
                        {'convenio': 702429, 'ref': '10/06/2000', 'valor': '10/06/2010'},
                        {'convenio': 702429, 'ref': '10/06/2001', 'valor': '10/06/2010'},
                        {'convenio': 703330, 'ref': '08/03/2029', 'valor': '08/03/2009'},
                        {'convenio': 704400, 'ref': '05/10/2209', 'valor': '05/10/2009'},
                        {'convenio': 704419, 'ref': '30/11/1975', 'valor': '30/11/2010'},
                        {'convenio': 705792, 'ref': '25/04/2002', 'valor': '25/04/2010'},
                        {'convenio': 707660, 'ref': '18/02/2001', 'valor': '18/02/2012'},
                        {'convenio': 713794, 'ref': '07/06/2001', 'valor': '07/06/2011'},
                        {'convenio': 717289, 'ref': '20/05/2044', 'valor': '20/05/2012'},
                        {'convenio': 718214, 'ref': '15/04/2001', 'valor': '15/04/2010'},
                        {'convenio': 721788, 'ref': '20/01/2005', 'valor': '20/01/2011'},
                        {'convenio': 727573, 'ref': '30/07/2007', 'valor': '30/07/2010'},
                        {'convenio': 735447, 'ref': '22/10/1984', 'valor': '22/10/2013'},
                        {'convenio': 744662, 'ref': '01/09/2001', 'valor': '01/09/2011'},
                        {'convenio': 750595, 'ref': '17/11/2001', 'valor': '17/11/2011'},
                        {'convenio': 750893, 'ref': '23/01/2047', 'valor': '23/01/2017'},
                        {'convenio': 754694, 'ref': '12/12/2001', 'valor': '12/12/2011'},
                        {'convenio': 755872, 'ref': '25/11/1979', 'valor': '25/11/2011'},
                        {'convenio': 758189, 'ref': '30/10/1968', 'valor': '30/10/2012'},
                        {'convenio': 769286, 'ref': '10/04/2031', 'valor': '10/04/2013'},
                        {'convenio': 770959, 'ref': '03/11/2004', 'valor': '03/11/2014'},
                        ]

            for flaw in fix_list1:
                tributos.loc[tributos['DATA_TRIBUTO'].astype(str).str.endswith(flaw), 'DATA_TRIBUTO'] = fix_list1[flaw]

            tributos = pd.merge(tributos, pagamentos.groupby('NR_CONVENIO').min(), how='left', on=['NR_CONVENIO'], left_index=False, right_index=False)
            tributos = pd.merge(tributos, convenios, how='left', on=['NR_CONVENIO'], left_index=False, right_index=False)

            tributos['DATA_TRIBUTO'] = tributos['DATA_TRIBUTO'].mask(tributos['DATA_TRIBUTO'].isna(), tributos['DATA'])
            tributos['DATA_TRIBUTO'] = tributos['DATA_TRIBUTO'].mask(tributos['DATA_TRIBUTO'].isna(), tributos['DIA_INIC_VIGENC_CONV'])
            
            tributos = tributos.drop(['DATA', 'DIA_INIC_VIGENC_CONV'], axis=1)

            for fix in fix_list2:
                tributos.loc[(tributos['NR_CONVENIO']==fix['convenio']) & 
                             (tributos['DATA_TRIBUTO'] == fix['ref']), 'DATA_TRIBUTO'] = fix['valor']

            return tributos
        

        feedback(self.logger, label='-> movimento', value='transforming...')

        convenios_list = convenios['NR_CONVENIO'].unique()
    
        assinatura_convenios = pd.DataFrame(convenios[['NR_CONVENIO', 'DIA_ASSIN_CONV', 
                                                    'VL_GLOBAL_CONV']], dtype=str)
        assinatura_convenios.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
        assinatura_convenios['TIPO'] = 'A'
        
        inicio_vigencia_convenios = pd.DataFrame(convenios[['NR_CONVENIO', 'DIA_INIC_VIGENC_CONV', 
                                                            'VL_GLOBAL_CONV']], dtype=str)
        inicio_vigencia_convenios.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
        inicio_vigencia_convenios['TIPO'] = 'I'
        
        fim_vigencia_convenios = pd.DataFrame(convenios[['NR_CONVENIO', 'DIA_FIM_VIGENC_CONV', 
                                                        'VL_GLOBAL_CONV']], dtype=str)
        fim_vigencia_convenios.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
        fim_vigencia_convenios['TIPO'] = 'F'
        
        desembolsos['NR_CONVENIO'] = desembolsos['NR_CONVENIO'].astype('int64')
        desembolsos = pd.DataFrame(desembolsos[desembolsos['NR_CONVENIO'].isin(convenios_list) & 
                                desembolsos['DATA_DESEMBOLSO'].notna() &
                                desembolsos['VL_DESEMBOLSADO'].notna() &
                                desembolsos['VL_DESEMBOLSADO']!=0], dtype=str)
        
        desembolsos = desembolsos[['NR_CONVENIO', 'DATA_DESEMBOLSO', 'VL_DESEMBOLSADO']]
        desembolsos.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
        desembolsos['TIPO'] = 'D'
        desembolsos['VALOR'] = desembolsos['VALOR'].str.replace(',', '.', regex=False)
        desembolsos['VALOR'] = desembolsos['VALOR'].astype(float)

        contrapartidas['NR_CONVENIO'] = contrapartidas['NR_CONVENIO'].astype('int64')
        contrapartidas = pd.DataFrame(contrapartidas[contrapartidas['NR_CONVENIO'].isin(convenios_list) & 
                                        contrapartidas['DT_INGRESSO_CONTRAPARTIDA'].notna() &
                                        contrapartidas['VL_INGRESSO_CONTRAPARTIDA'].notna() &
                                        contrapartidas['VL_INGRESSO_CONTRAPARTIDA']!=0], dtype=str)

        contrapartidas = contrapartidas[['NR_CONVENIO', 'DT_INGRESSO_CONTRAPARTIDA', 'VL_INGRESSO_CONTRAPARTIDA']]
        contrapartidas.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
        contrapartidas['TIPO'] = 'C'
        contrapartidas['VALOR'] = contrapartidas['VALOR'].str.replace(',', '.', regex=False)
        contrapartidas['VALOR'] = contrapartidas['VALOR'].astype(float)
        
        pagamentos = pd.merge(pagamentos, fornecedores, how='inner', on=['IDENTIF_FORNECEDOR', 'NOME_FORNECEDOR'], 
                            left_index=False, right_index=False)
        pagamentos = pagamentos[['NR_CONVENIO', 'FORNECEDOR_ID', 'DATA_PAG', 'VL_PAGO']]
        pagamentos.columns = ['NR_CONVENIO', 'FORNECEDOR_ID', 'DATA', 'VALOR']
        pagamentos['TIPO'] = 'P'
        pagamentos['VALOR'] = pagamentos['VALOR'].str.replace(',', '.', regex=False)
        pagamentos['VALOR'] = pagamentos['VALOR'].astype(float)    
        pagamentos = __fix_pagamentos__(pagamentos)
        

        tributos['NR_CONVENIO'] = tributos['NR_CONVENIO'].astype('int64')
        tributos = __fix_tributos__(tributos, pagamentos, convenios)
        tributos = tributos[['NR_CONVENIO', 'DATA_TRIBUTO', 'VL_PAG_TRIBUTOS']]
        tributos.columns = ['NR_CONVENIO', 'DATA', 'VALOR']
        tributos['TIPO'] = 'T'
        tributos['VALOR'] = tributos['VALOR'].str.replace(',', '.', regex=False)
        tributos['VALOR'] = tributos['VALOR'].astype(float)


        movimento = pd.concat([assinatura_convenios, inicio_vigencia_convenios, 
                            fim_vigencia_convenios, desembolsos, contrapartidas, 
                            pagamentos, tributos], ignore_index=True, sort=False)
                            
        
        movimento.loc[movimento['FORNECEDOR_ID'].isna(), 'FORNECEDOR_ID'] = -1
        movimento['MOV_ID'] = movimento.index + 1
        movimento.rename(columns={'DATA': 'DATA_MOV', 'TIPO': 'TIPO_MOV', 'VALOR': 'VALOR_MOV'}, inplace=True)
        movimento = set_types(movimento, csv_movimento_type)
        
        feedback(self.logger, label='-> movimento', value=f'{len(movimento)} linhas')
        
        return movimento

    def __transform_calendario__(self, movimento, current_date):
        def months_names(dt):
            months = {
            '1': 'janeiro', '2': 'fevereiro', '3': 'março', 
            '4': 'abril', '5': 'maio', '6': 'junho', 
            '7': 'julho', '8': 'agosto', '9': 'setembro', 
            '10': 'outubro', '11': 'novembro', '12': 'dezembro'
            }
            
            return months[str(dt.month)]

        feedback(self.logger, label='-> calendário', value='transforming...')

        min_dt = movimento['DATA_MOV'].min().date()
        periods = (current_date - min_dt).days + 1
        calendario = pd.DataFrame(columns=['DATA_MOV'], data=pd.date_range(min_dt, periods=periods, freq="D"))
        
        calendario.rename(columns={'DATA_MOV': 'DATA'}, inplace=True)

        calendario['ANO'] = calendario['DATA'].dt.year

        calendario['MES_NUMERO'] = calendario['DATA'].dt.month
        calendario['MES_NOME'] = calendario['DATA'].apply(months_names)
        calendario['ANO_MES_NUMERO'] = calendario['ANO']*100 + calendario['MES_NUMERO']
        calendario['MES_ANO'] = calendario['MES_NUMERO'].astype(str).str.zfill(2) + ' ' + calendario['ANO'].astype(str)
        calendario['MES_NOME_ANO'] = calendario['MES_NOME'] + ' ' + calendario['ANO'].astype(str)

        calendario['TRIMESTRE_NUMERO'] = calendario['DATA'].dt.quarter
        calendario['TRIMESTRE_NOME'] = calendario['DATA'].dt.quarter.astype(str) + 'o semestre'
        calendario['ANO_TRIMESTRE_NUMERO'] = calendario['ANO']*100 + calendario['TRIMESTRE_NUMERO']
        calendario['TRIMESTRE_ANO'] = calendario['TRIMESTRE_NUMERO'].astype(str).str.zfill(2) + ' ' + calendario['ANO'].astype(str)
        calendario['TRIMESTRE_NOME_ANO'] = calendario['TRIMESTRE_NOME'] + ' ' + calendario['ANO'].astype(str)

        calendario['SEMANA_NUMERO'] = calendario['DATA'].dt.isocalendar().week
        calendario['SEMANA_NOME'] = calendario['SEMANA_NUMERO'].astype(str) + 'a semana'
        calendario['ANO_SEMANA_NUMERO'] = calendario['ANO']*100 + calendario['SEMANA_NUMERO']
        calendario['SEMANA_ANO'] = calendario['SEMANA_NUMERO'].astype(str).str.zfill(2) + ' ' + calendario['ANO'].astype(str)
        calendario['SEMANA_NOME_ANO'] = calendario['SEMANA_NOME'] + ' ' + calendario['ANO'].astype(str)

        calendario['DIA_DA_SEMANA'] = calendario['DATA'].dt.weekday.replace(0, 'segunda-feira').replace(1, 'terça-feira').\
                                                                    replace(2, 'quarta-feira').replace(3, 'quinta-feira').\
                                                                    replace(4, 'sexta-feira').replace(5, 'sabado').replace(6, 'domingo')
        calendario = set_types(calendario, csv_calendario_type)

        feedback(self.logger, label='-> calendário', value=f'{len(calendario)} linhas')
        
        return calendario

    def __transform_data_atual__(self, current_date):
        feedback(self.logger, label='-> data atual', value='transforming...')
        data_atual = pd.DataFrame(data={'DATA_ATUAL': [current_date.strftime("%d/%m/%Y")]})
        data_atual = set_types(data_atual, csv_data_atual_type)
        feedback(self.logger, label='-> data atual', value='Success!')
        
        return data_atual

    def __risk_analyzer__(self, convenios, proponentes, emendas, emendas_convenios, fornecedores, movimento):
        ra = RiskAnalyzer()

        feedback(self.logger, label='-> risk analyzer', value='analyzing...')
        convenios['INSUCESSO'] = 0
        convenios.loc[convenios['SIT_CONVENIO'].isin(['Prestação de Contas Rejeitada', 'Inadimplente', 'Convênio Rescindido']), 'INSUCESSO'] = 1
        
        convenios_em_execucao_ = convenios.loc[convenios['SIT_CONVENIO'].str.lower()=='em execução']
        risks = ra.run(convenios_em_execucao_, proponentes, emendas, emendas_convenios, 
                                    fornecedores, movimento, append=False)
        convenios.loc[convenios['NR_CONVENIO'].isin(convenios_em_execucao_['NR_CONVENIO'].to_list()), 
            ['INSUCESSO']] = risks.to_list()
        feedback(self.logger, label='-> risk analyzer', value=f'{len(convenios_em_execucao_)} linhas')
        
        return convenios
