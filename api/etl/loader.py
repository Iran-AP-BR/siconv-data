# -*- coding: utf-8 -*-

from pathlib import Path
import gc
from .csv_data_tools import CSVTools
from .db_data_tools import DBTools
from .csv_types import *
from .utils import *

class CSVLoader(object):
    def __init__(self, config, logger) -> None:
        self.config = config
        self.csv_tools = CSVTools(config=config)
        self.logger = logger

    def load(self, municipios, proponentes, convenios, emendas, emendas_convenios,\
                   movimento, fornecedores, calendario, licitacoes, data_atual):
        self.logger.info('[Loading to csv files]')

        Path(self.config.DATA_FOLDER).mkdir(parents=True, exist_ok=True)
        
        feedback(self.logger, label='-> municipios', value='updating...')
        self.csv_tools.write_data(table=municipios, table_name='municipios')
        feedback(self.logger, label='-> municipios', value=f'{len(municipios)} linhas')

        del municipios
        gc.collect()

        feedback(self.logger, label='-> proponentes', value='updating...')
        self.csv_tools.write_data(table=proponentes, table_name='proponentes')
        feedback(self.logger, label='-> proponentes', value=f'{len(proponentes)} linhas')

        del proponentes
        gc.collect()

        feedback(self.logger, label='-> convenios', value='updating...')
        self.csv_tools.write_data(table=convenios, table_name='convenios')
        feedback(self.logger, label='-> convenios', value=f'{len(convenios)} linhas')

        del convenios
        gc.collect()
        
        feedback(self.logger, label='-> emendas', value='updating...')
        self.csv_tools.write_data(table=emendas, table_name='emendas')
        feedback(self.logger, label='-> emendas', value=f'{len(emendas)} linhas')

        del emendas
        gc.collect()

        feedback(self.logger, label='-> emendas_convenios', value='updating...')
        self.csv_tools.write_data(table=emendas_convenios, table_name='emendas_convenios')
        feedback(self.logger, label='-> emendas_convenios', value=f'{len(emendas_convenios)} linhas')

        del emendas_convenios
        gc.collect()
        
        feedback(self.logger, label='-> licitaçoes', value='updating...')
        self.csv_tools.write_data(table=licitacoes, table_name='licitacoes')
        feedback(self.logger, label='-> licitações', value=f'{len(licitacoes)} linhas')

        del licitacoes
        gc.collect()
        
        feedback(self.logger, label='-> movimento', value='updating...')
        self.csv_tools.write_data(table=movimento, table_name='movimento')
        feedback(self.logger, label='-> movimento', value=f'{len(movimento)} linhas')

        del movimento
        gc.collect()

        feedback(self.logger, label='-> fornecedores', value='updating...')
        self.csv_tools.write_data(table=fornecedores, table_name='fornecedores')
        feedback(self.logger, label='-> fornecedores', value=f'{len(fornecedores)} linhas')

        del fornecedores
        gc.collect()

        feedback(self.logger, label='-> calendario', value='updating...')
        self.csv_tools.write_data(table=calendario, table_name='calendario')
        feedback(self.logger, label='-> calendario', value=f'{len(calendario)} linhas')

        del calendario
        gc.collect()

        feedback(self.logger, label='-> data atual', value='updating...')
        self.csv_tools.write_data(table=data_atual, table_name='data_atual')
        feedback(self.logger, label='-> data atual', value='Success!')

        del data_atual
        gc.collect()

        self.logger.info('CSV loading: Processo finalizado com sucesso!')


class DBLoader(object):
    def __init__(self, config, engine, logger) -> None:
        self.config = config
        self.csv_tools = CSVTools(config=config)
        self.db_tools = DBTools(config=config, engine=engine)
        self.logger = logger

    def load(self):
        self.logger.info('[Loading to Database]')
        
        feedback(self.logger, label='-> municipios', value='updating...')
        municipios = self.csv_tools.read_data(tbl_name='municipios', dtypes=csv_municipios_type)
        rows = self.db_tools.write_db(municipios, 'municipios')
        feedback(self.logger, label='-> municipios', value=f'{rows} linhas')

        del municipios
        gc.collect()
        
        feedback(self.logger, label='-> proponentes', value='updating...')
        proponentes = self.csv_tools.read_data(tbl_name='proponentes', dtypes=csv_proponentes_type)
        rows = self.db_tools.write_db(proponentes, 'proponentes')
        feedback(self.logger, label='-> proponentes', value=f'{rows} linhas')

        del proponentes
        gc.collect()

        feedback(self.logger, label='-> emendas', value='updating...')
        emendas = self.csv_tools.read_data(tbl_name='emendas', dtypes=csv_emendas_type)
        rows = self.db_tools.write_db(emendas, 'emendas')
        feedback(self.logger, label='-> emendas', value=f'{rows} linhas')

        del emendas
        gc.collect()

        feedback(self.logger, label='-> emendas_convenios', value='updating...')
        emendas_convenios = self.csv_tools.read_data(tbl_name='emendas_convenios', dtypes=csv_emendas_type_convenios)
        rows = self.db_tools.write_db(emendas_convenios, 'emendas_convenios')
        feedback(self.logger, label='-> emendas_convenios', value=f'{rows} linhas')

        del emendas_convenios
        gc.collect()
       
        feedback(self.logger, label='-> licitações', value='updating...')
        licitacoes = self.csv_tools.read_data(tbl_name='licitacoes', dtypes=csv_licitacoes_type)
        rows = self.db_tools.write_db(licitacoes, 'licitacoes')
        feedback(self.logger, label='-> licitações', value=f'{rows} linhas')

        del licitacoes
        gc.collect()
       
        feedback(self.logger, label='-> calendário', value='updating...')
        calendario = self.csv_tools.read_data(tbl_name='calendario', dtypes=csv_calendario_type, parse_dates=parse_dates_calendario)
        rows = self.db_tools.write_db(calendario, 'calendario')
        feedback(self.logger, label='-> calendário', value=f'{rows} linhas')

        del calendario
        gc.collect()

        feedback(self.logger, label='-> convenios', value='updating...')
        convenios = self.csv_tools.read_data(tbl_name='convenios', dtypes=csv_convenios_type, parse_dates=parse_dates_convenios)
        rows = self.db_tools.write_db(convenios, 'convenios')
        feedback(self.logger, label='-> convenios', value=f'{rows} linhas')

        del convenios
        gc.collect()

        feedback(self.logger, label='-> fornecedores', value='updating...')
        fornecedores = self.csv_tools.read_data(tbl_name='fornecedores', dtypes=csv_fornecedores_type)
        rows = self.db_tools.write_db(fornecedores, 'fornecedores')
        feedback(self.logger, label='-> fornecedores', value=f'{rows} linhas')

        del fornecedores
        gc.collect()
        
        feedback(self.logger, label='-> movimento', value='updating...')
        movimento = self.csv_tools.read_data(tbl_name='movimento', dtypes=csv_movimento_type, parse_dates=parse_dates_movimento)
        rows = self.db_tools.write_db(movimento, 'movimento')
        feedback(self.logger, label='-> movimento', value=f'{rows} linhas')

        del movimento
        gc.collect()
        
        feedback(self.logger, label='-> atributos', value='updating...')
        sql_insert_atributos = '''
            insert into atributos
            (ATRIBUTO, VALOR)
            select * from (
                select distinct "SIT_CONVENIO" as ATRIBUTO, SIT_CONVENIO as VALOR 
                from convenios where not SIT_CONVENIO is null
                union all
                select distinct "NATUREZA_JURIDICA", NATUREZA_JURIDICA 
                from convenios where not NATUREZA_JURIDICA is null
                union all
                select distinct "MODALIDADE_TRANSFERENCIA", MODALIDADE 
                from convenios where not MODALIDADE is null
                union all
                select distinct "TIPO_PARLAMENTAR", TIPO_PARLAMENTAR 
                from emendas where not TIPO_PARLAMENTAR is null 
                union all
                select distinct "MODALIDADE_COMPRA", MODALIDADE_COMPRA 
                from licitacoes where not MODALIDADE_COMPRA is null
                union all
                select distinct "TIPO_LICITACAO", TIPO_LICITACAO 
                from licitacoes where not TIPO_LICITACAO is null
                union all
                select distinct "FORMA_LICITACAO", FORMA_LICITACAO 
                from licitacoes where not FORMA_LICITACAO is null
                union all
                select distinct "STATUS_LICITACAO", STATUS_LICITACAO 
                from licitacoes where not STATUS_LICITACAO is null
                ) attr;
        '''
        self.db_tools.execute_sql(f'truncate table atributos;')
        self.db_tools.execute_sql(sql_insert_atributos)
        feedback(self.logger, label='-> atributos', value='Success!')

        feedback(self.logger, label='-> data atual', value='updating...')
        data_atual = self.csv_tools.read_data(tbl_name='data_atual', 
                     dtypes=csv_data_atual_type, parse_dates=parse_dates_data_atual)
        self.db_tools.write_db(data_atual, 'data_atual')
        feedback(self.logger, label='-> data atual', value='Success!')

        del data_atual
        gc.collect()
        
        self.logger.info('Database loading: Processo finalizado com sucesso!')
