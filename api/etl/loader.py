# -*- coding: utf-8 -*-

from pathlib import Path
import gc
from .data_files_tools import FileTools
from .db_data_tools import DBTools
from .data_types import *
from .utils import *


class DBLoader(object):
    def __init__(self, config, engine, logger) -> None:
        self.config = config
        self.file_tools = FileTools(config=config)
        self.db_tools = DBTools(config=config, engine=engine)
        self.logger = logger

    def load(self):
        self.logger.info('[Loading to Database]')
        
        feedback(self.logger, label='-> municipios', value='updating...')
        municipios = self.file_tools.read_data(tbl_name='municipios')
        rows = self.db_tools.write_db(municipios, 'municipios')
        feedback(self.logger, label='-> municipios', value=f'{rows} linhas')

        del municipios
        gc.collect()
        
        feedback(self.logger, label='-> proponentes', value='updating...')
        proponentes = self.file_tools.read_data(tbl_name='proponentes')
        rows = self.db_tools.write_db(proponentes, 'proponentes')
        feedback(self.logger, label='-> proponentes', value=f'{rows} linhas')

        del proponentes
        gc.collect()

        feedback(self.logger, label='-> emendas', value='updating...')
        emendas = self.file_tools.read_data(tbl_name='emendas')
        rows = self.db_tools.write_db(emendas, 'emendas')
        feedback(self.logger, label='-> emendas', value=f'{rows} linhas')

        del emendas
        gc.collect()

        feedback(self.logger, label='-> emendas_convenios', value='updating...')
        emendas_convenios = self.file_tools.read_data(tbl_name='emendas_convenios')
        rows = self.db_tools.write_db(emendas_convenios, 'emendas_convenios')
        feedback(self.logger, label='-> emendas_convenios', value=f'{rows} linhas')

        del emendas_convenios
        gc.collect()
       
        feedback(self.logger, label='-> licitações', value='updating...')
        licitacoes = self.file_tools.read_data(tbl_name='licitacoes')
        rows = self.db_tools.write_db(licitacoes, 'licitacoes')
        feedback(self.logger, label='-> licitações', value=f'{rows} linhas')

        del licitacoes
        gc.collect()
       
        feedback(self.logger, label='-> calendário', value='updating...')
        calendario = self.file_tools.read_data(tbl_name='calendario')
        rows = self.db_tools.write_db(calendario, 'calendario')
        feedback(self.logger, label='-> calendário', value=f'{rows} linhas')

        del calendario
        gc.collect()

        feedback(self.logger, label='-> convenios', value='updating...')
        convenios = self.file_tools.read_data(tbl_name='convenios')
        rows = self.db_tools.write_db(convenios, 'convenios')
        feedback(self.logger, label='-> convenios', value=f'{rows} linhas')

        del convenios
        gc.collect()

        feedback(self.logger, label='-> fornecedores', value='updating...')
        fornecedores = self.file_tools.read_data(tbl_name='fornecedores')
        rows = self.db_tools.write_db(fornecedores, 'fornecedores')
        feedback(self.logger, label='-> fornecedores', value=f'{rows} linhas')

        del fornecedores
        gc.collect()
        
        feedback(self.logger, label='-> movimento', value='updating...')
        movimento = self.file_tools.read_data(tbl_name='movimento')
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
        data_atual = self.file_tools.read_data(tbl_name='data_atual')
        self.db_tools.write_db(data_atual, 'data_atual')
        feedback(self.logger, label='-> data atual', value='Success!')

        del data_atual
        gc.collect()
        
        self.logger.info('Database loading: Processo finalizado com sucesso!')
