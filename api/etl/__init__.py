# -*- coding: utf-8 -*-

import requests
import gc
from datetime import datetime, timezone, timedelta
import sqlalchemy as sa
from sqlalchemy_utils import database_exists
from .extraction import Extraction
from .transformation import Transformation
from .loader import CSVLoader, DBLoader
from .csv_data_tools import CSVTools
from .csv_exceptions import *
from .db_exceptions import *
from .utils import *

'''
exc_type, exc_obj, exc_tb = sys.exc_info()
fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
print(exc_type, fname, exc_tb.tb_lineno)
'''

class ETL(object):
    def __init__(self, config, logger) -> None:
        self.config = config
        self.engine = None
        self.logger = logger
        self.csv_tools = CSVTools(config=config)

    def pipeline(self, force_download=True, force_csv_update=False, force_database_update=False):
        csv_ok = False
        db_ok = False
        try:
            current_date = self.check_update(target='csv', force_update=force_csv_update or force_download)
            
            self.extractor = Extraction(config=self.config, logger=self.logger)
            extracted = self.extractor.extract(current_date=current_date, force_download=force_download)
            
            self.transformer = Transformation(logger=self.logger)
            transformed = self.transformer.transform(*extracted, current_date)
            
            del extracted
            gc.collect()

            self.csv_loader = CSVLoader(config=self.config, logger=self.logger)
            self.csv_loader.load(*transformed)

            del transformed
            gc.collect()

            csv_ok = True
        except CSVUpToDateException:
            self.logger.info('CSV: Dados já estão atualizados.')
            csv_ok = True
        except CSVUnchangedException:
            self.logger.info('CSV: Dados inalterados na origem.')
        except Exception as e:
            raise Exception(f'CSV files: {str(e)}')

        try:
            self.engine = self.connect_database()
            if self.engine is not None:
                self.check_update(target='db', force_update=force_database_update)
                
                self.db_loader = DBLoader(config=self.config, engine=self.engine, logger=self.logger)
                self.db_loader.load()

            db_ok = True
        except DBUpToDateException:
            self.logger.info('Database: Dados já estão atualizados.')
            db_ok = True 
        except DBUnchangedException:
            self.logger.info('Database: Dados inalterados na origem.')
        except Exception as e:
            raise Exception(f'Database: {str(e)}')

        return csv_ok and db_ok

    def connect_database(self):
        engine = None
        feedback(self.logger, label='-> database', value='connecting...')
        if database_exists(self.config.SQLALCHEMY_DATABASE_URI):
            engine = sa.create_engine(self.config.SQLALCHEMY_DATABASE_URI)
            feedback(self.logger, label='-> database', value='Success!')
        elif self.config.DATABASE_REQUIRED:
            raise DBNotFoundException(message='database not found!')
        else:
            feedback(self.logger, label='-> database', value='DATABASE NOT CONNECTED!')
        
        return engine

    def check_update(self, target, force_update=False):
        assert target in ['csv', 'db']
        
        def getCurrentDate():
            url = self.config.CURRENT_DATE_URI
            response = requests.get(url, stream=True)

            p = response.text.find('dos dados: <strong>﻿')
            dt = response.text[p+20:p+39].strip()
            current_date = datetime_validation(dt)
            if current_date is None:
                raise Exception(f'Invalid datetime: {dt}')
            return current_date

        if target == 'csv':
            self.logger.info('[Getting current date]')

            feedback(self.logger, label='-> data atual', value='connecting...')    
            
            current_date = getCurrentDate()

            feedback(self.logger, label='-> data atual', value=current_date.strftime("%Y-%m-%d"))
            
            last_date = self.csv_tools.get_csv_date()
            last_date_str = last_date.strftime("%Y-%m-%d") if last_date else 'Inexistente'
            feedback(self.logger, label='-> (CSV) última data', value=last_date_str)
            
        else:
            current_date = self.csv_tools.get_csv_date(with_exception=True)
            last_date = None
            current_date_table = 'data_atual'
            if sa.inspect(self.engine).has_table(current_date_table):
                last_date = self.engine.execute(f'select DATA_ATUAL from {current_date_table}').scalar()
            
            last_date_str = 'sem data!' if last_date is None else last_date.strftime("%Y-%m-%d")
            
            feedback(self.logger, label='-> (database) última data', value=last_date_str)

        today = datetime.now(timezone(timedelta(hours=-3))).date() 
        if not force_update and last_date:
            if  last_date >= today:
                raise CSVUpToDateException() if target == 'csv' else DBUpToDateException()

            if last_date == current_date:
                raise CSVUnchangedException() if target == 'csv' else DBUnchangedException()
        
        return current_date
