# -*- coding: utf-8 -*-

import requests
from datetime import datetime, timezone, timedelta
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
    def __init__(self, config, engine, logger) -> None:
        self.config = config
        self.engine = engine
        self.logger = logger
        self.csv_tools = CSVTools(config=config)
        self.extractor = Extraction(config=config, logger=logger)
        self.transformer = Transformation(logger=logger)
        self.csv_loader = CSVLoader(config=config, logger=logger)
        self.db_loader = DBLoader(config=config, engine=engine, logger=logger)


    def pipeline(self, force_download=False, force_csv_update=False, force_database_update=False):
        csv_ok = False
        db_ok = False
        try:
            current_date = self.check_update(target='csv', force_update=force_csv_update or force_download)
            extracted = self.extractor.extract(current_date=current_date, force_download=force_download)
            transformed = self.transformer.transform(*extracted, current_date)
            self.csv_loader.load(*transformed)
            csv_ok = True
        except CSVUpToDateException:
            self.logger.info('CSV: Dados já estão atualizados.')
            csv_ok = True
        except CSVUnchangedException:
            self.logger.info('CSV: Dados inalterados na origem.')
        except Exception as e:
            raise Exception(f'CSV files: {str(e)}')

        try:
            if self.engine is not None:
                self.check_update(target='db', force_update=force_database_update)
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

            feedback(self.logger, label='-> data atual', value=current_date.strftime("%d/%m/%Y"))
            
            last_date = self.csv_tools.get_csv_date()
            
        else:
            current_date = self.csv_tools.get_csv_date(with_exception=True)
            last_date = None
            current_date_table = 'data_atual'
            if self.engine.has_table(self.engine, current_date_table):
                last_date = self.engine.execute(f'select data_atual from {current_date_table}').scalar()
                if type(last_date) == str:
                    last_date = datetime_validation(last_date)

        today = datetime.now(timezone(timedelta(hours=-3))).date() 
        
        if not force_update and last_date:
            if  last_date >= today:
                raise CSVUpToDateException() if target == 'csv' else DBUpToDateException()

            if last_date == current_date:
                raise CSVUnchangedException() if target == 'csv' else DBUnchangedException()
        
        return current_date
