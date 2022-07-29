# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path
from .utils import datetime_validation


class CSVTools(object):
    def __init__(self, config) -> None:
        self.config = config
        Path(os.path.join(self.config.STAGE_FOLDER)).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(self.config.DATA_FOLDER)).mkdir(parents=True, exist_ok=True)


    def get_csv_date(self, with_exception=False):
        try:
            data_atual = self.read_data(tbl_name='data_atual', chunked=False)['DATA_ATUAL'][0]
            return datetime_validation(data_atual)
        except:
            if with_exception:
                raise Exception(f'Não foi possível obter a data atual.')
            else:
                return None

    def __strip_white_spaces__(self, dataframe):
        df_str = dataframe.select_dtypes(['object'])
        dataframe[df_str.columns] = df_str.apply(lambda x: x.str.strip())
        return dataframe

    def write_data(self, table, table_name):
        table.to_csv(os.path.join(self.config.DATA_FOLDER, f'{table_name}{self.config.FILE_EXTENTION}'),
                            compression=self.config.COMPRESSION_METHOD, sep=';', decimal=',',
                            encoding='utf-8', index=False)    

    def read_data(self, tbl_name, dtypes=str, parse_dates=False, chunked=True):
        return pd.read_csv(os.path.join(self.config.DATA_FOLDER, f'{tbl_name}{self.config.FILE_EXTENTION}'),
                        compression=self.config.COMPRESSION_METHOD, sep=';', decimal=',', 
                        encoding='utf-8', dtype=dtypes, chunksize=self.config.CHUNK_SIZE if chunked else None, 
                        parse_dates=parse_dates, dayfirst=True)

    def read_from_stage(self, tbl_name):
        return pd.read_csv(os.path.join(self.config.STAGE_FOLDER, f'{tbl_name}{self.config.FILE_EXTENTION}'),
                        compression=self.config.COMPRESSION_METHOD, sep=';', encoding='utf-8', dtype=str)

    def write_to_stage(self, table, table_name=''):
        table = self.__strip_white_spaces__(table)
        table.to_csv(os.path.join(self.config.STAGE_FOLDER, f'{table_name}{self.config.FILE_EXTENTION}'),
                            compression=self.config.COMPRESSION_METHOD, sep=';', encoding='utf-8', index=False)  

