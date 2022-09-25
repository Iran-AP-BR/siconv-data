# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path
from .utils import datetime_validation


class FileTools(object):
    def __init__(self, config) -> None:
        self.config = config
        Path(os.path.join(self.config.STAGE_FOLDER)).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(self.config.DATA_FOLDER)).mkdir(parents=True, exist_ok=True)


    def get_files_date(self, with_exception=False):
        try:
            data_atual = self.read_data(tbl_name='data_atual')['DATA_ATUAL'][0]
            return datetime_validation(str(data_atual), dayfirst=False)
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
        table.to_parquet(os.path.join(self.config.DATA_FOLDER, f'{table_name}{self.config.FILE_EXTENTION}'),
                            compression=self.config.COMPRESSION_METHOD, index=False)    

    def read_data(self, tbl_name):
        return pd.read_parquet(os.path.join(self.config.DATA_FOLDER, 
                                            f'{tbl_name}{self.config.FILE_EXTENTION}'))

    def read_from_stage(self, tbl_name):
        return pd.read_parquet(os.path.join(self.config.STAGE_FOLDER, 
                                            f'{tbl_name}{self.config.FILE_EXTENTION}'))

    def write_to_stage(self, table, table_name=''):
        table = self.__strip_white_spaces__(table)
        table.to_parquet(os.path.join(self.config.STAGE_FOLDER, f'{table_name}{self.config.FILE_EXTENTION}'),
                            compression=self.config.COMPRESSION_METHOD, index=False)  
