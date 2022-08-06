# -*- coding: utf-8 -*-

class DBTools(object):
    def __init__(self, config, engine) -> None:
        self.config = config
        self.engine = engine

    def write_db(self, data_frame, table_name):
        rows = 0
        self.engine.execute(f'truncate table {table_name};')
        for df in data_frame:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            rows += len(df)
        return rows
  

