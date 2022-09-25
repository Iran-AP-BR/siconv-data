# -*- coding: utf-8 -*-

class DBTools(object):
    def __init__(self, config, engine) -> None:
        self.config = config
        self.engine = engine

    def write_db(self, data_frame, table_name):
        rows_count = 0
        nrows = len(data_frame)
        self.engine.execute(f'truncate table {table_name};')
        for index in range(0, nrows, self.config.CHUNK_SIZE):
            df = data_frame[index:index+self.config.CHUNK_SIZE]
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            rows_count += len(df)
        return rows_count

    def execute_sql(self, sql_statement):
        self.engine.execute(sql_statement)
  

