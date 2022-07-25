# coding: utf-8


from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from sqlalchemy_utils import database_exists
import sqlalchemy as sa
from datetime import datetime
from pathlib import Path
from app.logger import app_log
from etl.utils import feedback
from etl import ETL
from etl.db_exceptions import DBNotFoundException
from etl.risk_analyzer import MLModel, Metrics, DimensionalityReductionMetrics
from etl.text_transformer import TextTransformer


#sched = BlockingScheduler()
#@sched.scheduled_job('cron', day_of_week='*', hour='8/1', minute=1, max_instances=1)
def etl_pipeline(config, engine, logger):
    etl = ETL(config=config, engine=engine, logger=logger)
    done = etl.pipeline(force_download=False, force_csv_update=False, force_database_update=False)

    if done or datetime.utcnow().hour >= 21:
        app_log.info('Done')
        #sched.shutdown(wait=False)


if __name__ == '__main__':
    try:
        success = False
        env_path = '/home/siconvdata/.env'
        if Path(env_path).is_file():
            load_dotenv(dotenv_path=env_path, override=True)
        else:
            load_dotenv(dotenv_path='.env', override=True)

        from app.config import Config

        config = Config()

        engine = None
        feedback(app_log, label='-> database', value='connecting...')
        if database_exists(Config.SQLALCHEMY_DATABASE_URI):
            engine = sa.create_engine(Config.SQLALCHEMY_DATABASE_URI)
            feedback(app_log, label='-> database', value='Success!')
        elif config.DATABASE_REQUIRED:
            raise DBNotFoundException(message='database not found!')
        else:
            app_log.warning('DATABASE NOT CONNECTED!')

        #sched.start()
        etl_pipeline(config=config, engine=engine, logger=app_log)
        success = True
    except DBNotFoundException as e:
        app_log.error(e.message)
    except Exception as e:
        app_log.error(str(e))
    
    if not success:
        app_log.info('Processo falhou!')