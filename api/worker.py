# coding: utf-8


from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR
from dotenv import load_dotenv

from datetime import datetime
from pathlib import Path
from app.logger import app_log
from etl import ETL

USE_SCHEDULER = False
TIMEOUT_HOUR = 21

def get_env(env_path):
    path = Path(env_path)
    if path.is_file():
        load_dotenv(dotenv_path=env_path, override=True)
    else:
        load_dotenv(dotenv_path=path.name, override=True)

    from app.config import Config
    return Config()

def etl_pipeline_error_listener(event):
    app_log.info('Processo falhou!')

def etl_pipeline_max_instances(event):
    app_log.info('max instances!')

def etl_pipeline(config):

    etl = ETL(config=config, logger=app_log)
    done = etl.pipeline(force_download=False, force_csv_update=False, force_database_update=False)
    
    if USE_SCHEDULER:
        timeout = datetime.utcnow().hour >= TIMEOUT_HOUR
        if done:
            app_log.info('Done!')
            sched.shutdown(wait=False)
        elif timeout:
            app_log.info('Time out!')
            sched.shutdown(wait=False)


if __name__ == '__main__':

    config = get_env(env_path = '/home/siconvdata/.env')

    if USE_SCHEDULER:

        sched = BlockingScheduler(timezone=config.TIMEZONE)
        sched.add_job(lambda: etl_pipeline(config), 'cron', day_of_week='*', hour='8/1', minute='50', 
                    max_instances=1, id='etl', name='etl_pipeline', coalesce=True)
        sched.add_listener(etl_pipeline_error_listener, EVENT_JOB_ERROR)

        sched.start()

    else:
        etl_pipeline(config)    