# coding: utf-8
from apscheduler.schedulers.blocking import BlockingScheduler
from app.workers.download_data import update
from datetime import datetime

sched = BlockingScheduler()

def task():
    if update() or datetime.utcnow().hour >= 21:
        sched.shutdown(wait=False)

@sched.scheduled_job('cron', day_of_week='*', hour='8/1', minute='*/15', max_instances=1)
def update_job():
    task()

if __name__ == '__main__':
    sched.start()
