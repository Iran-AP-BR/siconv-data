# coding: utf-8
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from app.workers.download_data import update

sched = BlockingScheduler()
@sched.scheduled_job('cron', day_of_week='*', hour='4/4', minute='25')
def update_job():
    update()

sched.start()
