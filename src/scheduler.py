"""
Scheduler is an independent event-trigger management module.
Extends from flask_apscheduler package and allows, polling for changes of event schedules.

"""

from flask_apscheduler import APScheduler
from typing import Callable
import time
import datetime
from flask import Flask
import pytz
import logging


class Scheduler:

    def __init__(self, app: Flask, polling_interval: int = 1, polling_threshold: int = 300):
        self.__log = logging.getLogger(name='event-scheduler')
        self._scheduler = APScheduler()
        self._polling = APScheduler()
        self.__polling_interval = polling_interval
        self.__polling_threshold = polling_threshold
        self.start(app)

    def start(self, app):
        self._scheduler.init_app(app)
        self._polling.init_app(app)
        self._scheduler.start()
        self._polling.start()
        self.__log.info('Scheduler initialization complete.')

    def wrap_job(self, func: Callable):
        _id = 'schedule_initializer'

        jobs = func()
        registered_jobs = [j.id for j in self._scheduler.get_jobs()]

        for job in jobs:
            if job[0] not in registered_jobs:
                self.add_job(*job)

        # add itself too
        if _id not in registered_jobs:
            self._scheduler.add_job(id=_id, func=self.wrap_job, args=[func],
                                    trigger='cron', minute='*/5')

    def __remove_polling_job(self, id: str) -> bool:
        if self._scheduler.get_job(id=id) is None:
            self.__log.info('Remove job: %s from polling, met polling buffer.', id)
            self._polling.remove_job(self.__generate_polling_id(id))
            return True

        now = datetime.datetime.fromtimestamp(time.time(), tz=pytz.timezone('Asia/Tokyo'))
        sched_run_time: datetime.datetime = self._scheduler.get_job(id).next_run_time

        if (sched_run_time - now).seconds <= self.__polling_threshold:
            self.__log.info('Remove job: %s from polling, met polling buffer.', id)
            self._polling.remove_job(self.__generate_polling_id(id))
            return True
        return False

    def __generate_polling_id(self, id: str):
        # return id
        return id + '#_polling'

    def __update_scheduler(self, id: str, poll_func: Callable):
        if self.__remove_polling_job(id=id):
            return
        poll_run_time:datetime.datetime = poll_func(id)
        # KNOW if the schedule was deleted, remove polling and also scheduler
        if poll_run_time is None:
            self._polling.remove_job(self.__generate_polling_id(id))
            self._scheduler.remove_job(id)
            self.__log.info('Polled schedule not found, removing job from scheduler: %s', id)
            return
        sched_run_time: datetime.datetime = self._scheduler.get_job(id).next_run_time
        if not poll_run_time == sched_run_time:
            self.__log.info('Job schedule has been updated, updating schedule for: %s;'
                            'previous schedule: %s, new schedule: %s', id,
                            sched_run_time, poll_run_time)
            self._scheduler.modify_job(id=id, trigger='date', run_date=poll_run_time)

    def add_job(self, id: str,  trigger_func: Callable, trigger_at: Callable):
        if self._scheduler.get_job(id):
            raise ValueError('<id> already taken, please provide unique id.')

        _polling_id = self.__generate_polling_id(id)
        self._polling.add_job(id=_polling_id, func=self.__update_scheduler,
                              args=[id, trigger_at],
                              trigger='cron', second='*/{}'.format(self.__polling_interval))

        _trigger_at = trigger_at(id)
        if not isinstance(_trigger_at, datetime.datetime):
            raise ValueError('<trigger_at must return a datetime.>')
        self.__add_job(id=id, func=trigger_func, trigger_at= _trigger_at)

    def __add_job(self, id: str,  func: Callable, trigger_at: datetime.datetime):
        self.__log.info('Scheduler add job: id: %s', id)
        self._scheduler.add_job(id=id, func=func, trigger='date', run_date=trigger_at, args=[id])



def _execute_event(id: str):
    print(f'Event: {id} has been triggered.')


def _trigger_time(id:str, sec: float=5):
    if not _trigger_time.fix:
        _trigger_time.fix = datetime.datetime.fromtimestamp(time.time() + sec, tz=pytz.timezone('Asia/Tokyo'))

    return _trigger_time.fix


_trigger_time.fix: datetime.date = None


def _trigger_time_variable(id:str, sec=20, jump=5):
    if not _trigger_time_variable.fix:
        _trigger_time_variable.fix = datetime.datetime.fromtimestamp(time.time() + sec, tz=pytz.timezone('Asia/Tokyo'))
        return _trigger_time_variable.fix

    now = datetime.datetime.fromtimestamp(time.time() + sec, tz=pytz.timezone('Asia/Tokyo'))
    if (now - _trigger_time_variable.fix).seconds >= jump:
        _trigger_time_variable.fix = now

    return _trigger_time_variable.fix


_trigger_time_variable.fix: datetime.date = None

def _initialize_logger():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(name='event-scheduler')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)


if __name__ == '__main__':
    _initialize_logger()
    _app = Flask(__name__)

    sched = Scheduler(_app, polling_interval=1, polling_threshold=5)

    sched.add_job(id='hello', trigger_func=_execute_event, trigger_at=_trigger_time_variable)

    _app.run()
