from os import environ, remove
from os.path import exists
from time import sleep
from pathlib import Path
from multiprocessing import Pool, Process, cpu_count, JoinableQueue
from multiprocessing.managers import BaseManager

from project_globals import ChartYear, Environment, semaphore_off, semaphore_on, is_semaphore_set

def pm_init(): print(f'+   multiprocessing shared object manager', flush=True)
pool_notice = '(Pool)'
job_queue = JoinableQueue()
result_lookup = None
chart_yr = None
environs = None
jm = None

job_manager_semaphore = 'job_manager_semaphore'

class JobManager:

    def __init__(self, q, lookup):
        semaphore_on(job_manager_semaphore)
        print(f'+     job manager (Pool size = {cpu_count()})', flush=True)
        results = {}
        with Pool() as p:
            while is_semaphore_set(job_manager_semaphore):
                while not q.empty():
                    job = q.get()
                    results[job] = p.apply_async(job.execute, callback=job.execute_callback, error_callback=job.error_callback)
                jobs = list(results.keys())
                for job in jobs:
                    if results[job].ready():
                        result = results[job].get()
                        lookup[result[0]] = result[1]
                        del results[job]
                        q.task_done()
                sleep(1)
        p.close()

    def __del__(self): print(f'-     job manager exiting', flush=True)


class WaitForProcess(Process):
    def start(self, **kwargs):
        if is_semaphore_set(job_manager_semaphore): semaphore_off(job_manager_semaphore)
        # noinspection PyArgumentList
        super().start(**kwargs)
        while not is_semaphore_set(job_manager_semaphore):
            sleep(0.1)

class SharedObjectManager(BaseManager): pass
SharedObjectManager.register('ENV', Environment)
SharedObjectManager.register('CY', ChartYear)
som = SharedObjectManager()
