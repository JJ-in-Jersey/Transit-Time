from time import sleep
from multiprocessing import Pool, Process, cpu_count, JoinableQueue
from multiprocessing.managers import BaseManager

from project_globals import ChartYear, Environment
from Semaphore import SimpleSemaphore as semaphore

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
        semaphore.on(job_manager_semaphore)
        print(f'+     job manager (Pool size = {cpu_count()})', flush=True)
        results = {}
        with Pool() as p:
            while semaphore.is_on(job_manager_semaphore):
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
                sleep(0.1)
        p.close()

    def __del__(self): print(f'-     job manager exiting', flush=True)


class WaitForProcess(Process):
    def start(self, **kwargs):
        if semaphore.is_on(job_manager_semaphore): semaphore.off(job_manager_semaphore)
        # noinspection PyArgumentList
        super().start(**kwargs)
        while not semaphore.is_on(job_manager_semaphore):
            sleep(0.1)
