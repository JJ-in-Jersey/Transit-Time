from os import environ, remove
from os.path import exists
from time import sleep
from pathlib import Path
from multiprocessing import Pool, Process, cpu_count
from multiprocessing.managers import BaseManager

from project_globals import ChartYear, DownloadDirectory
process_running_semaphore = Path(environ['TEMP'] + '/process_running_semaphore.tmp')

class JobManager:

    def __init__(self, q):
        remove(process_running_semaphore)
        print(f'+   job manager (Pool size = {cpu_count()})', flush=True)
        r = {}
        with Pool() as p:
            while True:
                while not q.empty():
                    job = q.get()
                    r[job] = p.apply_async(job.execute, callback=job.execute_callback)
                jobs = list(r.keys())
                for job in jobs:
                    if r[job].ready():
                        if len(r[job].get()):
                            del r[job]
                            q.task_done()
                        else: r[job] = p.apply_async(job.execute())
                sleep(0.1)

class WaitForProcess(Process):
    def start(self, **kwargs):
        open(process_running_semaphore, 'w').close()
        super().start(**kwargs)
        while exists(process_running_semaphore):
            sleep(0.1)

class SharedObjectManager(BaseManager): pass
SharedObjectManager.register('DD', DownloadDirectory)
SharedObjectManager.register('CY', ChartYear)
def pm_init(): print(f'+   multiprocessing shared object manager', flush=True)