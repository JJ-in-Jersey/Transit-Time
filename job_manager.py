from tt_singleton.singleton import Singleton
from tt_semaphore import simple_semaphore as semaphore
from multiprocessing import Manager, Pool, cpu_count, Process
from time import sleep


class JobManager(metaclass=Singleton):

    def put(self, job):
        self.queue.put(job)

    def get(self, key):
        return self.results_lookup[key]

    def wait(self):
        self.queue.join()

    def __init__(self):
        self.manager = Manager()
        self.queue = self.manager.JoinableQueue()
        self.results_lookup = self.manager.dict()

        # qm = Process(target=QueueManager, args=(self.queue, self.results_lookup))
        qm = WaitForProcess(target=QueueManager, name='QueueManager', args=(self.queue, self.results_lookup))
        qm.start()

# jm = mpm.WaitForProcess(target=mpm.JobManager, args=(mpm.job_queue, mpm.result_lookup))


class QueueManager(metaclass=Singleton):

    def __init__(self, q, lookup):
        print(f'+     queue manager (Pool size = {cpu_count()})\n', flush=True)
        semaphore.on(self.__class__.__name__)
        results = {}
        with Pool(2) as p:
            while True:
                # pull submitted jobs and start them in the pool
                while not q.empty():
                    job = q.get()
                    results[job] = p.apply_async(job.execute, callback=job.execute_callback, error_callback=job.error_callback)

                # check results for complete job and put them on external lookup
                jobs = list(results.keys())
                for job in jobs:
                    if results[job].ready():
                        result = results[job].get()
                        lookup[result[0]] = result[1]  # results format is tuple of (key, data, init time)
                        del results[job]
                        q.task_done()

                sleep(0.05)


class WaitForProcess(Process, metaclass=Singleton):

    def start(self, **kwargs):
        semaphore_file_name = Process.__getattribute__(self, 'name')
        if semaphore.is_on(semaphore_file_name):
            semaphore.off(semaphore_file_name)
        # noinspection PyArgumentList
        super().start(**kwargs)
        while not semaphore.is_on(semaphore_file_name):
            sleep(0.1)
