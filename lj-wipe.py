#!/usr/bin/env python
import time, traceback, uuid

from lj import lj
class LJClient:
    def __init__(self, login, password):
        self.login = login
        self.password = password

    def get_lj(self):
        ljclient = lj.LJServer("lj-wipe/1.0", uuid.uuid4().urn)
        ljclient.login(self.login, self.password)
        return ljclient

import Queue
from multiprocessing import Process, Pool, JoinableQueue, Lock, Event, Value

class TaskControl:
    def __init__(self, cls_worker, count, *args, **kwargs):
        self.queue = JoinableQueue()
	self.stopped = Event()
        self.count_processed = Value('i', 0)

        self.processes = [cls_worker(self, *args) for _ in range(count)]
        map(Process.start, self.processes)

    def is_active(self):
        return not self.stopped.is_set()

    def stop(self):
        self.stopped.set()
	for p in self.processes:
		p.join()

    def send_chunk(self, items):
        map(self.queue.put, items)
        print '--- waiting for queue to complete'
        self.queue.join()

    def get(self):
        while self.is_active():
            try:
                yield self.queue.get(timeout = 1)
            except Queue.Empty:
                pass

    def tick(self):
        self.queue.task_done()
        self.count_processed.value += 1
        if not self.count_processed.value % 20:
            print '%d items processed' % self.count_processed.value

    def get_stats(self):
        return (self.count_processed.value, self.queue.qsize())

class CleanWorker(Process):
    def __init__(self, control, client):
        self.control = control
        self.client = client
        Process.__init__(self)

    def run(self):
        ljclient = self.client.get_lj()

        for data in self.control.get():
            try:
                    ljclient.delevent(data['itemid']) # rate limit may raise here
            except Exception, e:
                    print "Exception while deleting entry %s" % data, e

            self.control.tick()

if __name__ == '__main__':
        import argparse, sys
        from datetime import datetime

        parser = argparse.ArgumentParser()
        parser.add_argument("--login", help="LJ login")
        parser.add_argument("--password", help="LJ password")
        parser.add_argument("--process", type=int, help="num parallel threads", default = 10)
        parser.add_argument("--chunk", type=int, help="number of entries to fetch at once", default = 200)
        parser.add_argument("--before", type=str, help="first date to keep (e.g. 2017-01-01, will delete everything before")

        args = parser.parse_args()

        if not args.before:
            parser.print_help()
            sys.exit()

        args.before =  datetime.strptime(args.before, '%Y-%m-%d')

        ljc = LJClient(args.login, args.password)
        controller = TaskControl(CleanWorker, args.process, ljc)

	ljclient = ljc.get_lj()
	print 'main thread logged in'
        
        while True:
                items = ljclient.getevents_lastn(n = args.chunk, before = args.before)['events']
                print '--- fetched %d items' % len(items)
                if not items:
                    break
                controller.send_chunk(items)
            
        controller.stop()
