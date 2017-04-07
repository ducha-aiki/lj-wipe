#!/usr/bin/env python

# (c) 2017 Mykola Grechukh <schroedinger@mykola.space>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

    def is_alive(self):
        alive = filter(bool, map(Process.is_alive, self.processes))
	print '---- %d child processes are still alive' % len(alive)
        return alive

    def stop(self):
        self.stopped.set()
        self.queue.close()
        print '-- waiting for processes to finish'
	map(Process.join, self.processes)
        self.queue.cancel_join_thread()

    def send_chunk(self, items):
        map(self.queue.put, items)
        print '--- waiting for queue to complete'
        while self.get_stats()[1] and self.is_alive():
            time.sleep(1)

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
	time.sleep(0.5)

    def get_stats(self):
        stats = self.count_processed.value, self.queue.qsize()
        print '--- %d items processed, %d queued' % stats
        return stats

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
                    return

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
        
        while controller.is_alive():
                items = ljclient.getevents_lastn(n = args.chunk, before = args.before)['events']
                print '--- fetched %d items' % len(items)
                if not items:
                    break
                controller.send_chunk(items)
        print 'stopping'
        controller.stop()
        if items:
            print 'still left unprocessed items'
            sys.exit(1)
        else:
            print 'all done'
