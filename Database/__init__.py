__author__ = 'Mark'

from threading import Thread, activeCount
from Queue import Queue, Empty
import time
import sys
import logging
from random import uniform
import unittest
import Config


class DBInterface(Thread):

    STOP_ALL_THREADS = False

    def __init__(self, start_queue, end_queue, fileoutput=False):
        Thread.__init__(self)

        self.start_queue = start_queue
        self.end_queue = end_queue

        self.fetch_next = False
        self.terminate = False

        self.logger = logging.getLogger(self.__class__.__name__)

        self.all_queues_populated = False

        self.fout = fileoutput

        self.out_file = open(Config.FOUT_FILE, 'w+')

        self.out_file.write('%s\n' % '\t'.join(['sku', 'price', 'status', 'option',
                                                'store', 'shipping', 'date_modified']))

        self.all_queues_populated = True

    def run(self):
        while True:
            try:
                x = self.end_queue.get(block=False)
                self.logger.info('CompletedTask: %s' % str(repr(x)))
                if self.fout:
                    self.out_file.write('%s\n' % '\t'.join((str(y) for y in x.for_db())))
                else:
                    print x.to_json()
                self.end_queue.task_done()
            except Empty:
                # If the queue is empty and the stop all flags thread is raised, it means that no more
                # stores are currently writing to the queue, so we can safely shut down the thread
                # after it writes everything from the queue into the file.
                if DBInterface.STOP_ALL_THREADS and activeCount() < 3:
                    logging.info('Shutting Down...')
                    return
                # If the queue is empty but the flag is not raised, continue because it means that threads
                # are still actively writing to the queue.
                else:
                    continue
            except Exception as e:
                self.logger.exception(e)
                with self.end_queue.mutex:
                    self.end_queue.queue.clear()
                DBInterface.STOP_ALL_THREADS = True
                return
