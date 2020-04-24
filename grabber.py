#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
    dump specific data from ckb node by rpc service
'''

import threading
import time
import json
import requests
from queue import Queue
import signal
import sys

from worker.block_crawler import CrawlThread, CrawlJob, StoreThread

class MonitorThread(threading.Thread):
    def __init__(self, crawl_job):
        threading.Thread.__init__(self)
        self.job = crawl_job
        self.exit = False

    def to_hms(self, t):
        m, s = divmod(t, 60)
        h, m = divmod(m, 60)
        return "%dh %dm %ds" % (h, m, s)

    def force_stop(self):
        self.exit = True

    def run(self):
        # wait for grab complete
        estimation = "."
        tm_start = time.time()
        tm_last = 0
        while not self.exit:
            percentage = self.job.percentage()
            passed = time.time() - tm_start
            if time.time() - tm_last > 1 and percentage > 0:
                tm_last = time.time()
                remains = int(passed / percentage * (100. - percentage));
                passed = self.to_hms(int(passed))
                remains = self.to_hms(remains)
                estimation = ". %s passed, %s remains." % (passed, remains)
            print("grab completed at: %.01f%%%s" % (percentage, estimation), end="\r")
            #sys.stdout.flush()
            if percentage >= 100:
                break

   

if __name__ == "__main__":
    # setup break handler
    def signal_handler(sig, frame):
        mthread.force_stop()
        print("\nUser pressed break.".ljust(32))
        time.sleep(1)
        sys.exit(-1)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    server = "http://192.168.1.65:8114"
    #server = "http://localhost:8114"
    dbsave = "sample.sqlite3"

    # create job
    print("starting jobs.")
    sys.stdout.flush()
    job = CrawlJob(
        block_range = (1490000, 1500000), 
        rpc = server,
        target = "header",
        crawl_number = 200)

    # run the monitor thread
    mthread = MonitorThread(job)
    mthread.setDaemon(True)
    mthread.start()

    # run the crawlers
    job.threads = []
    for thread_id in range(job.crawl_number):
        thread = CrawlThread(thread_id, job.job_queue, job.data_queue, job.rpc, job.target)
        job.threads.append(thread)
        thread.setDaemon(True)
        thread.start()

    # run the store thread
    sthread = StoreThread(job.data_queue, dbsave)
    sthread.setDaemon(True)
    sthread.start()
    
    # wait for store complete
    sthread.more_data = False
    while sthread.running:
        time.sleep(0.1)
        
    print("All crawling jobs have been completed!")