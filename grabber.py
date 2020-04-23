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

from block_crawler import CrawlThread, CrawlJob, StoreThread


if __name__ == "__main__":
    # create job
    server = "http://192.168.1.65:8114"
    #server = "http://localhost:8114"
    job = CrawlJob(
        block_range = (0, 1500000), 
        rpc = server,
        target = "header",
        crawl_number = 200)

    # run the crawlers
    job.threads = []
    for thread_id in range(job.crawl_number):
        thread = CrawlThread(thread_id, job.job_queue, job.data_queue, job.rpc, job.target)
        thread.setDaemon(True)
        thread.start()
        job.threads.append(thread)

    # run the store thread
    sthread = StoreThread(job.data_queue, "D:/sql.db")
    sthread.setDaemon(True)
    sthread.start()

    # monitor the jobs
    tm_start = time.time()
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    try:
        # wait for grab complete
        ticker = 0
        estimation = "."
        while not job.completed():
            percentage = job.percentage()
            ticker = ticker + 1
            if ticker%25 == 0 and percentage > 0:
                passed = int(time.time() - tm_start)
                remains = int(passed / percentage * (100 - percentage));
                m, s = divmod(remains, 60)
                h, m = divmod(m, 60)
                remains = "%dh %dm %ds" % (h, m, s)
                estimation = ". %ds passed, %s remains." % (passed, remains)
            print("grab completed at: %.01f%%%s" % (percentage, estimation), end="\r")
            sys.stdout.flush()
            time.sleep(0.04) # fps = 25

        # wait for store complete
        sthread.more_data = False
        print("\nstoring data...")

    except Exception as e:
        print("Actions cancelled by user.")
        exit(-1)

    while sthread.running:
        time.sleep(0.1)
    print("All crawling jobs have been completed!")