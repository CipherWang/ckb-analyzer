#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import json
import requests
from queue import Queue
import time
from worker.block_storage import CKBBlockStore

# get block data from rpc
class CrawlThread(threading.Thread):
    def __init__(self, thread_id, job_queue, data_queue, rpc, target):
        threading.Thread.__init__(self)
        self.thread_id = thread_id  
        self.job_queue = job_queue
        self.data_queue = data_queue
        self.rpc = rpc
        if not target in ("block", "header"):
            raise ValueError("Error: target must be 'block' or 'header'!"); 
        self.target = target
        
    def run(self):
        self.running = True
        self.crawl_spider()
        self.running = False

    def crawl_spider(self):
        method = "get_block_by_number" if self.target == "block" else "get_header_by_number"
        client = requests.session()
        header = {"Content-Type": "application/json", "Connection": "keep-alive"}
        while True:
            if self.job_queue.empty():
                break
            else:
                block_number = self.job_queue.get()
                url = self.rpc
                d = {"id": self.thread_id,
                     "jsonrpc": "2.0",
                     "method": method,
                     "params": [str(hex(block_number))]
                }
                try:
                    r = client.post(
                        url, 
                        data = json.dumps(d), 
                        headers=header)
                    self.data_queue.put(r.text)
                    
                except Exception as e:
                    print('working failure', e)

# store block data to db
class StoreThread(threading.Thread):
    def __init__(self, data_queue, dump_path):
        threading.Thread.__init__(self)
        self.data_queue = data_queue
        self.more_data = True
        self.storager = CKBBlockStore()
        self.dump_path = dump_path

    def run(self):
        self.running = True
        self.storager.initdb()
        self.store()
        self.storager.dump_headerdb(self.dump_path)
        print("complete save")
        self.running = False

    def store(self):
        while self.more_data or (self.data_queue.qsize() > 0):
            if self.data_queue.qsize() > 0:
                block_header = json.loads(self.data_queue.get())['result']
                self.storager.store_header(block_header)

# Job manager
class CrawlJob():
    def __init__(self, block_range, rpc, target, crawl_number):
        super().__init__()
        self.block_range = block_range
        self.rpc = rpc
        self.target = target
        self.crawl_number = crawl_number
        self.threads = []
        self.data_queue = Queue()
        # initialize job queue
        self.job_queue = Queue()
        for block_number in range(block_range[0], block_range[1]):
            self.job_queue.put(block_number)
        
    def completed(self):
        for thread in self.threads:
            if thread.running == True:
                return False
        return True

    def percentage(self):
        return 100.0 - self.job_queue.qsize()*100.0 / (self.block_range[1]-self.block_range[0])
