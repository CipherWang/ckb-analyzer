#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from time import time

class BlockHeaderParser():
    def work(self, record):
        raise "Please your implement specific code."

    def export(self):
        raise "Please your implement specific code."

    def analyze(self):
        pass

    def show(self):
        pass

class BlockWalker():
    def __init__(self, db_file):
        super().__init__()
        self.conn = sqlite3.connect(db_file)
        assert(self.conn)
        self.conn.row_factory = self.dict_factory
        self.parsers = []

    def dict_factory(self, cursor, row):  
        d = {}  
        for idx, col in enumerate(cursor.description):  
            d[col[0]] = row[idx]  
        return d

    def register_parser(self, parser):
        assert(isinstance(parser, BlockHeaderParser))
        self.parsers.append(parser)

    def walk_through(self, limit = None):
        # get the block range in db
        cursor = self.conn.cursor()
        cursor.execute("SELECT MIN(number) as min, MAX(number) as max FROM tb_header")
        bond = cursor.fetchone()
        if limit:
            bond['min'] = max(bond['min'], limit[0])
            bond['max'] = min(bond['max'], limit[1])
        cursor.execute("SELECT * FROM tb_header WHERE number >= %d AND number <= %d ORDER BY number;" % (bond['min'], bond['max']) )
        record = cursor.fetchone()
        # process single record
        lcont = 1
        last_time = time()
        while record:
            if time() - last_time > 0.1:
                print("step 1/2: data reading %d/%d." % (lcont, bond['max']-bond['min']), end="\r")
                last_time = time()
            lcont += 1
            for parser in self.parsers:
                parser.work(record)
            record = cursor.fetchone()
        # after single record process, do calculation
        print("\nstep 2/2: analyzing...")
        for parser in self.parsers:
            parser.analyze()
