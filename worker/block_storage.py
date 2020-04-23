#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3

class CKBBlockStore():
    def initdb(self):
        self.mem_conn = sqlite3.connect(":memory:")
        self.headerDB = self.mem_conn.cursor()
        self.headerDB.execute('''CREATE TABLE tb_header (
            number INTEGER, timestamp INTEGER,
            dao_C INTEGER, dao_AR INTEGER, dao_S INTEGER, dao_U INTEGER,
            epoch_number INTEGER, epoch_index INTEGER, epoch_length INTEGER)
        ''')
        self.headerDB.execute("CREATE UNIQUE INDEX blknum ON tb_header (number);")

    
    def dump_headerdb(self, dbpath):
        if not self.headerDB:
            print("error header db.")
            return
        bck = sqlite3.connect(dbpath)
        with bck:
            self.mem_conn.backup(bck)
        bck.close()

    def load_headerdb(self, dbpath):
        f = open(dbpath, 'w')
        for line in self.mem_conn.iterdump():
            data = line + '\n'
            data = data.encode("utf-8")
            f.write(data)
        f.close()

    def store_header(self, header):
        dao = header['dao'][2:]
        dao_big = "".join(dao[i:i+2] for i in range(62, -1, -2))
        (U, S, AR, C) = (int(dao_big[i:i+16], 16) for i in range(0, 64, 16))
        number = int(header['number'], 16)
        timestamp = int(header['timestamp'], 16)
        epoch = header['epoch'][2:].rjust(16, "0")
        (epoch_number, epoch_index, epoch_length) = \
            (int(epoch[-6:],16), int(epoch[-10:-6],16),  0 if epoch[2:-10]=='' else int(epoch[:-10],16))
        self.headerDB.execute("INSERT INTO tb_header VALUES (%d,%d,%d,%d,%d,%d,%d,%d,%d);" % (number, timestamp, C, AR, S, U, epoch_number, epoch_index, epoch_length))
        self.mem_conn.commit()

    def store_block(self, block):
        raise Exception("Not implemented.")

    def clear(self):
        if not self.headerDB:
            self.mem_conn.close()


        