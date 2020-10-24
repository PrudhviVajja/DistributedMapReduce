#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
@author: Prudhvi Vajja
"""

import rpyc
from rpyc.utils.server import ThreadedServer
from collections import defaultdict
import time
import logging as l

# # Logging File:
log_file = "reducer_log.log"
if not os.path.exists(log_file):
    print("Creating Log File if it doesn't exists.")
    f = open(log_file, 'x')
    f.close()
l.basicConfig(filename=log_file, filemode="a",
              format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s", level=l.INFO)


class Reducer(rpyc.Service):

    def __init__(self):
        pass

    def on_connect(self, conn):
        # print(f"{conn} got Connected......!") # What to do when connected
        pass

    def on_disconnect(self, conn):
        # print(f"{conn} got DisConnected......!") # What to do when disconnected
        pass

    def exposed_reducer(self, func, filename, kv_ip, kv_port):
        # Connect to Kvstore
        while True:
            try:
                kv_conn = rpyc.connect(kv_ip, kv_port, config={'allow_pickle': True, 'allow_public_attrs': True,
                                                               'sync_request_timeout': 240}).root
                break
            except:
                continue

        # Get Data from kv store:
        try:
            data = kv_conn.get_red_data(func, filename)
            l.info(data)
            l.info("data is received.")
        except:
            l.info("Unable to load data.")
            # return "Unable to load Data."

        if func == 'wordcount':
            try:
                words = data.split()
                in_memory = {}
                for word in words:
                    w = word.split(',')[0]
                    if w in in_memory:
                        in_memory[w] += 1
                    else:
                        in_memory[w] = 1

                # print(in_memory)
                for key, value in in_memory.items():
                    kv_conn.final_set(key, value, function)
            except:
                l.info("Error in mapper function for word count")

        if func == 'invertindex':
            try:
                words = data.split()
                files = kv_conn.get_files()  # Assign Folder

                in_memory = {}
                for word in words:
                    w = word.split(',')
                    if w[0] in in_memory:
                        in_memory[w[0]][w[2]] += 1
                    else:
                        in_memory[w[0]] = {f: 0 if f !=
                                           w[2] else 1 for f in files}

                # print(in_memory)
                for key, value in in_memory.items():
                    kv_conn.final_set(key, value, function)
            except:
                l.info("Unable to run invert index")

    def exposed_ack(self, var):
        return var


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
    t = ThreadedServer(Reducer, port=8080,
                       protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        t.start()
    except Exception:
        t.stop()
