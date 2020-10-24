#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
@author: Prudhvi Vajja
"""

import rpyc
from rpyc.utils.server import ThreadedServer
from collections import defaultdict
import time


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
        except:
            print("Unable to load data.")
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
                print("Error in mapper function for word count")

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
                print("Unable to run invert index")

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


def reducer(i, kv_port, m_port, function):

    red_kv_conn = rpyc.connect("localhost", kv_port, config={
                               'allow_pickle': True, 'allow_public_attrs': True}).root
    red_mas_conn = rpyc.connect("localhost", m_port, config={
                                'allow_pickle': True, 'allow_public_attrs': True}).root

    red_mas_conn.status(f"reducer {i} is connected to the Master")
    red_kv_conn.status(f"reducer {i} is connected to the KV store.")
    if function == 'wordcount':
        data = red_kv_conn.red_data(i)
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
            red_kv_conn.final_set(key, value, function)

        red_mas_conn.status(f"reducer {i} got completed")
        red_kv_conn.status(f"reducer {i} sent all the data to kv store")

    elif function == 'invertindex':
        data = red_kv_conn.red_data(i)

        red_mas_conn.status(f"reducer {i} got completed")
        red_kv_conn.status(f"reducer {i} sent all the data to kv store")

    else:
        red_mas_conn.status(f"{function} is not implemented in the Reducer")
