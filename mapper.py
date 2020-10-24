#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
@author: Prudhvi Vajja
"""

import rpyc
from rpyc.utils.server import ThreadedServer
import hashlib
import re
import logging as l
import time

# # Logging File:
log_file = "mapper_log.log"
if not os.path.exists(log_file):
    print("Creating Log File if it doesn't exists.")
    f = open(log_file, 'x')
    f.close()
l.basicConfig(filename=log_file, filemode="a",
              format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s", level=l.INFO)

class Mapper(rpyc.Service):
    
    def __init__(self):
        pass

    def on_connect(self, conn):
        # print(f"{conn} got Connected......!") # What to do when connected
        pass

    def on_disconnect(self, conn):
        # print(f"{conn} got DisConnected......!") # What to do when disconnected
        pass
    
    def exposed_mapper(self, func, filename, kv_ip, kv_port):
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
            data = kv_conn.get_map_data(func, filename)
            l.info(data)
            l.info("Data is loaded.")
        except:
            l.info("Unable to load data.")
            # return "Unable to load Data."
            
        try:
            if func == 'wordcount':
                words = (re.sub('[^A-Za-z0-9]+',' ', data)).split()
                
                for word in words:
                    word = word.lower()
                    kv_conn.set(word, 1)
        except:
            l.info("Error in mapper function for word count")
            
        if func == 'invertindex':
            try:
                words = (re.sub('[^A-Za-z0-9]+',' ', data)).split()
            
                for word in words:
                    word = word.lower()
                    kv_conn.i_set(word, 1, func)
            except:
                l.info("Unable to run invert index")
        
    def exposed_ack(self, var):
        return var


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
    t = ThreadedServer(Mapper, port=8080,
                    protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)        
    try:
        t.start()
    except Exception:
        t.stop()