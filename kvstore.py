#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
@author: Prudhvi Vajja
"""

import rpyc
import os, platform, ctypes, signal
import glob
import hashlib
from rpyc.utils.server import ThreadedServer
from configparser import ConfigParser # module to read config file



class KV_store(rpyc.Service):
    
    def __init__(self):
        pass
        # global filename
        # global num_red
        # global num_map
        # self.num_red = num_red
        # self.num_map = num_map
        # self.folder = filename
        # global function
        
        # if function == 'invertindex':
        #     self.mapper = {}
        #     self.files = glob.glob1(self.folder,"*.txt")
        #     for i,f in enumerate(self.files):
        #         self.mapper['mapper'+str(i)] = f
        # print(self.mapper)

    def on_connect(self, conn):
        # print(f"{conn} got Connected......!") # What to do when connected
        pass

    def on_disconnect(self, conn):
        # print(f"{conn} got DisConnected......!") # What to do when disconnected
        pass
    
    def exposed_save_to_file(self, data, filename):
        t = open(filename, "w")
        t.write("".join(data))
        t.close()
    
    # def exposed_split(self, filename, num_map, func):
    #     try:
    #         if func == 'wordcount':
    #             f = open(filename, 'r')
    #             size = os.stat(filename).st_size
    #             for i in range(self.num_map):
    #                 data = f.readlines(size//self.num_map)
    #                 tmp = 'mapper' + str(i) + '.txt'
    #                 t = open(tmp, "w")
    #                 t.write("".join(data))
    #                 t.close()
    #             f.close()
    #             print("Data is splited")
    #             return True
    #         elif func == 'invertindex':
    #             files = glob.glob1('invertindex',"*.txt")
    #             for i,f in enumerate(files):
    #                 self.mapper['mapper'+str(i)] = f
    #             print(self.mapper)
    #     except:
    #         l.error("Unable to split data as per requirment.")
            
        
    # def exposed_get_files(self):
    #     return glob.glob1(self.folder,"*.txt")
    
        
    # def exposed_map_data(self, i, func):
    #     try:
    #         if func == 'wordcount':
    #             extension = '.txt'
    #             filename = 'mapper' + str(i) + extension
    #             with open(filename, 'r') as f:
    #                 data = f.read()
    #                 return data
    #         elif func == 'invertindex':
    #             fi = self.mapper['mapper' + str(i)]
    #             path = self.folder + '/' + fi
    #             with open(path, 'r') as f:
    #                 data = f.read()
    #                 return data,fi
    #     except:
    #         l.error(f"data for mapper {i} is not read")
        
    # def exposed_red_data(self, i):
    #     try:
    #         extension = '.txt'
    #         tmp = 'reducer' + str(i) + extension          # files = glob.glob(f'*.{extension}')
    #         with open(tmp, 'r') as f:
    #             data = f.read()
    #             return data
    #     except:
    #         l.error(f"Data for reducer {i} is not ready")

    # # def exposed_status(self, status):
    # #     print(status)
    #     # l.info(status)

    # def exposed_create_files(self, num_map, num_red, func):
    #     try:
    #         self.num_red = num_red
    #         self.num_map = num_map
            
    #         for i in range(self.num_red):
    #             tmp = 'reducer' + str(i) + '.txt'
    #             if os.path.exists(tmp):
    #                 os.remove(tmp)
    #             f = open(tmp, "x")
    #             f.close()
                
    #         if func == 'wordcount':
    #             for i in range(self.num_map):
    #                 tmp = 'mapper' + str(i) + '.txt'
    #                 if os.path.exists(tmp):
    #                     os.remove(tmp)
    #                 f = open(tmp, "x")
    #                 f.close()
            
    #         tmp = func + '.txt'
    #         if os.path.exists(tmp):
    #             os.remove(tmp)
    #             f = open(tmp, "x")
    #             f.close()
    #     except:
    #         l.error("hasn't created empty files for I/O operations Should not be a problem.")
            
    # def exposed_i_set(self, word, cnt, f=""):
    #     try:
    #         hash_map = int.from_bytes(hashlib.md5(word.encode()).digest(), 'big') % self.num_red
    #         tmp = 'reducer' + str(hash_map) + '.txt'
    #         output = open(tmp, 'a')
    #         output.write(word + ',' + str(cnt) + ',' + f + '\n')
    #         output.close()
    #     except:
    #         l.error(f"mapper {i} is not able append values.")

    # def exposed_set(self, word, cnt):
    #     try:
    #         hash_map = int.from_bytes(hashlib.md5(word.encode()).digest(), 'big') % self.num_red
    #         tmp = 'reducer' + str(hash_map) + '.txt'
    #         output = open(tmp, 'a')
    #         output.write(word + ',' + str(cnt) + '\n')
    #         output.close()
    #     except:
    #         l.error(f"mapper {i} is not able append values.")
        
    # def exposed_final_i_set(self, word, cnt):
    #     try:
    #         output = open(tmp, 'a')
    #         output.write(word + ',' + str(cnt) + '\n')
    #         output.close()
    #     except:
    #         l.error("Error in writing output file.")   
        
    # def exposed_final_set(self, word, cnt, func):
    #     try:
    #         tmp = func + '.txt'
    #         if func == 'wordcount':
    #             output = open(tmp, 'a')
    #             output.write(word + ',' + str(cnt) + '\n')
    #             output.close()
    #         elif func == 'invertindex':
    #             output = open(tmp, 'a')
    #             output.write(word + '=>' + str(cnt) + '\n')
    #     except:
    #         l.error("Error in writing output file.")   
        
    # def exposed_stop(self):
    #     pid = os.getpid()

    #     if platform.system() == 'Windows':
    #         PROCESS_TERMINATE = 1
    #         handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
    #         ctypes.windll.kernel32.TerminateProcess(handle, -1)
    #         ctypes.windll.kernel32.CloseHandle(handle)
    #     else:
    #         os.kill(pid, signal.SIGTERM)

    # def exposed_ack(self):
    #     return True


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
    t = ThreadedServer(KV_store, port=8080,
                    protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)        
    try:
        t.start()
    except Exception:
        t.stop()