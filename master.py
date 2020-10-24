#!/usr/bin/python3

# Import Socket packages
import rpyc
from rpyc.utils.server import ThreadedServer
from configparser import ConfigParser  # module to read config file
import threading
import os
import glob
import time
import sys
from multiprocessing import Process
import socket


import googleapiclient.discovery
from google.oauth2 import service_account
from six.moves import input
import logging as l
import gcp

# # Logging File:
log_file = "master_log.log"
if not os.path.exists(log_file):
    print("Creating Log File if it doesn't exists.")
    f = open(log_file, 'x')
    f.close()
l.basicConfig(filename=log_file, filemode="a",
              format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s", level=l.INFO)


class Master(rpyc.Service):
    def __init__(self):
        """
        Intialize any required variables:
        """
        global scopes
        global sa_file
        global credentials
        global compute
        global project
        global zone

    def on_connect(self, conn):
        # print(f"{conn} got Connected......!")
        pass

    def on_disconnect(self, conn):
        # print(f"{conn} got DisConnected......!")
        pass

    def exposed_create_delete_instance(self):
        try:
            sample_opeartion = gcp.create_instance(
                compute, project, zone, "demo-instance", "test.sh")
            gcp.wait_for_operation(
                compute, project, zone, sample_opeartion['name'])
            print(" Gcp is connected")
            # gcp.delete_instance(compute, project, zone, "demo-instance")
            return "Sucessuly created an instance"
        except:
            return "Instance was not created"

    def exposed_initcluster(self, map_count, red_count, filename, kv_ip, kv_port, func):
        l.info("Client has started init_cluster..")
        
        # Intialize Mappers:
        self.mappers = []
        for i in range(map_count):
            self.mappers.append('mapper'+str(i))
            
        # Intialize Reducers:
        self.reducers = []
        for i in range(red_count):
            self.reducers.append('reducer'+str(i))
        
        # Connect to KVStore:
        while True:
            try:
                kvstore_conn = rpyc.connect(kv_ip, kv_port, config={'allow_pickle': True, 'allow_public_attrs': True,
                                                                    'sync_request_timeout': 240}).root
                l.info("Master is connected to Kvstore.")
                break
            except:
                continue
            
        # Intialize empty files for mappers and reducers and set params in kvstore:
        kvstore_conn.params(map_count, red_count, func)
        # Divide data accoring to mappers:
        try:
            if func == 'wordcount':
                f = open(filename, 'r')
                size = os.stat(filename).st_size
                for i in range(map_count):
                    data = f.readlines(size//map_count)
                    tmp = 'mapper' + str(i) + '.txt'
                    kvstore_conn.save_to_file(data, tmp)
                f.close()
                l.info("Data is splited according to mappers and stored at kvstore.")
            elif func == 'invertindex':
                files = glob.glob1('invertindex', "*.txt")
                for i, f in enumerate(files):
                    self.mapper['mapper'+str(i)] = f
        except:
            l.error("Unable to split data as per requirment.")

        # self.start_mappers(map_count)

        # self.start_reducers(red_count)

    def exposed_run_mapreduce(self, map_count, red_count, kv_ip, kv_port, func):
        # Start Mappers
        l.info("Starting Mappers")
        self.start_mappers(map_count, kv_ip, kv_port)
        
        l.info("Waiting for mappers to start")
        time.sleep(30) # Waiting for mappers to start....
        
        if len(self.map_ips) != len(self.mappers):
            l.error("Required number of mappers are not created.")
        
        
        l.info("Waiting for master to connect to mappers Ips")
        # Connect to mappers
        self.mapper_conn = [] 
        for mapper,ip in zip(self.mappers, self.map_ips):
            while True:
                try:
                    conn = rpyc.connect(ip, 8080, config={'allow_pickle': True, 'allow_public_attrs': True,
                                                                    'sync_request_timeout': 240}).root
                    l.info("connected to" + mapper + "at ip" + ip)
                    self.mapper_conn.append(conn)
                    break
                except:
                    continue
        l.info("All Mappers are connected to master")
        
        # Assign Tasks to mappers
        mappers = []
        for i, mapper in enumerate(self.mapper_conn):
            filename = 'mapper' + str(i) + '.txt'
            mappers.append(rpyc.async_(mapper.mapper)(func, filename, kv_ip, kv_port))
            mappers[i].set_expiry(None)

        # wait till all mappers completes its assigned task
        for mapper in mappers:
            while not mapper.ready:
                continue
        l.info('Mappers have completed their assigned task...')
        
        l.info("Destroy Mappers")
        # Destroy Mappers:
        self.destroy_instance(self.mappers)
        
        # Fault Tolerance
        # self.fault_tolerance()

        # Start Reducers
        l.info("Starting Reducers")
        self.start_reducers(red_count, kv_ip, kv_port)
        
        
        l.info("Waiting for reducers to start")
        time.sleep(30) # Waiting for mappers to start....
        
        if len(self.red_ips) != len(self.reducers):
            l.error("Required number of reducers are not created.")
        
        # Connect to reducers
        self.reducer_conn = [] 
        for reducer,ip in zip(self.reducers, self.red_ips):
            while True:
                try:
                    conn = rpyc.connect(ip, 8080, config={'allow_pickle': True, 'allow_public_attrs': True,
                                                                    'sync_request_timeout': 240}).root
                    l.info("connected to" + reducer + "at ip" + ip)
                    self.reducer_conn.append(conn)
                    break
                except:
                    continue
        l.info("All Reducers are connected to Master")
        
        # Assign Tasks to reducers:
        reducers = []
        for i, reducer in enumerate(self.reducer_conn):
            filename = 'reducer' + str(i) + '.txt'
            reducers.append(rpyc.async_(reducer.reducer)(func, filename, kv_ip, kv_port))
            reducers[i].set_expiry(None)

        # wait till all reducers completes its assigned task
        for reducer in reducers:
            while not reducer.ready:
                continue
            
        l.info('Reducers have completed their assigned task...')
        
        l.info("Destroy reducers")
        # Destroy Reducers:
        self.destroy_instance(self.reducers)
        

    def start_mappers(self, map_count, kv_ip, kv_port):
        self.map_ips = [] # Ip address of mappers
        for mapper in self.mappers:
            try:
                mapper_operation = gcp.create_instance(compute, project, zone, mapper, "mapper.sh")
                gcp.wait_for_operation(compute, project, zone, mapper_operation['name'])
                map_ip = gcp.get_ipaddress(compute, project, zone, mapper)
                self.map_ips.append(map_ip[0])
                l.info(mapper + "Instance is created sucessfully.")
            except:
                l.error("unable to create" + mapper)
            

    def start_reducers(self, red_count, kv_ip, kv_port):
        self.red_ips = []
        for reducer in self.reducers:
            try:
                reducer_operation = gcp.create_instance(compute, project, zone, reducer, "reducer.sh")
                gcp.wait_for_operation(compute, project, zone, reducer_operation['name'])
                red_ip = gcp.get_ipaddress(compute, project, zone, "reducer")
                self.red_ips.append(red_ip[0])
                l.info(reducer + "Instance is created sucessfully.")
            except:
                l.error("unable to create" + reducer)
                
    def destroy_instance(self, list_of_instance):
        for instance in list_of_instance:
            gcp.delete_instance(compute, project, zone, instance)

    def split_data(self, filename, num_map, func):
        try:
            if func == 'wordcount':
                f = open(filename, 'r')
                size = os.stat(filename).st_size
                for i in range(self.num_map):
                    data = f.readlines(size//self.num_map)
                    tmp = 'mapper' + str(i) + '.txt'
                    t = open(tmp, "w")
                    t.write("".join(data))
                    t.close()
                f.close()
                print("Data is splited")
                return True
            elif func == 'invertindex':
                files = glob.glob1('invertindex', "*.txt")
                for i, f in enumerate(files):
                    self.mapper['mapper'+str(i)] = f
                print(self.mapper)
        except:
            l.error("Unable to split data as per requirment.")

    def fault_tolerance(self):
        pass

    def exposed_status(self, status):
        print(status)

    # def exposed_connkv(self, ip, port):
    #     # while True:
    #     try:
    #         kvstore_conn = rpyc.connect(ip, port, config={
    #                                     'allow_pickle': True, 'allow_public_attrs': True}).root
    #         l.info("Master is connected to Kvstore.")
    #         tmp = kvstore_conn.ack("Hey!")
    #         return tmp + "Connected to KV Store."
    #     except:
    #         return "Not connected to KV."

    def exposed_ack(self, var):
        return var
        # for mapp in self.mapper_list:


if __name__ == "__main__":
    scopes = ['https://www.googleapis.com/auth/cloud-platform']
    sa_file = 'prudhvi-vajja-f62a24ed2484.json'
    credentials = service_account.Credentials.from_service_account_file(
        sa_file, scopes=scopes)
    compute = googleapiclient.discovery.build(
        'compute', 'v1', credentials=credentials)
    project = 'prudhvi-vajja'
    # zone = 'northamerica-northeast1-a'
    zone = 'us-central1-b'

    port = 8080
    try:
        rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
        rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
        t = ThreadedServer(Master, port=port,
                           protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)

        # l.info(f"Starting master at port = {port}")
        try:
            t.start()
        except Exception:
            t.stop()
    except:
        print('Error.!')
        # l.error("Unable to start Master. Check if the given port is available.")
        # sys.exit(0)

    # start = time.time()
    # p = False
    # # Read aruguments from config file __init__cluster():
    # try:
    #     parser = ConfigParser()
    #     parser.read('config.ini')
    #     master_port = int(parser['master']['port'])
    #     kvstore_port = int(parser['kvserver']['port'])
    #     filename = parser['inputfile']['filename']
    #     function = parser['master']['function']

    #     if function == 'wordcount':
    #         num_map = int(parser['master']['num_map'])
    #         num_red = int(parser['master']['num_red'])
    #     elif function == 'invertindex':
    #         filecount = len(glob.glob1(filename,"*.txt"))
    #         num_map = filecount
    #         num_red = filecount
    #     else:
    #         print(f"This {function} is not implemented yet 🤺 kill the program.")
    #         l.warning(f"This {function} is not implemented yet 🤺 kill the program.")

    #     l.info(f"[MapReduce for {function} is getting started with {num_map} Mappers {num_red} Reducers.]")

    #     # Connecting to KV store....
    #     print("Connecting KV Store..")
    #     l.info("Connecting KV Store..")
    #     kv_conn = rpyc.connect("localhost", kvstore_port, config={'allow_pickle':True, 'allow_public_attrs':True}).root
    #     connect_to_kvstore(filename, num_map, function)

    #     print("Start Master Server")
    #     l.info("Start Master Server.")
    #     p = Process(target=start_master, args=(master_port,))
    #     p.start()

    #     print(f"Starting {num_map} mappers as requested.. wait for some time")
    #     l.info(f"Starting {num_map} mappers as requested.. wait for some time")

    #     start_mappers(num_map, kvstore_port, master_port, function)

    #     print("Mapping task in done.. Start Reducing")
    #     l.info("Mapping task in done.. Start Reducing")

    #     start_reducers(num_red, kvstore_port, master_port, function)

    #     print("MapReduce task is Done!")
    #     l.info("MapReduce task is Done!")

    #     destroy_cluster() # destroy Cluster

    # except:
    #     l.error("Unable to start the cluster properly check the port numbers properly")
    #     destroy_cluster()
