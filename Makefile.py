import gcp
import rpyc

import googleapiclient.discovery
from google.oauth2 import service_account
from six.moves import input



# def create_master():
#     pass

# def create_kvstore():
#     pass



if __name__ == "__main__":
    # Intialize credentials:
    # scopes = ['https://www.googleapis.com/auth/cloud-platform']
    # sa_file = 'prudhvi-vajja-f62a24ed2484.json'
    # credentials = service_account.Credentials.from_service_account_file(sa_file, scopes=scopes)
    # compute = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
    compute = googleapiclient.discovery.build('compute', 'v1')
    project = 'prudhvi-vajja'
    zone = 'northamerica-northeast1-a'
    
    # Create Master:
    master_operation = gcp.create_instance(compute, project, zone, "master", "master.sh")
    gcp.wait_for_operation(compute, project, zone, master_operation['name'])
    
    master_ip = gcp.get_ipaddress(compute, project, zone, 'master')
    print(master_ip[0], master_ip[1], type(master_ip[1]))
    # gcp.delete_instance(compute, project, zone, 'master')
    
    # Create KVStore
    # kvstore_operation = gcp.create_instance(compute, project, zone, "kvstore", "kvstore.sh")
    # gcp.wait_for_operation(compute, project, zone, kvstore_operation['name'])
    
    # kvstore_ip = gcp.get_ipaddress(compute, project, zone, 'kvstore')
    try:
        # Connect to master:
        master_conn = rpyc.connect(master_ip[1], 8080, config={'allow_pickle':True, 'allow_public_attrs':True}).root
        
        # # Init_cluster
        # # master_conn.init_cluster()
        print(master_conn.create_delete_instance())
    except:
        # gcp.delete_instance(compute, project, zone, 'master')
        print("Instance is not created.")
    
    # Run MapReduce
    # master_conn.run_mapreduce()
    
    # Destroy cluster
    
    
    
    
    
    
    
    
    # Create KVStore:
    
    
    # instance_name = 'demo-instance'

    # create instance
    # operation = create_instance(compute, project, zone, instance_name)
    # wait_for_operation(compute, project, zone, operation['name'])

    #list instances
    # instances = list_instances(compute, project, zone)

    # print('Instances in project %s and zone %s:' % (project, zone))
    # for instance in instances:
    #     print(' - ' + instance['name'])

    #delete instance
    # operation = delete_instance(compute, project, zone, instance_name)
    # wait_for_operation(compute, project, zone, operation['name'])

    #list instances
    # instances = list_instances(compute, project, zone)

    # print('Instances in project %s and zone %s:' % (project, zone))
    # for instance in instances:
    #     print(' - ' + instance['name'])
        
    # int_ip, ext_ip = getIPAddresses(compute, project, zone, 'test-instance')
    # print(int_ip, ext_ip)
