# Import Modules
import argparse
import os
import time

import googleapiclient.discovery
from google.oauth2 import service_account
from six.moves import input


#source: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/compute/api/create_instance.py

# [START create_instance]
def create_instance(compute, project, zone, name):
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        project='debian-cloud', family='debian-9').execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'startup-script.sh'), 'r').read()
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }, {
                'key': 'url',
                'value': image_url
            }, {
                'key': 'text',
                'value': image_caption
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()
# [END create_instance]

# [START list_instances]
def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None
# [END list_instances]

# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
# [END delete_instance]

# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)
# [END wait_for_operation]


# [START getIPAddresses]
def get_ipaddress(compute, project, zone, name):
    instance = compute.instances().get(project=project, zone=zone, instance=name).execute()
    external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
    internal_ip = instance['networkInterfaces'][0]['networkIP']
    return internal_ip, external_ip
# [END getIPAddresses]


if __name__ == '__main__':
    scopes = ['https://www.googleapis.com/auth/cloud-platform']
    sa_file = 'prudhvi-vajja-f62a24ed2484.json'
    credentials = service_account.Credentials.from_service_account_file(sa_file, scopes=scopes)
    compute = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
    project = 'prudhvi-vajja'
    zone = 'northamerica-northeast1-a'
    instance_name = 'demo-instance'
    
    # create instance
    # operation = create_instance(compute, project, zone, instance_name)
    # wait_for_operation(compute, project, zone, operation['name'])
    
    #list instances
    instances = list_instances(compute, project, zone)
    
    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])
    
    #delete instance
    # operation = delete_instance(compute, project, zone, instance_name)
    # wait_for_operation(compute, project, zone, operation['name'])
    
    #list instances
    instances = list_instances(compute, project, zone)
    
    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])
        
    int_ip, ext_ip = getIPAddresses(compute, project, zone, 'test-instance')
    print(int_ip, ext_ip)