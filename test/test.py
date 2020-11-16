#!/usr/bin/python3
import gcp
import googleapiclient.discovery
from google.oauth2 import service_account

compute = googleapiclient.discovery.build('compute', 'v1')
project = 'prudhvi-vajja'
# zone = 'northamerica-northeast1-b'
zone = 'us-central1-b'
gcp.delete_instance(compute, project, zone, 'master')