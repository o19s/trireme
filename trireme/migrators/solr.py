from invoke import task
import requests
import os
from config import solr_url


def upload_file(local_path, remote_path):
    fd = open(local_path, 'r')
    response = requests.post(remote_path, data=fd)
    fd.close()
    return response


def find_cores():
    cores = []
    cores_path = 'db/solr'

    potential_cores = os.listdir(cores_path)
    for potential_core in potential_cores:
        # Ignore any non-directory files in here
        if os.path.isdir("{}/{}".format(cores_path, potential_core)):
            cores.append(potential_core)
    return cores


@task(help={'core': "Name of the core to run against. Omitting this value will create all cores"})
def create(core=None):
    cores = []
    if core:
        cores.append(core)
    else:
        cores = find_cores()

    for core in cores:
        print('Creating Core {}'.format(core))

        print('Uploading schema.xml')
        response = upload_file('db/solr/{}/schema.xml'.format(core), '{}/resource/{}/schema.xml'.format(solr_url, core))
        if response.status_code == 200:
            print("SUCCESS")
        else:
            raise RuntimeError('Error uploading schema.xml')

        print('Uploading solrconfig.xml')
        response = upload_file('db/solr/{}/solrconfig.xml'.format(core), '{}/resource/{}/solrconfig.xml'.format(
            solr_url, core))
        if response.status_code == 200:
            print("SUCCESS")
        else:
            raise RuntimeError('Error uploading solrconfig.xml')

        response = requests.get('{}/admin/cores?action=CREATE&name={}'.format(solr_url, core))
        if response.status_code == 200:
            print('Core created, you may view the status in the web interface')


@task(help={'core': "Name of the core to run against. Omitting this value will create all cores"})
def migrate(core=None):
    cores = []
    if core:
        cores.append(core)
    else:
        cores = find_cores()

    for core in cores:
        print('Updating Core {}'.format(core))

        print('Uploading schema.xml')
        response = upload_file('db/solr/{}/schema.xml'.format(core), '{}/resource/{}/schema.xml'.format(solr_url, core))
        if response.status_code == 200:
            print("SUCCESS")
        else:
            raise RuntimeError('Error uploading schema.xml')

        print('Uploading solrconfig.xml')
        response = upload_file('db/solr/{}/solrconfig.xml'.format(core), '{}/resource/{}/solrconfig.xml'.format(solr_url, core))
        if response.status_code == 200:
            print("SUCCESS")
        else:
            raise RuntimeError('Error uploading solrconfig.xml')

        print('Reloading core')
        response = requests.get('{}/admin/cores?action=RELOAD&name={}'.format(solr_url, core))
        if response.status_code == 200:
            print('Successfully reloaded Solr core')


@task(help={'name': "Name of the core you want to create"})
def add_core(name):
    if name:
        path = "db/solr/{}".format(name)
        if os.path.exists(path):
            print("File or directory: {} already exists. Aborting.".format(path))
        else:
            print("Creating directory {}".format(path))
            os.makedirs(path)

            print("Creating EMPTY solrconfig.xml")
            fd = open("{}/solrconfig.xml".format(path), 'w')
            fd.close()

            print("Creating EMPTY schema.xml")
            fd = open("{}/schema.xml".format(path), 'w')
            fd.close()
    else:
        print(
            "Call add_core with the --name parameter specifying a name core. Ex: foo.bar - where foo is your keyspace "
            "and bar the table name")
