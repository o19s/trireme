from __future__ import absolute_import
import os
from invoke import Collection, task
from trireme.migrators import cassandra, solr, data


@task
def setup():
    # Create the necessary directories
    directories = ['db', 'db/solr', 'db/migrations', 'db/data']
    for directory in directories:
        os.makedirs(directory)

ns = Collection(cassandra, solr, data)
ns.add_task(setup)
