"""
The data migrator enables migrations that modify records in a cassandra
database.

If the migration completes successfully, the migration is recorded in the
migrations table so it is never run again.
"""

from __future__ import absolute_import
from invoke import task, run
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import datetime
import subprocess
from trireme_config import contact_points, keyspace, migration_master, username, password, replication

contact_point = contact_points[0]
cluster = None
session = None


def connect(migration_keyspace):
    global cluster, session

    # Setup the auth provider
    auth_provider = PlainTextAuthProvider(username=username, password=password)

    # Contact to Cassandra
    cluster = Cluster(contact_points, auth_provider=auth_provider)
    session = cluster.connect(migration_keyspace)


def disconnect():
    session.shutdown()
    cluster.shutdown()


def authentication_enabled():
    return username and password


def master():
    """
    Fail if this is not the migration master.
    """
    if not migration_master:
        raise Exception("Not the migration master (set migration_master=True)")


@task
def migrate():
    master()
    # Connect to Cassandra
    connect(keyspace)

    # Pull all migrations from the disk
    print('Loading migrations')

    # Load migrations from disk
    disk_migrations = os.listdir('db/data')
    for disk_migration in disk_migrations:
        if not disk_migration.endswith('.py'):
            disk_migrations.remove(disk_migration)

    # Pull all migrations from C*
    results = session.execute("SELECT * FROM {}.migrations".format(keyspace))
    for row in results:  # Remove any disk migration that matches this record
        if row.migration in disk_migrations:
            disk_migrations.remove(row.migration)

    if len(disk_migrations) > 0:  # Sort the disk migrations, to ensure they are run in order
        disk_migrations.sort()  # Prepare the migrations table insert statement

        insert_statement = session.prepare("INSERT INTO {}.migrations (migration) VALUES (?)".format(keyspace))

        # Iterate over remaining migrations and run them
        for migration in disk_migrations:
            if migration.endswith('.py'):
                print("Running migration: {}".format(migration))

                cmd = ["/app/.heroku/python/bin/python", migration]
                result = subprocess.run(cmd, cwd='/app/db/trireme/db/data', env={'PYTHONPATH': '/app', 'ENVIRONMENT': os.getenv('ENVIRONMENT')}, stdout=subprocess.PIPE)
                if result.returncode != 0:
                    print("Script returned {}. Migration partially applied.".format(result.returncode))
                    print("Script output:")
                    print(result.stdout.decode('utf-8'))
                else:
                    session.execute(insert_statement, [migration])
    else:
        print('All migrations have already been run.')

    disconnect()


@task(help={'name': 'Name of the migration. Ex: update_users_session'})
def add_migration(name):
    master()
    if name:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
        path = "db/data/{}_{}.py".format(timestamp, name)
        fd = open(path, 'w')
        fd.close()

        print("Created migration: {}".format(path))
    else:
        print('Call add_migration with the --name parameter specifying a name for the migration. Ex: update_users_session')
