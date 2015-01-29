from invoke import task
import os
from subprocess import call
from cassandra.cluster import Cluster
import datetime

# TODO Move to config file
contact_points = ["127.0.0.1"]
keyspace = "foo"
migration_master = True

cluster = None
session = None


def connect(migration_keyspace):
    global cluster, session

    # Contact to Cassandra
    cluster = Cluster(contact_points)
    session = cluster.connect(migration_keyspace)

def disconnect():
    session.shutdown()
    cluster.shutdown()

@task
def create():
    if migration_master:
        # Note we use the system keyspace in connect call since the target keyspace doesn't exist yet.
        connect('system')

        # Create the keyspace, we use simple defaults (SimpleStrategy, RF: 2)
        print('Creating keyspace {}'.format(keyspace))
        session.execute('CREATE KEYSPACE IF NOT EXISTS {} '
                        'WITH REPLICATION = {{'
                        '\'class\': \'NetworkTopologyStrategy\', '
                        '\'Solr\': 1, '
                        '\'Cassandra\': 1, '
                        '\'Analytics\': 1}}'.format(keyspace))

        # Add the migrations table transparently, this will track which migrations have been run
        session.execute('CREATE TABLE IF NOT EXISTS {}.migrations ('
                        'migration text, '
                        'PRIMARY KEY(migration));'.format(keyspace))

        # Provide some help text, as most installs will not use SimpleStrategy for replication
        print('Keyspace created')

        disconnect()


@task
def drop():
    if migration_master:
        # Connect to Cassandra
        connect(keyspace)

        # Drop the keyspace
        print('Dropping keyspace {}'.format(keyspace))
        session.execute('DROP KEYSPACE {}'.format(keyspace))

        disconnect()

@task
def migrate():
    if migration_master:
        # Connect to Cassandra
        connect(keyspace)

        # Determine the contact point
        contact_point = contact_points[0]

        # Pull all migrations from the disk
        print('Loading migrations')
        disk_migrations = os.listdir('db/migrations')  # Remove non-.cql files from the list of migrations

        for disk_migration in disk_migrations:
            if not disk_migration.endswith(".cql"):
                disk_migrations.remove(disk_migration)  # Pull all migrations from C*

        results = session.execute('SELECT * FROM {}.migrations'.format(keyspace))
        for row in results:  # Remove any disk migration that matches this record
            disk_migrations.remove(row.migration)

        if len(disk_migrations) > 0:  # Sort the disk migrations, to ensure they are run in order
            disk_migrations.sort()  # Prepare the migrations table insert statement

            insert_statement = session.prepare('INSERT INTO {}.migrations (migration) VALUES (?)'.format(keyspace))

            # Iterate over remaining migrations and run them
            for migration in disk_migrations:
                if migration.endswith(".cql"):
                    print('Running migration: {}'.format(migration))
                    command = 'cqlsh -f db/migrations/{} -k {} {}'.format(migration, keyspace, contact_point)

                    status = call(command, shell=True)

                    if status == 0:
                        session.execute(insert_statement, [migration])
        else:
            print('All migrations have already been run.')

    disconnect()

@task(help={'name':"Name of the migration. Ex: add_users_table"})
def add_migration(name):
    if name:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
        path = 'db/migrations/{}_{}.cql'.format(timestamp, name)
        fd = open(path, 'w')
        fd.close()

        print('Created migration: {}'.format(path))
    else:
        print("Call add_migration with the --name parameter specifying a name for the migration. Ex: add_users_table")
