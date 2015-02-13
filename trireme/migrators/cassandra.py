from invoke import task, run
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import datetime
from config import contact_points, keyspace, migration_master, username, password

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


def cqlsh_command(**kwargs):
    # Start out with the base command
    command = "cqlsh"

    # Add authentication
    if authentication_enabled():
        kwargs['u'] = username
        kwargs['p'] = password

    # Add additional arguments
    for key in kwargs.keys():
        command += " -{} \"{}\"".format(key, kwargs[key])

    # Specify the host
    command += " {}".format(contact_point)

    return command


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

        # Pull all migrations from the disk
        print('Loading migrations')

        # Load migrations from disk
        disk_migrations = os.listdir('db/migrations')
        for disk_migration in disk_migrations:
            if not disk_migration.endswith(".cql"):
                disk_migrations.remove(disk_migration)

        # Pull all migrations from C*
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

                    result = run(cqlsh_command(f="db/migrations/{}".format(migration), k=keyspace), hide='stdout')

                    if result.ok:
                        session.execute(insert_statement, [migration])
        else:
            print('All migrations have already been run.')

        dump_schema()

    disconnect()


@task
def dump_schema():
    result = run(cqlsh_command(k=keyspace, e="DESCRIBE KEYSPACE {}".format(keyspace)), hide='stdout')
    schema_file = open('db/schema.cql', 'w')
    schema_file.write(result.stdout)
    schema_file.close()


@task
def load_schema():
    print('Verifying keyspace is not present')
    connect('system')
    rows = session.execute("SELECT * FROM schema_keyspaces WHERE keyspace_name = %s", [keyspace])
    if len(rows) > 0:
        disconnect()
        print('Keyspace already exists. Drop it first with cassandra.drop then try again')
    else:
        disconnect()
        print('Loading the schema in db/schema.cql')

        result = run(cqlsh_command(f='db/schema.cql'), hide='stdout')

        if result.ok:
            print('Load successful. Updating migrations table')
            connect(keyspace)

            # Load migrations from disk
            disk_migrations = os.listdir('db/migrations')  # Remove non-.cql files from the list of migrations
            for disk_migration in disk_migrations:
                if not disk_migration.endswith(".cql"):
                    disk_migrations.remove(disk_migration)

            # Write each migration into the migrations table
            insert_statement = session.prepare('INSERT INTO {}.migrations (migration) VALUES (?)'.format(keyspace))
            for disk_migration in disk_migrations:
                session.execute(insert_statement, [disk_migration])

            disconnect()
        else:
            print('Errors while loading schema.cql')


@task(help={'name': "Name of the migration. Ex: add_users_table"})
def add_migration(name):
    if name:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
        path = 'db/migrations/{}_{}.cql'.format(timestamp, name)
        fd = open(path, 'w')
        fd.close()

        print('Created migration: {}'.format(path))
    else:
        print("Call add_migration with the --name parameter specifying a name for the migration. Ex: add_users_table")
