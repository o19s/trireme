from invoke import Collection
from db.migrators import cassandra, solr

namespace = Collection(cassandra, solr)
