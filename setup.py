from setuptools import setup, find_packages  # Always prefer setuptools over distutils

setup(
    name='trireme',
    version='1.0.0.dev2',

    description='Migration tool providing support for Apache Cassandra, DataStax Enterprise Cassandra, & DataStax '
                'Enterprise Solr.',

    url='https://github.com/o19s/trireme',
    download_url='https://github.com/o19s/trireme/tarball/1.0.0.dev2',

    author='Christopher Bradford',
    author_email='cbradford@opensourceconnections.com',

    license='BSD',

    classifiers=[
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        'Programming Language :: Python :: 3',
    ],

    keywords='cassandra solr dse dsc migrations development',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    install_requires=['lz4', 'blist', 'cassandra-driver', 'requests', 'invoke']
)
