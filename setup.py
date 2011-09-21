from setuptools import setup

dependencies = """
SQLAlchemy
psycopg2
"""

setup(
    name='datastore',
    version='0.1',
    install_requires=dependencies.split()
)

