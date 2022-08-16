from setuptools import setup

setup(
    name='SparkETLWorkflow',
    version='0.1',
    packages=['com', 'com.db', 'com.db.fw', 'com.db.fw.etl', 'com.db.fw.etl.core', 'com.db.fw.etl.core.Dummy',
              'com.db.fw.etl.core.common', 'com.db.fw.etl.core.writer', 'com.db.fw.etl.core.readers',
              'com.db.fw.etl.core.Pipeline', 'com.db.fw.etl.core.Exception', 'com.db.fw.etl.core.processor',
              'com.db.fw.etl.test'],
    url='',
    license='',
    author='piyush.acharya',
    author_email='piyush.acharya@databricks.com',
    description='For Meatadata driven Workflow',
    setup_requires=['wheel']
)
