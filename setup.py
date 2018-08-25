from setuptools import setup

setup(
    name='distributed_index',
    version='0.1',
    description='Demo that showcases a simple distributed index implementation.',
    author='Johannes Gontrum',
    author_email='gontrum@me.com',
    include_package_data=True,
    license='Not open source',
    entry_points={
          'console_scripts': [
              'start_master = distributed_index.master_node.service:start_api',
              'start_slave = distributed_index.slave_node.service:start_api'
          ]
      }
)
