import os

import yaml

current_dir = os.path.dirname(os.path.realpath(__file__))

configuration = yaml.load(open(f"{current_dir}/../config.yml"))
configuration['slave']['run_command'] = f"{current_dir}/../env/bin/start_slave"
configuration['slave']['logfile'] = current_dir + "/../logs/slave_node_{number}.log"
