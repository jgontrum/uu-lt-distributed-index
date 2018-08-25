import subprocess

from supercell.service import Service
from tornado.httpclient import AsyncHTTPClient
from tornado.options import define

from distributed_index import configuration
from distributed_index.master_node.handlers.health import HealthHandler
from distributed_index.master_node.handlers.index import IndexHandler

define('slave_nodes_num', type=int, help="Number of slave nodes to spawn.")
define('slave_nodes_port', type=int, help="The port of the first slave node.")


class MasterNodeService(Service):
    """The main service of the supercell application"""

    def bootstrap(self):
        """
        Set the configuration from /config.yml in case the values are not set
        via command line argument.
        """
        if not self.config['port']:
            self.config['port'] = configuration['master']['port']

        if not self.config['address']:
            self.config['address'] = configuration['master']['host']

        if not self.config['slave_nodes_num']:
            self.config['slave_nodes_num'] = \
                configuration['slave']['number_of_slaves']

        if not self.config['slave_nodes_port']:
            self.config['slave_nodes_port'] = configuration['slave']['port']

    def start_slave_nodes(self):
        """
        Start the slave nodes as sub processes. This ensures that they will be
        terminated when the master process is stopped.
        :return: A list of name & port of each started slave node.
        """
        nodes = []
        for i in range(self.config.slave_nodes_num):
            name = configuration['slave']['name'].format(number=i)
            port = f"{self.config.slave_nodes_port + i}"

            command = configuration['slave']['run_command'].split()
            command += [f"--node_name={name}"]
            command += [f"--port={port}"]
            command += ["--max_grace_seconds=0"]
            command += ["--logfile=" +
                        configuration['slave']['logfile'].format(number=i)]

            subprocess.Popen(command)

            self.slog.info(f"Spawned node {i + 1}/"
                           f"{self.config.slave_nodes_num} on port {port}.")

            nodes.append({
                "name": name,
                "port": port
            })

        return nodes

    def run(self):
        """
        Contains the main logic of the service, settings up handlers,
        managed objects, etc.
        """
        http_client = AsyncHTTPClient(max_clients=100)
        self.environment.add_managed_object("http_client", http_client)

        self.slog.info(
            f"Running Master Node on "
            f"'http://{self.config.address}:{self.config.port}'."
        )

        self.environment.add_handler(r"/api/health", HealthHandler, {})
        self.environment.add_handler(r"/api/index", IndexHandler, {})

        # Start the slave nodes and remember their names & ports
        nodes = self.start_slave_nodes()
        self.environment.add_managed_object("nodes", nodes)


def start_api():
    """
    Entry point to run the api.
    """
    service = MasterNodeService()
    service.main()
