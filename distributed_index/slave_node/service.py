import spacy
from supercell.service import Service
from tornado.httpclient import AsyncHTTPClient
from tornado.options import define

from distributed_index import configuration
from distributed_index.slave_node.handlers.health import HealthHandler
from distributed_index.slave_node.handlers.index import IndexHandler
from distributed_index.slave_node.handlers.merge import MergeHandler
from distributed_index.slave_node.handlers.partial_index import \
    PartialIndexHandler

# Register a custom command line argument to set the name of this node.
define('node_name', type=str, help="Name of the slave node.")


class SlaveNodeService(Service):
    """The main service of the supercell application"""

    def bootstrap(self):
        """
        Set custom options.
        """

        if not self.config['address']:
            self.config['address'] = configuration['master']['host']

    def run(self):
        """
        Contains the main logic of the service, settings up handlers,
        managed objects, etc.
        """
        # Client used to make non-blocking requests to other nodes.
        http_client = AsyncHTTPClient(max_clients=100)
        self.environment.add_managed_object("http_client", http_client)

        # Container to store the index in that this node is assigned to.
        # Note: This is the reason this api is *not* state-less.
        self.environment.add_managed_object("index_container", {"index": None})

        # Load the spaCy NLP models.
        nlp = spacy.load('en', disable=['parser', 'ner'])
        self.environment.add_managed_object("nlp", nlp)

        self.slog.info(
            f"Running Slave Node "
            f"('{self.config.node_name}') "
            f"on 'http://{self.config.address}:{self.config.port}'."
        )

        self.environment.add_handler(r"/api/health", HealthHandler, {})
        self.environment.add_handler(r"/api/index", IndexHandler, {})
        self.environment.add_handler(r"/api/partial_index",
                                     PartialIndexHandler, {})
        self.environment.add_handler(r"/api/merge", MergeHandler, {})


def start_api():
    """
    Entry point to run the api.
    """
    service = SlaveNodeService()
    service.main()
