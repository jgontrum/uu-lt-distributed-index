import json

from supercell.api import RequestHandler
from supercell.api import async
from supercell.api import provides
from supercell.mediatypes import Return

from distributed_index.shared.models import HealthResponse


@provides('application/json', default=True)
class HealthHandler(RequestHandler):
    """Handler for /api/health"""

    @async
    def get(self):
        http_client = self.environment.http_client

        # Call all nodes and collect their health status.
        nodes = []
        for node_info in self.environment.nodes:
            result = yield http_client.fetch(
                f"http://{self.config.address}:{node_info['port']}/api/health"
            )

            nodes.append(json.loads(result.body))

        model = HealthResponse(
            {
                "health": "green",
                "node_name": "master_node",
                "slave_nodes": nodes
            }
        )
        model.validate()
        raise Return(model)
