from supercell.api import RequestHandler
from supercell.api import async
from supercell.api import provides
from supercell.mediatypes import Return

from distributed_index.shared.models import NodeHealth


@provides('application/json', default=True)
class HealthHandler(RequestHandler):
    """Handler for /api/health"""

    @async
    def get(self):
        response = NodeHealth(
            {
                "health": "green",
                "node_name": self.config.node_name
            }
        )

        response.validate()

        raise Return(response)
