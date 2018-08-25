from supercell.api import RequestHandler
from supercell.api import async
from supercell.api import provides
from supercell.decorators import consumes
from supercell.mediatypes import Return, Error

from distributed_index.shared.models import InvertedIndexModel
from distributed_index.slave_node.models import PartialIndexRequest


@consumes('application/json', model=PartialIndexRequest)
@provides('application/json', default=True)
class PartialIndexHandler(RequestHandler):
    """Handler for /api/partial_index"""

    @async
    def post(self, model=None):
        # Create a partial index that contains only entries for the words
        # that are passed to the function.

        index_container = self.environment.index_container
        index = index_container['index']

        if not index:
            raise Error(500, additional={
                "message": "Index must be created first."
            })

        partial_index = index.create_partial_index(model.words)

        response = InvertedIndexModel(partial_index)
        response.validate()
        raise Return(response)
