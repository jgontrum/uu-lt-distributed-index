from supercell.api import RequestHandler
from supercell.api import async
from supercell.api import provides
from supercell.decorators import consumes
from supercell.mediatypes import Return

from distributed_index.shared.inverted_index import InvertedIndex
from distributed_index.slave_node.models import IndexRequest, IndexResponse


@consumes('application/json', model=IndexRequest)
@provides('application/json', default=True)
class IndexHandler(RequestHandler):
    """Handler for /api/index"""

    @async
    def post(self, model=None):
        index_container = self.environment.index_container
        nlp = self.environment.nlp

        documents = [doc.serialize() for doc in model.documents]

        # Create a new inverted index for the documents this node is
        # assigned to.
        inverted_index = InvertedIndex(nlp)
        inverted_index.index(documents)

        # Store the created index in memory to keep it for future requests.
        index_container['index'] = inverted_index

        response = IndexResponse({
            "success": True,
            "words": inverted_index.words()
        })
        response.validate()
        raise Return(response)
