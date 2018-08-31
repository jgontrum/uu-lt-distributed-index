import json

from supercell.api import RequestHandler
from supercell.api import async
from supercell.api import provides
from supercell.decorators import consumes
from supercell.mediatypes import Return, Error
from tornado.httpclient import HTTPRequest

from distributed_index.shared.inverted_index import InvertedIndex
from distributed_index.shared.models import InvertedIndexModel
from distributed_index.slave_node.models import MergeRequest


@consumes('application/json', model=MergeRequest)
@provides('application/json', default=True)
class MergeHandler(RequestHandler):
    """Handler for /api/merge"""

    @async
    def post(self, model=None):
        # Performs the merge stage. Receives a list of words that the node is
        # responsible for and a list of other nodes to call.
        index_container = self.environment.index_container
        http_client = self.environment.http_client
        nlp = self.environment.nlp

        words = model.words
        nodes = model.nodes
        index = index_container['index']

        if not index:
            raise Error(500, additional={
                "message": "Index must be created first."
            })

        # Retrieve a partial index from all other nodes that contains only
        # the words this node is assigned to.
        requests = [
            HTTPRequest(
                f"http://{self.config.address}:{node['port']}"
                f"/api/partial_index",
                method="POST",
                body=json.dumps({"words": words}),
                headers={'content-type': 'application/json'},
                request_timeout=3600
            ) for node in nodes
        ]

        responses_raw = yield [
            http_client.fetch(request) for request in requests
        ]

        # Check that all requests were successful
        if not all([response.code == 200 for response in responses_raw]):
            raise Error(500, additional={
                "message": "Error creating partial index."
            })

        # Merge all the partial indices
        partial_indices = [
            json.loads(response.body) for response in responses_raw
        ]
        local_partial_index = index.create_partial_index(words)
        partial_indices.append(local_partial_index)

        merged_index = InvertedIndex.merge(nlp, *partial_indices)

        # Return the merged index
        response = InvertedIndexModel(merged_index.inverted_index)
        response.validate()
        raise Return(response)
