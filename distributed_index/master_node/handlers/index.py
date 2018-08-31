import json
import time
from collections import defaultdict
from copy import copy
from itertools import chain
from math import ceil
from random import shuffle

from supercell.api import RequestHandler
from supercell.api import async
from supercell.api import provides
from supercell.decorators import consumes
from supercell.mediatypes import Return, Error
from tornado.httpclient import HTTPRequest

from distributed_index.master_node.models import IndexRequest, IndexResponse
from distributed_index.shared.inverted_index import InvertedIndex


@consumes('application/json', model=IndexRequest)
@provides('application/json', default=True)
class IndexHandler(RequestHandler):
    """Handler for /api/index"""

    @async
    def post(self, model=None):
        # Main pipeline for the distributed indexing task.
        http_client = self.environment.http_client
        nodes = self.environment.nodes
        documents = model.documents

        #
        # STEP 1:
        # Split the documents into batches and send them to the slave nodes.
        #
        t0 = time.time()

        # In case we have more nodes than documents, scale down the number
        # of nodes to have one for each document
        if len(documents) < len(nodes):
            nodes = nodes[:len(documents)]

        batch_size = int(ceil(len(documents) / len(nodes)))
        batches = [
            [doc for doc in documents[i * batch_size: (i + 1) * batch_size]]
            for i in range(0, len(nodes))
        ]

        # Shuffle the nodes to better distribute the load over them,
        # because the last batch might be smaller than 'batch_size'.
        shuffled_nodes = copy(nodes)
        shuffle(shuffled_nodes)

        # Zip together the nodes and their batch and create a request
        requests = []
        for node, batch in zip(shuffled_nodes, batches):
            url = f"http://{self.config.address}:{node['port']}/api/index"
            payload = {
                "documents": [
                    doc.serialize()
                    for doc in batch
                ]
            }

            request = HTTPRequest(
                url,
                method="POST",
                body=json.dumps(payload),
                headers={'content-type': 'application/json'},
                request_timeout=3600
            )
            requests.append(request)

        # Send request to index the batch to all nodes in parallel
        responses_raw = yield [
            http_client.fetch(request) for request in requests
        ]

        # Check that all requests were successful
        if not all([response.code == 200 for response in responses_raw]):
            raise Error(500, additional={
                "message": "Slave node could not create index."
            })

        responses = [json.loads(response.body) for response in responses_raw]

        #
        # STEP 2:
        # Now that we crated a partial index for each batch, we need to split
        # the tokens used in the whole corpus over all nodes.
        #

        t1 = time.time()
        words = set(chain(*[response['words'] for response in responses]))

        # Redistribute the tokens over the nodes by using the modulo operator
        # on their hash value.
        words_distributed = defaultdict(list)
        for word in words:
            words_distributed[hash(word) % len(nodes)].append(word)

        # Build a request that tells each node what tokens it is
        # responsible for.
        requests = []
        for node, words_for_node in zip(nodes, words_distributed.values()):
            url = f"http://{self.config.address}:{node['port']}/api/merge"
            payload = {
                "words": words_for_node,
                "nodes": [node_ for node_ in nodes if node_ != node]
            }

            request = HTTPRequest(
                url,
                method="POST",
                body=json.dumps(payload),
                headers={'content-type': 'application/json'},
                request_timeout=3600
            )
            requests.append(request)

        # Send request to index the batch to all nodes in parallel
        responses_raw = yield [
            http_client.fetch(request) for request in requests
        ]

        # Check that all requests were successful
        if not all([response.code == 200 for response in responses_raw]):
            raise Error(500, additional={"message": "Error merging indices."})

        responses = [json.loads(response.body) for response in responses_raw]

        #
        # STEP 3:
        # Merge the partial indices that were calculated by the nodes and
        # return them. The returned index is equal to an index that would have
        # been created by a single node.
        #

        t2 = time.time()

        # Support the case of an empty response. This can happen with small
        # corpora that only contain stop words.
        if responses:
            merged_index = InvertedIndex.merge(None, *responses)
        else:
            merged_index = InvertedIndex(None)

        t3 = time.time()
        response = IndexResponse({
            "success": True,
            "index": merged_index.inverted_index,
            "stats": {
                "create_indices": t1 - t0,
                "merge_word_indices": t2 - t1,
                "merge_final_indices": t3 - t2,
                "overall": t3 - t0
            }
        })
        response.validate()
        raise Return(response)
