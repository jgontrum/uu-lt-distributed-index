import json
import logging
import os
import subprocess
from multiprocessing.pool import ThreadPool

import requests
import yaml
from aiohttp import web
from utils import run_on_second_event_loop

config = yaml.load(open('cluster_config.yml'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("master")
pool = ThreadPool()

routes = web.RouteTableDef()

# Load node config from the environment variable `NODES`
nodes = os.environ.get("NODES")
if nodes is not None:
    nodes = json.loads(nodes)
else:
    nodes = []


@routes.get('/health')
async def words(_):
    """
    GET /health

    Provides application/json
    Model:
    [
        "master_health": "green",
        "nodes": {
            1: {
                "health": "green"
            },
            ...
        }
    ]

    Description:
    Checks if the nodes and the master are still running.
    """

    ret = {
        "master_health": "green",
        "nodes": {}
    }

    # Run all health checks in a separate thread with a second event loop.
    # This has to be that complicated because there can only one event loop
    # per thread and the current one is already running the HTTP server
    # in an infinite loop.

    commands = ([{
        "command": requests.get,
        "args": ("%s/health" % node['url'],)
    } for node in nodes],)

    async_result = pool.apply_async(run_on_second_event_loop, commands)
    responses = async_result.get()

    for response, node in zip(responses, nodes):
        status_code, json_body = response
        ret['nodes'][node['id']] = json_body

    return web.json_response(ret, status=200)


if __name__ == '__main__':
    if not nodes:
        # If no nodes are externally running, spawn some.
        logger.info("Spawning %s nodes..." % config['number_of_nodes'])

        for i in range(1, config['number_of_nodes'] + 1):
            command = config['node_cmd'].split() + [
                '--port', str(config['port'] + i)]
            subprocess.Popen(command)  # , stdout=subprocess.DEVNULL)

            nodes.append({
                "url": "http://localhost:{}".format(config['port'] + i),
                "id": i
            })

            logger.info("Spawned node #{} on port {}.".format(
                i, config['port'] + i
            ))

    logger.info("Starting master node...")

    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, host="127.0.0.1", port="8000")
