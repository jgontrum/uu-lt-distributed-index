import json
import os
import signal
import subprocess
from collections import defaultdict
from time import sleep, time

import requests

experiments = [
    {
        "documents": 100,
        "nodes": 1
    },
    {
        "documents": 100,
        "nodes": 2
    },
    {
        "documents": 100,
        "nodes": 3
    },
    {
        "documents": 100,
        "nodes": 4
    },
    {
        "documents": 100,
        "nodes": 5
    },
    {
        "documents": 100,
        "nodes": 10
    },
    {
        "documents": 100,
        "nodes": 20
    },
    {
        "documents": 500,
        "nodes": 1
    },
    {
        "documents": 500,
        "nodes": 2
    },
    {
        "documents": 500,
        "nodes": 3
    },
    {
        "documents": 500,
        "nodes": 4
    },
    {
        "documents": 500,
        "nodes": 5
    },
    {
        "documents": 500,
        "nodes": 10
    },
    {
        "documents": 500,
        "nodes": 20
    },
    {
        "documents": 1000,
        "nodes": 1
    },
    {
        "documents": 1000,
        "nodes": 2
    },
    {
        "documents": 1000,
        "nodes": 3
    },
    {
        "documents": 1000,
        "nodes": 4
    },
    {
        "documents": 1000,
        "nodes": 5
    },
    {
        "documents": 1000,
        "nodes": 10
    },
    {
        "documents": 1000,
        "nodes": 20
    },
    {
        "documents": 5000,
        "nodes": 1
    },
    {
        "documents": 5000,
        "nodes": 2
    },
    {
        "documents": 5000,
        "nodes": 3
    },
    {
        "documents": 5000,
        "nodes": 4
    },
    {
        "documents": 5000,
        "nodes": 5
    },
    {
        "documents": 5000,
        "nodes": 10
    },
    {
        "documents": 5000,
        "nodes": 20
    },
]


def startup(number_of_nodes):
    # Wait for server to start
    started = False
    while not started:
        try:
            health_req = requests.get("http://localhost:8080/api/health")
            if health_req.status_code == 200:
                health = health_req.json()
                node_health = [
                    node['health'] == 'green' for node in health['slave_nodes']]
                if health['health'] == 'green' and all(node_health):
                    started = True
                    break
        except requests.exceptions.ConnectionError:
            pass

        wait = 1
        print(f"Wait for server to start... ({wait}s)")
        sleep(wait)


def cleanup():
    print("Kill server...")
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)


with open("results.csv", "w") as f:
    f.write("experiment,docs,nodes,all,processing,map,reduce,concatenate,"
            "transfer\n")

    for i, experiment in enumerate(experiments):
        print(f"Run experiment {i+1}: {experiment['documents']} docs "
              f"on {experiment['nodes']} nodes.")

        command = f"env/bin/start_master " \
                  f"--max_grace_seconds=0 " \
                  f"--logfile=/dev/null " \
                  f"--slave_nodes_num={experiment['nodes']}".split()

        process = subprocess.Popen(command, preexec_fn=os.setsid)

        startup(experiment['nodes'])

        # Run experiment!
        print("Start experiment...")
        metrics = defaultdict(float)

        data = [json.loads(line) for line in open(
            f"data/wikinews.dataset.{experiment['documents']}.jsonl")]

        request = {
            "documents": data
        }

        t0 = time()
        result_response = requests.post(
            "http://localhost:8080/api/index", json=request, timeout=3600)
        metrics['all'] = time() - t0

        result = result_response.json()

        metrics['processing'] = result['stats']['overall']
        metrics['map'] = result['stats']['create_indices']
        metrics['reduce'] = result['stats']['merge_word_indices']
        metrics['concatenate'] = result['stats']['merge_final_indices']
        metrics['transfer'] = metrics['all'] - metrics['processing']

        f.write(f"{i+1},{experiment['documents']},{experiment['nodes']},"
                f"{metrics['all']},{metrics['processing']},{metrics['map']},"
                f"{metrics['reduce']},{metrics['concatenate']},"
                f"{metrics['transfer']}\n")
        f.flush()

        print(f"Done with experiment ({int(metrics['all'])}s).")
        cleanup()
