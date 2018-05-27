import argparse
import asyncio
import json
import random

import spacy
from aiohttp import web
from inverted_index import InvertedIndex

routes = web.RouteTableDef()

inverted_index = None
nlp = spacy.load('en', disable=['parser', 'ner'])


@routes.post('/index')
async def index(request):
    """
        POST /index

        Consumes application/json
        Model:
        {
            "task": <int>,
            "documents": [
                {
                    "url": <str>,
                    "text": <str>,
                    "title": <str>,
                    "id": <int>
                },
                ...
            ]
        }


        Provides application/json
        Model:
        {
            "task": <int>,
            "success": <boolean>,
            "index_path": <str>
        }

        Description:
        Creates an inverted index for a list of provided documents which is then saved
        to file. The path is then returned as well as the id of the task and a flag
        that indicates a success.
        """
    global nlp
    global inverted_index

    try:
        request = await request.json()

        try:
            inverted_index = InvertedIndex(nlp)
            inverted_index.index(request['documents'])

            response = web.json_response({
                "success": True,
                "task": request['task']
            }, status=201)

        except Exception:
            response = web.json_response({
                'success': False,
                "task": request['task'],
                "message": "Error creating index."
            }, status=500)

    except json.decoder.JSONDecodeError or KeyError:
        response = web.json_response({
            'success': False,
            "task": request['task'],
            "message": "Invalid JSON format."
        }, status=400)

    return response


@routes.get('/health')
async def words(_):
    """
    GET /health

    Provides application/json
    Model:
    [
        "health": "green"
    ]

    Description:
    Returns if the node is still running.
    """
    return web.json_response({"health": "green"}, status=200)


@routes.get('/words')
async def words(_):
    """
    GET /words

    Provides application/json
    Model:
    [
        "word1",
        "word2",
        ...
    ]

    Description:
    Returns a list of all the words in the corpus
    """
    global inverted_index

    if not inverted_index:
        return web.json_response({
            "success": False,
            "message": "Index is not yet created."
        }, status=500)
    return web.json_response(inverted_index.words(), status=200)


@routes.post('/entries')
async def entries(request):
    """
    POST /entries

    Consumes application/json
    Model:
    [
        "word1", "word2", ...
    ]


    Provides application/json
    Model:
    {InvertedIndex}

    Description:
    Returns a partial inverted index that contains the words given
    in the request.
    """
    global inverted_index

    if not inverted_index:
        return web.json_response({
            "success": False,
            "message": "Index is not yet created."
        }, status=500)

    try:
        request = await request.json()
        response = web.json_response(
            inverted_index.create_partial_index(request), status=201)

    except json.decoder.JSONDecodeError or KeyError:
        response = web.json_response({
            'success': False,
            "task": request['task'],
            "message": "Invalid JSON format."
        }, status=400)

    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--port", help="Port to listen for requests.",
                        type=int, required=False,
                        default=8001)
    parser.add_argument("--host", help="Host to run on.",
                        type=str, required=False,
                        default="127.0.0.1")

    args = parser.parse_args()
    app = web.Application()
    app.add_routes(routes)
    web.run_app(app, host=args.host, port=args.port)
