import json
import logging
from wsgiref import simple_server

import falcon
import spacy
from inverted_index import InvertedIndex

inverted_index = None

class RequireJSON(object):
    """
    See http://falcon.readthedocs.io/en/stable/user/quickstart.html
    """

    def process_request(self, req, resp):
        if not req.client_accepts_json:
            raise falcon.HTTPNotAcceptable(
                'This API only supports responses encoded as JSON.',
                href='http://docs.examples.com/api/json')

        if req.method in ('POST', 'PUT'):
            if 'application/json' not in req.content_type:
                raise falcon.HTTPUnsupportedMediaType(
                    'This API only supports requests encoded as JSON.',
                    href='http://docs.examples.com/api/json')


class IndexResource:
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
    def __init__(self, nlp_model):
        self.nlp = nlp_model
        self.logger = logging.getLogger('node.' + __name__)

    def on_post(self, req, resp):
        global inverted_index
        try:
            request = json.load(req.stream)

            try:
                inverted_index = InvertedIndex(self.nlp)
                inverted_index.index(request['documents'])

                resp.body = json.dumps({
                    "task": request['task'],
                    "success": True
                })
            except Exception:
                resp.body = json.dumps({
                    "task": request['task'],
                    "success": False
                })

        except json.decoder.JSONDecodeError or KeyError:
            raise falcon.HTTPBadRequest(
                'Invalid JSON sent.')

        # Set the return data
        resp.status = falcon.HTTP_201
        resp.context_type = falcon.MEDIA_JSON


class EntriesResource:
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

    def __init__(self):
        self.logger = logging.getLogger('node.' + __name__)

    def on_post(self, req, resp):
        global inverted_index
        try:
            request = json.load(req.stream)

            # try:
            resp.body = json.dumps(
                inverted_index.inverted_index)
            # except Exception:
            #     resp.body = json.dumps({
            #         "success": False
            #     })

        except json.decoder.JSONDecodeError or KeyError:
            raise falcon.HTTPBadRequest(
                'Invalid JSON sent.')

        # Set the return data
        resp.status = falcon.HTTP_201
        resp.context_type = falcon.MEDIA_JSON


# Configure your WSGI server to load "things.app" (app is a WSGI callable)
app = falcon.API(middleware=[
    RequireJSON()
])

nlp = spacy.load('en', disable=['parser', 'ner'])


app.add_route('/index', IndexResource(nlp))
app.add_route('/entries', EntriesResource())

# Useful for debugging problems in your API; works with pdb.set_trace(). You
# can also use Gunicorn to host your app. Gunicorn can be configured to
# auto-restart workers when it detects a code change, and it also works
# with pdb.
if __name__ == '__main__':
    httpd = simple_server.make_server('127.0.0.1', 8000, app)
    httpd.serve_forever()
