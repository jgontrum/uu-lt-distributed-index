from schematics.models import Model
from schematics.types import StringType, BooleanType, IntType
from schematics.types.compound import ModelType, ListType

from distributed_index.shared.models import Document


class IndexRequest(Model):
    documents = ListType(ModelType(Document), required=True)


class IndexResponse(Model):
    success = BooleanType(required=True)
    words = ListType(StringType(), required=True)


class NodeModel(Model):
    name = StringType(required=True)
    port = IntType(required=True)


class MergeRequest(Model):
    nodes = ListType(ModelType(NodeModel), required=True)
    words = ListType(StringType, required=True)


class PartialIndexRequest(Model):
    words = ListType(StringType(), required=True)
