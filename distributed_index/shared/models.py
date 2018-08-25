from schematics.models import Model
from schematics.types import StringType, IntType
from schematics.types.compound import ListType, ModelType, DictType


class Document(Model):
    url = StringType(required=True)
    text = StringType(required=True)
    title = StringType(required=True)
    id = IntType(required=True)


class NodeHealth(Model):
    health = StringType(required=True, choices=[
        'green',
        'yellow',
        'red'
    ])
    node_name = StringType(required=True)


class HealthResponse(NodeHealth):
    slave_nodes = ListType(
        ModelType(NodeHealth),
        serialize_when_none=False,
        required=False
    )


class AnalyzedFields(Model):
    token = DictType(ListType(IntType))
    lemma = DictType(ListType(IntType))
    lemma_lowercase = DictType(ListType(IntType))
    token_lowercase = DictType(ListType(IntType))


class InvertedIndexModel(Model):
    text = ModelType(AnalyzedFields, required=True)
