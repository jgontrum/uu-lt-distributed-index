from schematics.models import Model
from schematics.types import BooleanType, FloatType
from schematics.types.compound import ModelType, ListType

from distributed_index.shared.models import Document, InvertedIndexModel


class StatisticsModel(Model):
    create_indices = FloatType(required=True)
    merge_word_indices = FloatType(required=True)
    merge_final_indices = FloatType(required=True)
    overall = FloatType(required=True)


class IndexRequest(Model):
    documents = ListType(ModelType(Document), required=True)


class IndexResponse(Model):
    success = BooleanType(required=True)
    index = ModelType(InvertedIndexModel, required=True)
    stats = ModelType(StatisticsModel, required=True)
