import json
from collections import defaultdict
from itertools import chain


class InvertedIndex:
    ANALYZE = {
        'token': lambda token: str(token),
        'lemma': lambda token: token.lemma_,
        'lemma_lowercase': lambda token: token.lemma_.lower(),
        'token_lowercase': lambda token: str(token).lower()
    }

    AVOID = {"SYM", "NUM", "PUNCT"}

    def __init__(self, nlp, fields=None, analyzers=None):
        if fields is None:
            fields = ['text']

        if analyzers is None:
            analyzers = [
                'token',
                'lemma',
                'token_lowercase',
                'lemma_lowercase'
            ]

        self.fields = fields
        self.analyzers = analyzers
        self.nlp = nlp

        self.inverted_index = {
            field: {
                analyzer: defaultdict(list)
                for analyzer in analyzers
            }
            for field in fields
        }

    def is_valid_token(self, token):
        if len(str(token).strip()) == 0:
            # Remove empty tokens
            return False

        # Remove stopwords, numbers and symbols
        return not (
                token.is_stop or
                token.pos_ in self.AVOID or
                token.tag_ in self.AVOID)

    def index(self, stream):
        """
        Process all documents in the stream
        and add them to the inverted index.
        """
        for doc in stream:
            for field in self.fields:
                text = doc.get(field)

                for token in self.nlp(text):
                    if not self.is_valid_token(token):
                        continue

                    for analyzer in self.analyzers:
                        self._add_to_index(
                            self.ANALYZE[analyzer](token),
                            doc['id'],
                            field,
                            analyzer
                        )

        self._sort_index()

    def _add_to_index(self, text, doc_id, field, analyzer):
        self.inverted_index[field][analyzer][text].append(int(doc_id))

    def _sort_index(self):
        """
        Sort postings and remove duplicates.
        """
        for field in self.fields:
            for analyzer in self.analyzers:
                index = self.inverted_index[field][analyzer]
                for token in list(index.keys()):
                    self.inverted_index[field][analyzer][token] = \
                        sorted(set(index[token]))

    def save_to_file(self, path):
        json.dump(self.inverted_index, open(path, 'w'))

    def create_partial_index(self, words):
        """
        Create a spartial index that only contains the given words.
        """
        partial_index = {}
        words = set(words)

        for field in self.fields:
            partial_index[field] = {}
            for analyzer in self.analyzers:
                partial_index[field][analyzer] = {
                    word: postings for word, postings
                    in self.inverted_index[field][analyzer].items()
                    if word in words
                }

        return partial_index

    def words(self):
        """
        Return a list of all the words used in this index.
        """
        return list(set(chain(
            *[
                self.inverted_index[field][analyzer].keys()
                for field in self.fields
                for analyzer in self.analyzers
            ]
        )))

    @classmethod
    def from_file(cls, nlp, path):
        data = json.load(open(path))
        fields = data.keys()
        analyzers = data[list(fields)[0]].keys()

        index = cls(nlp, fields, analyzers)
        index.inverted_index = data

        return index

    @classmethod
    def merge(cls, nlp, *indices):
        # Make sure all indices were created with the same settings
        fields = indices[0].keys()
        analyzers = indices[0][list(fields)[0]].keys()

        for index in indices:
            assert index.keys() == fields
            assert index[list(fields)[0]].keys() == analyzers

        # Create the base index
        merged_index = cls(nlp, fields, analyzers)

        for field in fields:
            for analyzer in analyzers:
                # Gather tokens
                tokens = chain(
                    *[index[field][analyzer].keys() for index in indices])

                # Concatenate postings
                inverted_index = {
                    token: chain(
                        *[index[field][analyzer].get(token, [])
                          for index in indices
                          ])
                    for token in tokens
                }

                merged_index.inverted_index[field][analyzer] = inverted_index

        merged_index._sort_index()

        return merged_index
