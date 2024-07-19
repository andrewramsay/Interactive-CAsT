from grpc import ServicerContext

from .abstract_searcher import AbstractSearcher
from .pyserini_searcher import PyseriniSearcher

from searcher_pb2 import SearchQuery, DocumentQuery, FulltextQuery
from search_result_pb2 import SearchResult, Document, Passage, Fulltext
from searcher_pb2_grpc import SearcherServicer


class BackendSelector(AbstractSearcher):
    def __init__(self) -> None:
        self.searchers = {"PYSERINI": PyseriniSearcher()}

        self.chosen_backend = None

    def search(self, search_query: SearchQuery, context: ServicerContext) -> SearchResult:
        if search_query.search_backend == 0:
            self.chosen_backend = self.searchers["PYSERINI"]
        else:
            return SearchResult()

        return self.chosen_backend.search(search_query, context)

    def get_document(self, document_query: DocumentQuery, context: ServicerContext) -> Document:
        # user might want to look up a doc directly without performing a
        # search first

        if self.chosen_backend:
            return self.chosen_backend.get_document(document_query, context)
        else:
            if document_query.search_backend == 0:
                self.chosen_backend = self.searchers["PYSERINI"]
            else:
                return Document()

            return self.chosen_backend.get_document(document_query, context)

    def get_fulltext(self, fulltext_query: FulltextQuery, context: ServicerContext) -> Fulltext:
        if fulltext_query.search_backend == 0:
            self.chosen_backend = self.searchers["PYSERINI"]
        else:
            return Fulltext()

        return self.chosen_backend.get_fulltext(fulltext_query, context)
