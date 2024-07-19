from abc import ABC, abstractmethod
from grpc import ServicerContext

from searcher_pb2 import SearchQuery, DocumentQuery, FulltextQuery
from search_result_pb2 import SearchResult, Document, Passage, Fulltext
from searcher_pb2_grpc import SearcherServicer


class AbstractSearcher(ABC, SearcherServicer):
    @abstractmethod
    def search(self, search_query: SearchQuery, context: ServicerContext) -> SearchResult:
        """
        Query an index and return search results
        """
        return SearchResult()

    @abstractmethod
    def get_document(self, document_query: DocumentQuery, context: ServicerContext) -> Document:
        """
        Given a document id, return a document's attributes
        """
        return Document()

    @abstractmethod
    def get_fulltext(self, fulltext_query: FulltextQuery, context: ServicerContext) -> Fulltext:
        """
        Given a ClueWeb-22 ID, return the document's full text (or a specific passage)
        """
        return Fulltext()
