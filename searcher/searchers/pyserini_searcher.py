import os

from .abstract_searcher import AbstractSearcher
from pyserini.search import LuceneSearcher
from searcher_pb2 import SearchQuery, DocumentQuery
from search_result_pb2 import SearchResult, Document, Passage

from bs4 import BeautifulSoup as bs
import lxml

class PyseriniSearcher(AbstractSearcher):

    def __init__(self):

        self.index_paths = {
            'CLUEWEB' : '/shared/indexes/clueweb',
            'TRECiKAT2023' : '/shared/indexes/trec_ikat_2023'
            # new indices go here
        }

        self.indexes = {}
        for name, path in self.index_paths.items():
            if not os.path.exists(path) or len(os.listdir(path)) == 0:
                print(f'WARNING: Not using index {name} because {path} does not exist')
                continue

            self.indexes[name] = LuceneSearcher(path)

        self.chosen_searcher = None
    
    def search(self, search_query: SearchQuery, context):

        query: str = search_query.query
        num_hits: int = search_query.num_hits

        if search_query.search_parameters.collection == 0:
            self.chosen_searcher = self.indexes["CLUEWEB"]
        elif search_query.search_parameters.collection == 1:
            self.chosen_searcher = self.indexes["TRECiKAT2023"]
        else:
            pass # TODO
        
        bm25_b = search_query.search_parameters.parameters["b"]
        bm25_k1 = search_query.search_parameters.parameters["k1"]
        
        self.chosen_searcher.set_bm25(float(bm25_k1), float(bm25_b))
        hits = self.chosen_searcher.search(query, num_hits)

        search_result = SearchResult()

        for hit in hits:
            retrieved_document = self.__convert_search_response(hit)
            search_result.documents.append(retrieved_document)

        return search_result

    
    def get_document(self, document_query: DocumentQuery, context):

        document_id = document_query.document_id
        
        # only ClueWeb in this version
        self.chosen_searcher = self.indexes['CLUEWEB']

        hit = self.chosen_searcher.doc(document_id)

        retrieved_document = self.__convert_search_response(hit)

        return retrieved_document

    
    def __convert_search_response(self, hit):

        retrieved_document = Document()
        soup = None

        try:
            #This works if it is a regular search hit
            soup = bs(hit.raw, "lxml")
            retrieved_document.id = hit.docid
            retrieved_document.score = hit.score
        except:
            #This works if it is a document lookup
            soup = bs(hit.raw(), "lxml")
            retrieved_document.id = hit.docid()
        
        retrieved_document.url = soup.find("url").text
        retrieved_document.title = soup.find("title").text
        
        
        passages = soup.find_all("passage")
        
        for passage in passages:
            chunked_passage = Passage()
            chunked_passage.id = passage["id"]
            chunked_passage.body = passage.text

            retrieved_document.passages.append(chunked_passage)    
        
        return retrieved_document
    
