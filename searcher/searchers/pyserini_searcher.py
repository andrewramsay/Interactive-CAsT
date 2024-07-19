import os
import json

from grpc import ServicerContext
from bs4 import BeautifulSoup as bs
import lxml
from pyserini.search import LuceneSearcher

from .abstract_searcher import AbstractSearcher
from searcher_pb2 import SearchQuery, DocumentQuery, FulltextQuery
from search_result_pb2 import SearchResult, Document, Passage, Fulltext

class PyseriniSearcher(AbstractSearcher):

    def __init__(self) -> None:

        self.index_paths = {
            # this index contains the full text of all ClueWeb records
            # in the iKAT collection
            'CLUEWEB' : '/shared/indexes/clueweb',
            # this index contains the segmented passages
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
    
    def search(self, search_query: SearchQuery, context: ServicerContext) -> SearchResult:

        query: str = search_query.query
        num_hits: int = search_query.num_hits

        if search_query.search_parameters.collection == 0:
            self.chosen_searcher = self.indexes["CLUEWEB"]
        elif search_query.search_parameters.collection == 1:
            self.chosen_searcher = self.indexes["TRECiKAT2023"]
        else:
            print("Unknown searcher selected!")
            return SearchResult()
        
        bm25_b = search_query.search_parameters.parameters["b"]
        bm25_k1 = search_query.search_parameters.parameters["k1"]
        
        self.chosen_searcher.set_bm25(float(bm25_k1), float(bm25_b))
        hits = self.chosen_searcher.search(query, num_hits)

        search_result = SearchResult()

        for hit in hits:
            retrieved_document = self.__convert_search_response(hit)
            search_result.documents.append(retrieved_document)

        return search_result

    
    # TODO combine this method with get_fulltext?
    def get_document(self, document_query: DocumentQuery, context: ServicerContext):
        document_id = document_query.document_id
        
        self.chosen_searcher = self.indexes['CLUEWEB']

        hit = self.chosen_searcher.doc(document_id)
        
        content = json.loads(hit.raw())
        retrieved_document = Document()
        retrieved_document.id = hit.docid()
        retrieved_document.url = content["url"]
        passage = Passage()
        passage.id = hit.docid().split(":")[1]
        passage.body = content["contents"]
        retrieved_document.passages.append(passage)

        return retrieved_document

    def get_fulltext(self, fulltext_query: FulltextQuery, context: ServicerContext) -> Fulltext:
        document_id = fulltext_query.document_id
        passage_id = fulltext_query.passage_id

        # if passage_id is < 0, it means this is a query for the fulltext
        # of the selected record
        index = self.indexes['CLUEWEB'] 
        if passage_id != -1:
            # searching for a specific passage instead
            index = self.indexes["TRECiKAT2023"]

        hit = index.doc(document_id)

        fulltext = Fulltext()

        # if we're returning a result from the fulltext index
        if passage_id == -1:
            data = json.loads(hit.raw())
            fulltext.id = document_id
            fulltext.url = data["url"]
            fulltext.title = data["title"]
            fulltext.body = data["contents"]
            # TODO: forgot to remove title before indexing, can remove this once index rebuilt
            fulltext.body = fulltext.body[fulltext.body.index("\n")+1:]
        else:
            # for the passage index, the contents are in trecweb format and need to pull out
            # a selected passage
            soup = bs(hit.raw(), "lxml")
            fulltext.id = f"{document_id}:{passage_id}"
            fulltext.url = soup.find("url").text
            fulltext.title = soup.find("title").text
            passages = soup.find_all("passage")
            fulltext.body = passages[passage_id].text
        
        return fulltext

    def __convert_search_response(self, hit) -> Document:

        retrieved_document = Document()
        soup = None

        try:
            # This works if it is a regular search hit
            soup = bs(hit.raw, "lxml")
            retrieved_document.id = hit.docid
            retrieved_document.score = hit.score
        except:
            # This works if it is a document lookup
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
    
