import os
import re
import sys
import time
import json

from typing import Any

import grpc
from markupsafe import Markup
from flask import Flask, render_template, request, jsonify, Response


from search_result_pb2 import SearchResult, Document
from searcher_pb2 import SearchQuery, DocumentQuery, FulltextQuery, SearchBackend, SearchParameters
from searcher_pb2_grpc import SearcherStub

from reranker_pb2 import RerankRequest
from reranker_pb2_grpc import RerankerStub

from rewriter_pb2 import RewriteRequest
from rewriter_pb2_grpc import RewriterStub

from utils.conversion_utils import context_converter
from google.protobuf.json_format import MessageToDict


sys.path.insert(0, "/shared")
sys.path.insert(0, "/shared/compiled_protobufs")


app = Flask(__name__)

searcher_channel = grpc.insecure_channel(os.environ["SEARCHER_URL"])
search_client = SearcherStub(searcher_channel)

reranker_channel = grpc.insecure_channel(os.environ["RERANKER_URL"])
rerank_client = RerankerStub(reranker_channel)

rewriter_channel = grpc.insecure_channel(os.environ["REWRITER_URL"])
rewrite_client = RewriterStub(rewriter_channel)


def rerank(rerank_request: RerankRequest, passage_limit: int, passage_count: int) -> list[dict[str, Any]]:
    """
    Submit a request to the reranker service and return a list of documents.
    """
    rerank_result = rerank_client.rerank(rerank_request)

    documents = []
    for document in rerank_result.documents:
        converted_document = MessageToDict(document)
        converted_document["passages"] = converted_document["passages"][:passage_count]
        documents.append(converted_document)

    return documents


def search(search_query: SearchQuery, skip_rerank: bool, passage_count: int, passage_limit: int) -> list[dict[str, Any]]:
    """
    Submit a request to the searcher service and return results.
    """
    search_result: SearchResult = search_client.search(search_query)

    if skip_rerank:
        documents: list[dict] = []
        for document in search_result.documents:
            converted_document = MessageToDict(document)
            converted_document["passages"] = converted_document["passages"][:passage_count]
            # split() with no args = split on whitespace
            for passage in converted_document["passages"]:
                for query_term in search_query.query.split():
                    # this encloses all case-insensitive matches of query_term in <strong> tags to
                    # highlight them in the passage
                    passage["body"] = re.sub(
                        f"({query_term})",
                        "<strong>\\1</strong>",
                        passage["body"],
                        flags=re.IGNORECASE,
                    )

                # replace the original string content of the passage body with a Markup object, which
                # allows the HTML to be parsed as expected instead of being escaped
                passage["body"] = Markup(passage["body"])

            documents.append(converted_document)

        return documents

    rerank_request = RerankRequest()
    rerank_request.search_query = search_query.query
    rerank_request.num_passages = passage_limit
    rerank_request.search_result.MergeFrom(search_result)

    documents = rerank(rerank_request, passage_count, passage_limit)

    return documents


@app.route("/")
def display_homepage() -> str:
    """
    Flask handler for displaying the homepage.
    """
    return render_template("homepage.html")


@app.route("/api/fulltext/<id>", defaults={"passage": ""})
@app.route("/api/fulltext/<id>/<passage>")
def fulltext(id: str, passage: str) -> str | Response:
    """
    API endpoint for retrieving fulltext of ClueWeb documents.

    This supports two types of lookup:
        - api/fulltext/<id> retrieves the whole document
        - api/fulltext/<id>/<passage> retrieves a single passage from the document
    """
    query = FulltextQuery()

    # pyserini is the only usable option here
    query.search_backend = SearchBackend.PYSERINI
    query.document_id = id
    if len(passage) == 0:
        query.passage_id = -1
    else:
        try:
            query.passage_id = int(passage)
        except ValueError:
            return f"Invalid passage ID {passage}"

    fulltext = search_client.get_fulltext(query)
    fulltext = MessageToDict(fulltext)
    return jsonify(fulltext)


@app.route("/<id>/fulltext")
def display_doc(id: str) -> str:
    """
    Display document fulltext via HTML template.
    """
    args = request.args
    document_query = DocumentQuery()

    if args.get("search_backend"):
        if args["search_backend"] == "pyserini":
            document_query.search_backend = SearchBackend.PYSERINI

    document_query.document_id = id
    retrieved_document = search_client.get_document(document_query)

    converted_document = MessageToDict(retrieved_document)

    return render_template("fulltext.html", doc=converted_document)


@app.route("/search", methods=["GET"])
def search_webui() -> str | Response:
    """
    Handle search(+rerank) requests made through the web UI.
    """
    args = request.args

    search_query = SearchQuery()
    search_query.query = args["query"].replace("_", " ")
    search_query.num_hits = int(args["numDocs"])
    search_query.search_parameters.parameters["b"] = args["b"]
    search_query.search_parameters.parameters["k1"] = args["k1"]

    if args["backend"] == "pyserini":
        search_query.search_backend = SearchBackend.PYSERINI
    else:
        return Response("Invalid search backend", 400)

    if args["collection"] == "TRECiKAT2023":
        search_query.search_parameters.collection = SearchParameters.Collection.TRECIKAT2023
    else:
        return Response("Invalid collection", 400)

    skip_rerank = args["skipRerank"] == "true"
    passage_count = int(args["passageCount"])
    passage_limit = int(args["passageLimit"])

    start_time = time.time()
    documents = search(search_query, skip_rerank, passage_count, passage_limit)
    end_time = time.time()
    duration = int(end_time - start_time)

    return render_template(
        "results.html",
        docs=documents,
        numFound=len(documents),
        duration=duration,
        query=search_query.query,
    )


@app.route("/rewrite", methods=["POST"])
def rewrite() -> Response:
    """
    Submit a request to the rewriter service.

    This handler is actually triggered by some Javascript in homepage.js.
    """
    client_rewrite_request = request.get_data()
    client_rewrite_request = json.loads(client_rewrite_request)

    rewrite_request = RewriteRequest()

    rewrite_request.search_query = client_rewrite_request["searchQuery"]

    if client_rewrite_request["turnsToUse"] == "raw":
        rewrite_request.query_context = client_rewrite_request["context"]

    else:
        rewrite_request.query_context = context_converter(
            client_rewrite_request["context"], int(client_rewrite_request["turnsToUse"])
        )

    if client_rewrite_request["rewriter"] == "T5":
        rewrite_request.rewriter = RewriteRequest.Rewriter.T5

    rewrite_result = rewrite_client.rewrite(rewrite_request)

    return jsonify({"rewrite": rewrite_result.rewrite, "context": rewrite_request.query_context})


@app.route("/api/search", methods=["GET", "POST"])
def search_api() -> Response:
    """
    API endpoint equivalent of running a search or search+rerank in the web UI.
    """
    request_dict = request.args if request.method == "GET" else request.form

    search_query = SearchQuery()
    query = request_dict.get("searchQuery", type=str)
    if query is None or len(query) == 0:
        return Response("Missing query string", 400)

    search_query.query = query.replace("_", " ")
    search_query.num_hits = request_dict.get("numDocs", default=50, type=int)
    search_query.search_parameters.parameters["b"] = request_dict.get("b", default="0.8", type=str)
    search_query.search_parameters.parameters["k1"] = request_dict.get("k1", default="4.4", type=str)

    backend = request_dict.get("backend", default="pyserini", type=str)
    if backend == "pyserini":
        search_query.search_backend = SearchBackend.PYSERINI
    else:
        return Response("Invalid search backend", 400)

    collection = request_dict.get("collection", default="TRECiKAT2023", type=str)
    if collection == "TRECiKAT2023":
        search_query.search_parameters.collection = SearchParameters.Collection.TRECIKAT2023
    else:
        return Response("Invalid collection", 400)

    passage_count = request_dict.get("passage_count", default=3, type=int)
    passage_limit = request_dict.get("passage_limit", default=20, type=int)
    skip_rerank = request_dict.get("skip_rerank", default=False, type=bool)

    documents = search(search_query, skip_rerank, passage_count, passage_limit)

    return jsonify(documents)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
