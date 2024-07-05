import os
import re
import sys
import time
import json

import grpc
from markupsafe import Markup
from flask import Flask, render_template, request, jsonify, Response

sys.path.insert(0, "/shared")
sys.path.insert(0, "/shared/compiled_protobufs")


from search_result_pb2 import SearchResult
from searcher_pb2 import SearchQuery, DocumentQuery
from searcher_pb2_grpc import SearcherStub

from reranker_pb2 import RerankRequest
from reranker_pb2_grpc import RerankerStub

from rewriter_pb2 import RewriteRequest
from rewriter_pb2_grpc import RewriterStub

from utils.conversion_utils import context_converter
from google.protobuf.json_format import MessageToDict

app = Flask(__name__)


searcher_channel = grpc.insecure_channel(os.environ["SEARCHER_URL"])
search_client = SearcherStub(searcher_channel)

reranker_channel = grpc.insecure_channel(os.environ["RERANKER_URL"])
rerank_client = RerankerStub(reranker_channel)

rewriter_channel = grpc.insecure_channel(os.environ["REWRITER_URL"])
rewrite_client = RewriterStub(rewriter_channel)


def rerank(rerank_request: RerankRequest, passage_limit: int, passage_count: int):
    rerank_result = rerank_client.rerank(rerank_request)

    documents = []
    for document in rerank_result.documents:
        converted_document = MessageToDict(document)
        converted_document["passages"] = converted_document["passages"][:passage_count]
        documents.append(converted_document)

    return documents


def search(
    search_query: SearchQuery, skipRerank: bool, passage_count: int, passage_limit: int
):
    search_result = search_client.search(search_query)

    if skipRerank:
        documents = []
        for document in search_result.documents:
            converted_document = MessageToDict(document)
            converted_document["passages"] = converted_document["passages"][
                :passage_count
            ]
            # split() with no args = split on whitespace
            for passage in converted_document["passages"]:
                for query_term in search_query.query.split():
                    # this encloses all case-insensitive matches of query_term in <strong> tags to
                    # highlight them in the passage
                    passage["body"] = re.sub(
                        f"({query_term})",
                        f"<strong>\\1</strong>",
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
def display_homepage():
    return render_template("homepage.html")


@app.route("/<id>/fulltext")
def display_doc(id):
    args = request.args
    document_query = DocumentQuery()

    if args.get("search_backend"):
        if args["search_backend"] == "pyserini":
            document_query.search_backend = 0

    document_query.document_id = id
    retrieved_document = search_client.get_document(document_query)

    converted_document = MessageToDict(retrieved_document)

    return render_template("fulltext.html", doc=converted_document)


@app.route("/search", methods=["GET"])
def search_webui():
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
        search_query.search_backend = 0
    else:
        return "Invalid search backend", 400

    if args["collection"] == "TRECiKAT2023":
        search_query.search_parameters.collection = 1
    else:
        return "Invalid collection", 400

    skipRerank = args["skipRerank"] == "true"
    passage_count = int(args["passageCount"])
    passage_limit = int(args["passageLimit"])

    start_time = time.time()
    documents = search(search_query, skipRerank, passage_count, passage_limit)
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
def rewrite():
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
        rewrite_request.rewriter = 0

    rewrite_result = rewrite_client.rewrite(rewrite_request)

    return {"rewrite": rewrite_result.rewrite, "context": rewrite_request.query_context}


@app.route("/api/search", methods=["GET", "POST"])
def search_api() -> Response:
    """
    API endpoint equivalent of running a search or search+rerank in the web UI.
    """
    request_dict = request.args if request.method == "GET" else request.form

    search_query = SearchQuery()
    query = request_dict.get("searchQuery", type=str)
    if query is None or len(query) == 0:
        return "Missing query string", 400

    search_query.query = query.replace("_", " ")
    search_query.num_hits = request_dict.get("numDocs", default=50, type=int)
    search_query.search_parameters.parameters["b"] = request_dict.get(
        "b", default="0.8", type=str
    )
    search_query.search_parameters.parameters["k1"] = request_dict.get(
        "k1", default="4.4", type=str
    )

    backend = request_dict.get("backend", default="pyserini", type=str)
    if backend == "pyserini":
        search_query.search_backend = 0
    else:
        return "Invalid search backend", 400

    collection = request_dict.get("collection", default="TRECiKAT2023", type=str)
    if collection == "TRECiKAT2023":
        search_query.search_parameters.collection = 1
    else:
        return "Invalid collection", 400

    passage_count = request_dict.get("passage_count", default=3, type=int)
    passage_limit = request_dict.get("passage_limit", default=20, type=int)
    # reranker= request_dict.get("reranker", default="T5", type=str)
    skipRerank = request_dict.get("skipRerank", default=False, type=bool)

    documents = search(search_query, skipRerank, passage_count, passage_limit)

    return jsonify(documents)


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
