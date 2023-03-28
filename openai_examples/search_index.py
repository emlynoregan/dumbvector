# In this program we open an index, sort it by similarity to a query, then
# return the top N chunks.

import time
start_time = time.time()

import argparse
from dumbvector.util import time_function
from dumbvector.dumb_index import file_to_dumb_index, docs_from_dumb_index
from dumbvector.docs import get_docs_file_and_cache_reader
from dumbvector.search import top_k_similar

import openai
import json
import os
import numpy as np

end_time = time.time()
print (f'import time: {end_time - start_time}')

def read_credentials():
    with open('credentials.json', 'r') as f:
        return json.load(f)

def get_embedding(text, engine="text-embedding-ada-002"):
    response = openai.Embedding.create(
        input=[text],
        model=engine
    )
    embedding = response['data'][0]['embedding']
    return embedding

def main():
    # usage: python search_index.py index_filename docs_path query [num_results]

    parser = argparse.ArgumentParser()

    parser.add_argument('index_filename', help='the name of the index file to use')
    parser.add_argument('docs_path', help='the path to the docs file')
    parser.add_argument('query', help='the query to search for')
    # default to 20 results
    parser.add_argument('num_results', help='the number of results to return', nargs='?', default=20)

    args = parser.parse_args()

    index_filename = args.index_filename
    docs_path = args.docs_path
    query = args.query
    num_results = int(args.num_results)

    # read the credentials
    credentials = read_credentials()

    openai.api_key = credentials['openaikey']

    # read the index
    index_name = os.path.basename(index_filename)
    # remove extension from index name
    index_name = os.path.splitext(index_name)[0]
    index_path = os.path.dirname(index_filename)
    index = time_function(file_to_dumb_index, f"load the index '{index_name}' from filesystem")(index_name, index_path)

    # number of elements in the index
    print ("Number of records in the index:", len(index.get('vectors')))

    # get the embedding for the query
    embedding = time_function(get_embedding, f"Get an embedding for the query from OpenAI")(query, engine="text-embedding-ada-002")

    embedding = np.array(embedding)

    # vector_dtype = index.get('vectors').dtype

    # # convert the embedding to the same dtype as the index
    # embedding = embedding.astype(vector_dtype)

    # # # get chunks to cache the chunks
    # # time_function(get_chunks_from_dumb_index)(s3_session, s3_bucket, index, 0, 1)

    docs_reader = get_docs_file_and_cache_reader(docs_path)

    # do the basic search
    sorted_index = time_function(top_k_similar, f"Find the top {num_results} results using cosine similarity")(index, embedding, num_results)

    # now get the Docs of results
    docs = time_function(docs_from_dumb_index, f"load the original docs from filesystem")(sorted_index, docs_reader, 0, num_results)

    doclist = docs.get("doclist")

    # print the results
    # just print "text" for each chunk
    print ("**********")
    print ("Results:")
    print ("**********")
    for ix, doc in enumerate(doclist):
        print (ix, doc['text'])

    print ("done")

if __name__ == '__main__':
    main()

