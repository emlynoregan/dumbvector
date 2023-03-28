# Dumb Vector
_Semantic Search done the dumb way._

Dumb Vector is a python library implementing a really dumb brute force approach to semantic search. It's fast! It's simple! It's dumb!

# Quickstart
``` py
from dumbvector.docs import make_docs_v1, get_docs_file_and_cache_reader, get_docs_file_and_cache_writer
from dumbvector.index import create_dumb_index, docs_from_dumb_index
from dumbvector.search import top_k_similar
from openai.embedding_utils import get_embedding
import numpy as np

# Create some docs
doclist = [
    {"text": "hello", "embedding": get_embedding("hello", model="text-embedding-ada-002")},
    {"text": "world", "embedding": get_embedding("world", model="text-embedding-ada-002")},
    {"text": "hello world", "embedding": get_embedding("hello world", model="text-embedding-ada-002")}
]

name = "my_docs"
docs = make_docs_v1(name, doclist)
writer = get_docs_file_and_cache_writer("path/to/docs/folder", overwrite=True)
writer(docs)

# Create an index
index_name = "my_index"
docs_list = [docs]
def f_get_vector_from_doc(doc):
    return np.array(doc["embedding"])

dumb_index = create_dumb_index(index_name, docs_list, f_get_vector_from_doc)

# Search
query = "hello"
query_embedding = get_embedding(query)
k = 2

result_index = top_k_similar(dumb_index, query_embedding, k)

# Get the docs
reader = get_docs_file_and_cache_reader("path/to/docs/folder")
docs = docs_from_dumb_index(result_index, reader, 0, k)

for ix, doc in enumerate(docs.get("doclist")):
    print(f"Result {ix}: {doc['text']}")

# Output:
# Result 0: hello
# Result 1: hello world
```

# How it works
Say you have N documents, and you want to search through them. 

You generate an embedding for each document, with D dimensions.

A dumbindex is a list of N vectors, each with D dimensions, paired with a reference to the document that the vector came from.

A dumbindex search calculates the cosine similarity between the query vector and each vector in the dumbindex, and returns the top K results.

Cosine similarity is a measure of how similar two vectors are. It's a number between -1 and 1, where 1 is the most similar, and -1 is the least similar.

It is calculated like so:
``` py
def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
```

ie: the dot product of the two vectors, divided by the product of the norms of the two vectors.

We can assume that the vectors are unit vectors, so the norms are 1 (if your embeddings are not unit vectors, you should normalize them first). This means that the cosine similarity is the dot product of the two vectors.

So we need to calculate the dot product of the query vector and each vector in the dumbindex. This is a matrix multiplication!

The query vector is a 1xD matrix, and the dumbindex is an NxD matrix. The transpose of the dumbindex is a DxN matrix.

So if we multiple the query vector by the transpose of the dumbindex, we get a 1xN matrix, which is the cosine similarity of the query vector with each vector in the dumbindex.

We then find the top k results from this result vector; these are the search results.

This algorithm is O(N) in the number of documents (if we assume D is a constant). That's not great in terms of algorithmic complexity. 

However, implementing this as a single matrix multiplication using numpy means that it's very fast in practice up to quite large numbers of documents. For example, using sbert embeddings, I can search through 1,000,000 documents in under 1 second on my laptop, using the 
current implementation.

Tip: If you want another speedup at the (very small) cost of some accuracy, try casting all your vectors to float16. This will make the matrix multiplication faster, but will also reduce the accuracy of the cosine similarity calculation. eg:

``` py
smaller_embedding = np.array(embedding, dtype=np.float16)
```

# Docs
A Docs file is a file storing a list of json-compatible documents. These documents are your source records, the things that you want 
to search through.

A simple convention is to include an embedding vector for each document, in the attribute "embedding", but this isn't required. You 
could put more than one embedding vector in there, you could use a different attribute name or names, or you could even leave 
generating the embedding until index building time (see below).

If you do include an embedding, it should be a numpy array of floats, like so:
``` py
text = <some text that we are searching>
raw_embedding = <get an embedding from somewhere, based on text>
np_embedding = np.array(raw_embedding)

doc = {
    "text": text,
    "embedding": embedding
}
```

Do this, because when the binary format for Docs files is emitted, numpy arrays are converted to a binary format that is far more
compact than a list of floats.

Once you have an array of documents (a "doclist"), you can create a Docs file like so:
``` py
from dumbvector.docs import make_docs_v1

doclist = [{"text": "hello", "embedding": [1, 2, 3]}, ...]
name = "my_docs"
docs = make_docs_v1(name, doclist)
```

The docs object is a python dictionary, with the following attributes:
``` py
{
    "name": <name of the docs>,
    "version": 1.1,
    "doclist": <list of documents>
}
```
So the way to access your original doc dictionaries is like so:
``` py
doclist = docs["doclist"]
# I want the first doc
doc = doclist[0]
```

You can then write the docs to a file in your docs folder like so:
``` py
from dumbvector.docs import docs_to_file

docs_to_file(docs, "path/to/docs/folder")
```
It'll be named "my_docs.docs".

*NOTE: A Docs file is a binary format which is compact and very fast to read and write.*

You can read a Docs file back like so:
``` py
from dumbvector.docs import file_to_docs

name = "my_docs"
docs = file_to_docs(name, "path/to/docs/folder")
```

A better way to read a Docs file is to use a Docs reader, which is a higher order function that returns a function that reads a Docs file.
``` py
from dumbvector.docs import get_docs_file_and_cache_reader

name = "my_docs"
reader = get_docs_file_and_cache_reader("path/to/docs/folder")
docs = reader(name)
```
This is better because it caches the Docs files in memory, so you don't have to read them from disk every time you want to use them.

A better way to write a Docs file is to use a Docs writer, which is a higher order function that returns a function that writes a Docs file.
``` py
from dumbvector.docs import get_docs_file_and_cache_writer

name = "my_docs"
writer = get_docs_file_and_cache_writer("path/to/docs/folder", overwrite=True)
writer(docs)
```
This will write the docs to the file "my_docs.docs" in the docs folder. It will also cache the docs in memory.
If you want to overwrite an existing file, and the cache, you can set the `overwrite` parameter to True.
Otherwise it will only write the file if it doesn't already exist, and it will only cache the docs if they aren't already cached.

Note there are readers and writers for just the filesystem and just the cache as well.

path_to_docs_list() is a function which reads all docs files in a folder and returns a list of Docs.
``` py
from dumbvector.docs import path_to_docs_list

docs_list = path_to_docs_list("path/to/docs/folder")
```

file_docs_exists() is a function which checks if a docs file exists on the filesystem.
``` py
from dumbvector.docs import file_docs_exists

exists = file_docs_exists("my_docs", "path/to/docs/folder")
```

# dumbindex
Once you have some Docs, you can create a dumbindex file from them. A dumbindex file is a file storing the embeddings for each
doc in the Docs, and a corresponding reference to the Docs it comes from and the doc's index in the Docs.

A dumbindex is required for searching through (one or more) Docs.

To create a dumbindex, you need a list of docs, and a function which gets one embedding from a single doc.
``` py
from dumbvector.index import create_dumb_index

index_name = "my_index"
docs_list = [Docs, ...]
def get_embedding(doc):
    return doc["embedding"]

dumb_index = create_dumb_index(index_name, docs_list, get_embedding)
```

The vector returned by _get_embedding()_ should be a one dimensional numpy array of floats. It should be normalized to 
have a length of 1. Many models will do this for you, but if you are using a model that doesn't, you can normalize it like so:
``` py
import numpy as np

def get_embedding(doc):
    embedding = doc["embedding"]
    norm_embedding = embedding / np.linalg.norm(embedding)
    return norm_embedding
```

You can then write the index to a file in your index folder like so:
``` py
from dumbvector.index import dumb_index_to_file

dumb_index_to_file(dumb_index, "path/to/index/folder")
```
It'll be named "my_index.dumbindex".

*NOTE: An Index file is a binary format which is compact and very fast to read and write.*

You can read an Index file back like so:
``` py
from dumbvector.index import file_to_dumb_index

name = "my_index"
dumb_index = file_to_dumb_index(name, "path/to/index/folder")
```

A better way to read an Index file is to use an Index reader, which is a higher order function that returns a function that reads an Index file.
``` py
from dumbvector.index import get_dumb_index_file_and_cache_reader

name = "my_index"
reader = get_dumb_index_file_and_cache_reader("path/to/index/folder")
dumb_index = reader(name)
```

A better way to write an Index file is to use an Index writer, which is a higher order function that returns a function that writes an Index file.
``` py
from dumbvector.index import get_dumb_index_file_and_cache_writer

name = "my_index"
writer = get_dumb_index_file_and_cache_writer("path/to/index/folder", overwrite=True)
writer(dumb_index)
```

Note there are readers and writers for just the filesystem and just the cache as well.

dumb_index_exists() is a function which checks if an Index file exists on the filesystem.
``` py
from dumbvector.index import dumb_index_exists

exists = dumb_index_exists("my_index", "path/to/index/folder")
```

Finally, you can get a list of doc dictionaries from your index, in order, as a Docs object, like so:
``` py
from dumbvector.index import docs_from_dumb_index

dumb_index = ...
docs_reader = get_docs_file_and_cache_reader("path/to/docs/folder")

docs = docs_from_dumb_index(dumb_index, docs_reader, 0, 20)

# docs is a Docs object with the first 20 docs from the index
```
This is an essential function for retrieving results after searching.

# search
Once you have some Docs and a dumbindex, you can search through them.

Say the user has typed in a query, and you want to find the most similar docs to the query:

``` py
from dumbvector.search import top_k_similar
from dumbvector.index import docs_from_dumb_index

query = "this is a query"
query_embedding = model.encode(query)
k = 10

top_k_index = top_k_similar(dumb_index, query_embedding, k) # this is the semantic search

docs_reader = get_docs_file_and_cache_reader("path/to/docs/folder")
docs = docs_from_dumb_index(top_k_index, docs_reader, 0, k)

for ix, doc in enumerate(docs.get("doclist")):
    print(f"Result {ix}: {doc['text']}")
```

This will print the top 10 results from the search.

For more examples, see the examples folders:

* [OpenAI Examples - requires OpenAI API key](openai_examples)
* [SBert Examples - uses a downloadable SBert model](sbert_examples)
