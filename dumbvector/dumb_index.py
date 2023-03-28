'''
This module implements functions for creating and working with dumb indices, in memory and on disk.

Dumb indices store a list of vectors, and a list of references to individual documents. Each list is the same length,
and the vectors and documents in the same position in their respective lists are related.

Dumb indices also have a list of docsnames, which is a list of strings, one for each docs used in the index.

The Docs references are a pair of (docsnameix, docix), where docsnameix is the index of the docsname in the docsnames list,
and docix is the index of the document in the doclist for that docsname.

The whole dumbindex is serialized as bson.

A dumbindex looks like this:
{
    "name": "my_index",
    "version": 1,
    "docsnames": ["docsname1", "docsname2", ...],
    "vectors": [vector1, vector2, ...],
    "docrefs": [(docsnameix1, docix1), (docsnameix2, docix2), ...]
}

The bson format is a dictionary, which is then serialized as bson. It looks like this:
{
    "n": "my_index",
    "v": 1,
    "d": ["docsname1", "docsname2", ...],
    "v": [vector1, vector2, ...],
    "r": [(docsnameix1, docix1), (docsnameix2, docix2), ...]
}
'''

import bson
from dumbvector.bsonutil import replace_bytearrays_with_numarrays, replace_numarrays_with_bytearrays, numarray_to_bsu_bytearray
from dumbvector.docs import make_docs_v1
from dumbvector.util import time_function
import os
import numpy as np

def create_dumb_index(index_name, docs, f_get_vector_from_doc, normalize_vectors=False):
    list_of_docs = docs if isinstance(docs, list) else [docs]

    docsnames = [d.get("name") for d in list_of_docs]

    vectors = []

    docrefs = []

    for docsnameix, docs in enumerate(list_of_docs):
        doclist = docs.get("doclist")
        for docix, doc in enumerate(doclist):
            vector = f_get_vector_from_doc(doc)
            vectors.append(vector)
            docrefs.append((docsnameix, docix))

    vectors = np.array(vectors)

    if normalize_vectors:
        vectors = vectors / np.linalg.norm(vectors, axis=1)[:, None]
    
    dumb_index = {
        "name": index_name,
        "version": 1,
        "docsnames": docsnames,
        "vectors": vectors,
        "docrefs": docrefs
    }

    return dumb_index

def dumb_index_to_binary(dumb_index):
    # convert vectors back to a dumb list of lists
    vectors = dumb_index.get("vectors")

    # convert vectors to a bytearrays
    # vectors_ba = [
    #     numarray_to_bsu_bytearray(v) for v in vectors
    # ]

    vectors_ba = vectors.tobytes()
    dimensions = vectors.shape
    dtype = vectors.dtype

    di = bson.dumps({
        "n": dumb_index.get("name"),
        "v": dumb_index.get("version"),
        "d": dumb_index.get("docsnames"),
        "z": vectors_ba,
        "zt": dtype.str,
        "zd": dimensions,
        "r": dumb_index.get("docrefs")
    })
    return di

def binary_to_dumb_index(binary):
    di = bson.loads(binary)
    if di.get("v") != 1:
        raise Exception("invalid version")

    # convert vectors back to a numpy array
    dimensions = di.get("zd")
    dtype = np.dtype(di.get("zt")) # di.get("zt") is a string like "<f4"    
    vectors = np.frombuffer(di.get("z"), dtype=dtype).reshape(dimensions)

    # vectors_ba = di.get("z")
    # vectors = time_function(replace_bytearrays_with_numarrays)(vectors_ba)

    dumb_index = {
        "name": di.get("n"),
        "version": di.get("v"),
        "docsnames": di.get("d"),
        "vectors": vectors,
        "docrefs": di.get("r")
    }
    return dumb_index

def sanitize_dumb_index_name_for_filesystem(name):
    # disallowed characters: /, \, :, *, ?, ", <, >, |
    disallowed_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|"]
    fixed_name = name
    for char in disallowed_chars:
        fixed_name = fixed_name.replace(char, "_")
    return fixed_name

def create_full_pathname_for_dumb_index_file(name, base_path=None):
    fixed_name = sanitize_dumb_index_name_for_filesystem(name)
    if base_path is None:
        return fixed_name + ".dumbindex"
    else:
        return os.path.join(base_path, fixed_name + ".dumbindex")

def dumb_index_to_file(dumb_index, base_path=None):
    name = dumb_index.get("name")
    full_pathname = create_full_pathname_for_dumb_index_file(name, base_path)
    binary = time_function(dumb_index_to_binary)(dumb_index)
    with open(full_pathname, "wb") as f:
        f.write(binary)

def file_to_dumb_index(name, path=None):
    full_pathname = create_full_pathname_for_dumb_index_file(name, path)
    if not os.path.exists(full_pathname):
        raise Exception("file not found")
    with open(full_pathname, "rb") as f:
        binary = f.read()
    dumb_index = binary_to_dumb_index(binary)
    return dumb_index

def dumb_index_exists(name, path=None):
    full_pathname = create_full_pathname_for_dumb_index_file(name, path)
    return os.path.exists(full_pathname)

def docs_from_dumb_index(dumb_index, docs_reader, offset, amount):
    '''
    Returns a list of documents from the dumb index, starting at offset and returning amount documents,
    as a new Docs.
    '''
    name = dumb_index.get("name")
    dumb_index_docrefs = dumb_index.get("docrefs")
    docrefs = dumb_index_docrefs[offset:offset+amount]
    # docs_reader is a function that takes a docsname and returns a Docs
    docsnames = dumb_index.get("docsnames")
    doclist = [
        docs_reader(docsnames[docsnameix]).get("doclist")[docix]
        for docsnameix, docix in docrefs
    ]
    docs_name = f"{name}_{offset}_{amount}"
    docs = make_docs_v1(docs_name, doclist)
    return docs

def get_dumb_index_file_reader(path, fallback_reader=None):
    def reader(name):
        dumb_index = file_to_dumb_index(name, path)
        if not dumb_index:
            if fallback_reader:
                dumb_index = fallback_reader(name)
        return dumb_index
    return reader

def get_dumb_index_file_writer(path, overwrite=False, next_writer=None):
    def writer(dumb_index):
        name = dumb_index.get("name")
        need_write = overwrite or not dumb_index_exists(name, path)
        if need_write:
            dumb_index_to_file(dumb_index, path)
        if next_writer:
            next_writer(dumb_index)
    return writer

C_DUMB_INDEX_CACHE = {}

def _is_in_cache(name):
    return name in C_DUMB_INDEX_CACHE

def _write_to_cache(dumb_index):
    global C_DUMB_INDEX_CACHE
    name = dumb_index.get("name")
    C_DUMB_INDEX_CACHE[name] = dumb_index

def _read_from_cache(name):
    return C_DUMB_INDEX_CACHE.get(name)

def clear_cache():
    global C_DUMB_INDEX_CACHE
    C_DUMB_INDEX_CACHE = {}

def get_dumb_index_cache_reader(fallback_reader=None):
    def reader(name):
        dumb_index = _read_from_cache(name)
        if not dumb_index:
            if fallback_reader:
                dumb_index = fallback_reader(name)
        return dumb_index
    return reader

def get_dumb_index_cache_writer(overwrite=False, next_writer=None):
    def writer(dumb_index):
        need_write = overwrite or not _is_in_cache(dumb_index.get("name"))
        if need_write:
            _write_to_cache(dumb_index)
        if next_writer:
            next_writer(dumb_index)
    return writer

def get_dumb_index_file_and_cache_reader(path, fallback_reader=None):
    file_reader = get_dumb_index_file_reader(path, fallback_reader)
    cache_reader = get_dumb_index_cache_reader(file_reader)
    return cache_reader

def get_dumb_index_file_and_cache_writer(path, overwrite=False, next_writer=None):
    file_writer = get_dumb_index_file_writer(path, overwrite, next_writer)
    cache_writer = get_dumb_index_cache_writer(overwrite, file_writer)
    return cache_writer
