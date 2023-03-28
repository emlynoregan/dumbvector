'''
This module implements functions for converting from Docs (in memory list of dicts) to 
a binary format and back. It also implements functions for reading and writing
Docs to and from the file system.
A Docs structure is a dictionary containing the name, version, and a list of dictionaries, 
(each one is an actual doc) which should be json serializable.
A docs structure looks like this:
{
    "name": "my_index",
    "version": 1,
    "doclist": [doc1, doc2, ...]
}
Docs are used to store the native documents to be indexed in dumb vector.
The binary format is the same dictionary structure,
which is then serialized as bson.

Actually there's a special case: where we find a list of numbers, we convert it to a byte array.
We do this by first figuring out the smallest type that can hold all the numbers in the list,
and then emitting a byte array of that type, with a leading byte indicating the type.
If we find a byte array in the input, we add a leading byte indicating that it's a byte array.
'''

import bson
import os
from dumbvector.bsonutil import replace_bytearrays_with_numarrays, replace_numarrays_with_bytearrays, encode_ndarrays, decode_ndarrays

def make_docs_v1(name, doclist):
    return {
        "name": name,
        "version": 1.1,
        "doclist": doclist
    }

def docs_to_binary(docs):
    doclist = docs.get("doclist")
    doclist2 = encode_ndarrays(doclist)
    name = docs.get("name")
    return bson.dumps({
        "n": name,
        "v": 1.1,
        "d": doclist2
    })

def binary_to_docs(binary):
    outer = bson.loads(binary)
    version = outer.get("v")
    if not version in [1, 1.1]:
        raise Exception(f"invalid version {version}")
    if not isinstance(outer.get("d"), list):
        raise Exception("invalid docfile")
    doclist = outer.get("d")
    if version == 1:
        doclist = replace_bytearrays_with_numarrays(doclist)
    elif version == 1.1:
        doclist = decode_ndarrays(doclist)
    name = outer.get("n")
    return {
        "name": name,
        "version": 1,
        "doclist": doclist
    }

def sanitize_docs_name_for_filesystem(name):
    # disallowed characters: /, \, :, *, ?, ", <, >, |
    disallowed_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|"]
    fixed_name = name
    for char in disallowed_chars:
        fixed_name = fixed_name.replace(char, "_")
    return fixed_name

def create_full_pathname_for_docs(name, base_path):
    fixed_name = sanitize_docs_name_for_filesystem(name)
    if base_path:
        return os.path.join(base_path, fixed_name + ".docs")
    else:
        return fixed_name + ".docs"

def docs_to_file(docs, path=None):
    name = docs.get("name")
    if not name:
        raise Exception("docs must have a name")
    full_path = create_full_pathname_for_docs(name, path)

    with open(full_path, "wb") as f:
        f.write(docs_to_binary(docs))

def file_to_docs(name, path=None):
    full_path = create_full_pathname_for_docs(name, path)
    if not os.path.exists(full_path):
        return None
    with open(full_path, "rb") as f:
        return binary_to_docs(f.read())

def file_docs_exists(name, path=None):
    full_path = create_full_pathname_for_docs(name, path)
    return os.path.exists(full_path)

def path_to_docs_list(path):
    all_docs = []
    for filename in os.listdir(path):
        if filename.endswith(".docs"):
            docs = file_to_docs(filename[:-5], path)
            all_docs.append(docs)
    return all_docs

def get_docs_file_reader(path, fallback_reader=None):
    def read_docs(name):
        docs = file_to_docs(name, path)
        if docs is None and fallback_reader is not None:
            docs = fallback_reader(name)
        return docs
    return read_docs

def get_docs_file_writer(path, overwrite=False, next_writer=None):
    def write_docs(docs):
        name = docs.get("name")
        if not name:
            raise Exception("docs must have a name")
        need_write = overwrite or not file_docs_exists(name, path)
        if need_write:
            docs_to_file(docs, path)
        if next_writer is not None:
            next_writer(docs)
    return write_docs

C_DOCS_CACHE = {}

def _is_in_cache(name):
    return name in C_DOCS_CACHE

def _write_to_cache(docs):
    global C_DOCS_CACHE
    name = docs.get("name")
    C_DOCS_CACHE[name] = docs

def _read_from_cache(name):
    return C_DOCS_CACHE.get(name)

def clear_cache():
    global C_DOCS_CACHE
    C_DOCS_CACHE = {}

def get_docs_cache_reader(fallback_reader=None):
    def read_docs(name):
        docs = _read_from_cache(name)
        if docs is None and fallback_reader is not None:
            docs = fallback_reader(name)
            if docs is not None:
                _write_to_cache(docs)
        return docs
    return read_docs

def get_docs_cache_writer(overwrite=False, next_writer=None):
    def write_docs(docs):
        name = docs.get("name")
        if not name:
            raise Exception("docs must have a name")
        need_write = overwrite or not _is_in_cache(name)
        if need_write:
            _write_to_cache(docs)
        if next_writer is not None:
            next_writer(docs)
    return write_docs

def get_docs_file_and_cache_reader(path, fallback_reader=None):
    file_reader = get_docs_file_reader(path, fallback_reader)
    cache_reader = get_docs_cache_reader(file_reader)
    return cache_reader

def get_docs_file_and_cache_writer(path, overwrite=False, next_writer=None):
    cache_writer = get_docs_cache_writer(overwrite, next_writer)
    file_writer = get_docs_file_writer(path, overwrite, cache_writer)
    return file_writer


