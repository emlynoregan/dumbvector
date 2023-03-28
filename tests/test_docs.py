import unittest
from dumb_vector_s3 import docs, bsonutil
import os

class Tests(unittest.TestCase):
    folder = os.path.normpath("tests/scratch")

    def test_docs_to_binary(self):
        vec = list(range(1000))

        doclist = [{"a": "here is a string"}, {"b": 2}, {"c": vec}]
        d = docs.make_docs_v1("test_docs_to_binary", doclist)
        binary = docs.docs_to_binary(d)
        d2 = docs.binary_to_docs(binary)
        self.assertEqual(d, d2)
    
    def test_docs_to_file(self):
        doclist = [{"a": 1}, {"b": 2.3}]
        name = "test_docs_to_file"
        d = docs.make_docs_v1(name, doclist)
        path = os.path.join(self.folder, "test_docs_to_file")
        # create the path if it doesn't exist
        if not os.path.exists(path):
            os.mkdir(path)
        docs.docs_to_file(d, path)
        d2 = docs.file_to_docs(name, path)
        self.assertEqual(d, d2)

    def test_vector_to_docs_bytearray(self):
        vec = list(range(1000))
        docs_bytearray = bsonutil.numarray_to_bsu_bytearray(vec)
        vec2 = bsonutil.bsu_bytearray_to_numarray(docs_bytearray)
        self.assertEqual(vec, vec2)

    def test_file_and_cache_reader_writer(self):
        doclist = [{"a": 1}, {"b": 2.3}]
        name = "test_file_and_cache_reader_writer"
        d = docs.make_docs_v1(name, doclist)
        path = os.path.join(self.folder, "test_file_and_cache_reader_writer")
        # create the path if it doesn't exist
        if not os.path.exists(path):
            os.mkdir(path)
        
        writer = docs.get_docs_file_and_cache_writer(path)
        writer(d)

        reader = docs.get_docs_file_and_cache_reader(path)
        d2 = reader(name)

        self.assertIsNotNone(d2)
        self.assertEqual(d, d2)

        

    
