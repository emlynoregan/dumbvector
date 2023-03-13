import unittest
import json
import boto3

from dumb_vector_s3 import write_chunks_to_s3, read_chunk_from_s3, yield_chunks_from_s3, create_dumb_index, \
    get_dumb_index_bytes, get_dumb_index_from_bytes, write_dumb_index_to_s3, read_dumb_index_from_s3, \
    get_chunks_from_dumb_index, flush_cache, yield_file_pairs_from_s3

def read_credentials():
    with open('tests/credentials.json', 'r') as f:
        return json.load(f)

# def write_chunk_to_s3(boto3_session, s3_bucket, s3_path, chunk):
def remove_all_s3_objects_in_path(boto3_session, s3_bucket, s3_path):
    s3 = boto3_session.resource('s3')
    bucket = s3.Bucket(s3_bucket)

    for obj in bucket.objects.filter(Prefix=s3_path):
        obj.delete()

class Tests(unittest.TestCase):
    def test_write_chunks_to_s3(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path = 'tests/test_write_chunks_to_s3'
        s3_file = 'test.json'
        chunks = [{
            "name": "test",
        }]

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path)

        chunks = write_chunks_to_s3(s3_session, s3_bucket, s3_path, s3_file, chunks)

        flush_cache()

        # now read it back
        chunk = read_chunk_from_s3(s3_session, s3_bucket, s3_path, s3_file, 0)

        self.assertIsNotNone(chunk)
        self.assertEqual(chunk['name'], 'test')

    def test_yield_file_pairs_from_s3(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path1 = 'tests/test_yield_file_pairs_from_s3/1'
        s3_path2 = 'tests/test_yield_file_pairs_from_s3/2'
        s3_file1 = 'chunks1.json'
        s3_file2 = 'chunks2.json'

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path1)
        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path2)

        # write some chunks
        def get_chunks(offset, amount):
            return [
                {
                    "name": "test",
                    "i": i+offset,
                }
                for i in range(amount)
            ]

        write_chunks_to_s3(s3_session, s3_bucket, s3_path1, s3_file1, get_chunks(0, 3))
        write_chunks_to_s3(s3_session, s3_bucket, s3_path1, s3_file2, get_chunks(3, 3))
        write_chunks_to_s3(s3_session, s3_bucket, s3_path2, s3_file1, get_chunks(6, 4))
        write_chunks_to_s3(s3_session, s3_bucket, s3_path2, s3_file2, get_chunks(10, 4))

        flush_cache()

        # now read the file pairs
        s3_paths = [s3_path1, s3_path2]
        file_pairs = list(yield_file_pairs_from_s3(s3_session, s3_bucket, s3_paths))
        self.assertEqual(len(file_pairs), 4)

    def test_yield_chunks_from_s3(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path = 'tests/test_yield_chunks_from_s3'
        s3_file = 'chunks.json'

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path)

        # write some chunks
        chunks = [
            {
                "name": "test",
                "i": i,
            }
            for i in range(10)
        ]
        chunks = write_chunks_to_s3(s3_session, s3_bucket, s3_path, s3_file, chunks)

        flush_cache()

        # now read them back
        s3_paths = [s3_path]
        file_pair = (0, s3_file)
        chunks = list(yield_chunks_from_s3(s3_session, s3_bucket, s3_paths, file_pair))

        self.assertEqual(len(chunks), 10)
        for i, chunk in enumerate(chunks):
            self.assertEqual(chunk['i'], i)

    def test_create_dumb_index(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path = 'tests/test_create_dumb_index'
        s3_file = 'chunks.json'

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path)

        # write some chunks
        chunks = [
            {
                "name": "test",
                "vector": [i, i, i],
            }
            for i in range(10)
        ]
        write_chunks_to_s3(s3_session, s3_bucket, s3_path, s3_file, chunks)

        flush_cache()

        def get_vector(chunk):
            return chunk['vector']

        # create a dumb index        
        dumb_index = create_dumb_index(s3_session, s3_bucket, [s3_path], get_vector)

        print(dumb_index)

        self.assertIsNotNone(dumb_index)
        self.assertEqual(len(dumb_index['paths']), 1)
        self.assertEqual(dumb_index['paths'][0], s3_path)

        self.assertEqual(len(dumb_index['file_pairs']), 1)
        self.assertEqual(dumb_index['file_pairs'][0], (0, s3_file))

        self.assertEqual(len(dumb_index['triples']), 10)
        for i, triple in enumerate(dumb_index['triples']):
            vector, fileix, chunkix = triple
            self.assertEqual(vector, [i, i, i])
            self.assertEqual(fileix, 0)
            self.assertEqual(chunkix, i)

    def test_get_dumb_index_bytes(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path = 'tests/test_get_dumb_index_bytes'
        s3_file = 'chunks.json'

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path)

        # write some chunks
        chunks = [
            {
                "name": "test",
                "vector": [1/(i+1), 1/(i+1), 1/(i+1)],
            }
            for i in range(10)
        ]
        write_chunks_to_s3(s3_session, s3_bucket, s3_path, s3_file, chunks)

        flush_cache()

        def get_vector(chunk):
            return chunk['vector']
        
        # create a dumb index
        s3_paths = [s3_path]
        dumb_index = create_dumb_index(s3_session, s3_bucket, s3_paths, get_vector)

        # get the dumb index as bytes
        dumb_index_bytes = get_dumb_index_bytes(dumb_index, 1, 3)

        # now get the dumb index from bytes
        dumb_index_from_bytes = get_dumb_index_from_bytes(dumb_index_bytes)

        # the dumb index from bytes should be the same as the original dumb index
        self.assertEqual(dumb_index, dumb_index_from_bytes)

    def test_write_dumb_index_to_s3(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path = 'tests/test_write_dumb_index_to_s3'
        s3_file = 'chunks.json'
        s3_path_dumb_index = s3_path + '/dumb_index'
        s3_file_dumb_index = 'dumbo.dumb'

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path)

        # write some chunks
        chunks = [
            {
                "name": "test",
                "vector": [1/(i+1), 1/(i+1), 1/(i+1)],
            }
            for i in range(10)
        ]

        write_chunks_to_s3(s3_session, s3_bucket, s3_path, s3_file, chunks)

        flush_cache()

        def get_vector(chunk):
            return chunk['vector']
        
        # create a dumb index
        dumb_index = create_dumb_index(s3_session, s3_bucket, [s3_path], get_vector)

        # write the dumb index to S3
        write_dumb_index_to_s3(s3_session, s3_bucket, s3_path_dumb_index, s3_file_dumb_index, dumb_index, 1, 3)

        # read the dumb index from S3
        dumb_index_from_s3 = read_dumb_index_from_s3(s3_session, s3_bucket, s3_path_dumb_index, s3_file_dumb_index)

        # the dumb index from S3 should be the same as the original dumb index
        self.assertEqual(dumb_index, dumb_index_from_s3)

    def test_get_chunks_from_dumb_index(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        s3_path = 'tests/test_get_chunks_from_dumb_index'
        s3_file = 'chunks.json'

        remove_all_s3_objects_in_path(s3_session, s3_bucket, s3_path)

        # write some chunks
        chunks = [
            {
                "name": "test",
                "i": i,
                "vector": [1/(i+1), 1/(i+1), 1/(i+1)],
            }
            for i in range(10)
        ]

        write_chunks_to_s3(s3_session, s3_bucket, s3_path, s3_file, chunks)

        flush_cache()

        def get_vector(chunk):
            return chunk['vector']
        
        # create a dumb index
        dumb_index = create_dumb_index(s3_session, s3_bucket, [s3_path], get_vector)

        # get the chunks from the dumb index
        page_chunks = get_chunks_from_dumb_index(s3_session, s3_bucket, dumb_index, 3, 2)

        # there should be 2 chunks
        self.assertEqual(len(page_chunks), 2)

        # the first chunk should be the fourth chunk in the dumb index (check using i)
        self.assertEqual(page_chunks[0]['i'], 3)

        # the second chunk should be the fifth chunk in the dumb index (check using i)
        self.assertEqual(page_chunks[1]['i'], 4)