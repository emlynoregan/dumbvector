import unittest
import json
import boto3

from dumb_vector_s3 import write_chunks_to_s3, read_chunk_from_s3, create_dumb_index, \
    write_dumb_index_to_s3, read_dumb_index_from_s3, \
    yield_chunks_from_dumb_index, sort_dumb_index_by_similarity, vector_to_bytes, bytes_to_vector, \
    flush_cache, dumb_index_exists_on_s3, create_dimension_mask, filter_vector_by_mask
from openai.embeddings_utils import get_embedding
import openai
import base64

# timing
import time

def read_credentials():
    with open('tests/credentials.json', 'r') as f:
        return json.load(f)

# def write_chunk_to_s3(boto3_session, s3_bucket, s3_path, chunk):
def remove_all_s3_objects_in_path(boto3_session, s3_bucket, s3_path):
    s3 = boto3_session.resource('s3')
    bucket = s3.Bucket(s3_bucket)

    for obj in bucket.objects.filter(Prefix=s3_path):
        obj.delete()

def time_function(func):
    def timed(*args, **kw):
        ts = time.time()
        try:
            result = func(*args, **kw)
        finally:
            te = time.time()

            print ('%r  %2.2f sec' % \
                (func.__name__, te-ts))
        return result

    return timed

class Tests(unittest.TestCase):
    s3_path = 'tests/test_using_real_data1_smaller'
    s3_chunk_file = 'chunks.json'
    s3_index_file = 'yt_embeddings_db_5tmGKTNW8DQ.dumb'
    source = 'tests/yt_embeddings_db_5tmGKTNW8DQ.json'
    dimension_mask = None

    # in the setup, we create a bunch of chunks and write them to S3
    def setUp(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        
        # remove_all_s3_objects_in_path(s3_session, s3_bucket, self.s3_chunk_path)
        # remove_all_s3_objects_in_path(s3_session, s3_bucket, self.s3_path)

        # check to see if the first chunk exists
        # if it does, then we don't need to create any chunks
        print ("try to get the first chunk")
        chunk = time_function(read_chunk_from_s3)(s3_session, s3_bucket, self.s3_path, self.s3_chunk_file, 0)

        if not chunk:
            # open a file full of chunks
            with open(self.source, 'r') as f:
                src_chunks = json.load(f)

            if not src_chunks:
                raise Exception("no chunks found in file")

            # make the chunks 10 times larger by repeating them
            print (f"making chunks 10 times larger")
            chunks = []
            for i in range(10):
                copy_chunks = [
                    {**chunk} for chunk in src_chunks
                ]
                chunks.extend(copy_chunks)

            print (f"number of chunks: {len(chunks)}")
            # write the chunks to S3
            for chunk in chunks:
                embedding = chunk["embedding"]
                embedding_bytes = vector_to_bytes(embedding, 16)                
                chunk["embedding"] = base64.b64encode(embedding_bytes).decode('utf-8')
                chunk["vector_type"] = 16

            print (f"writing chunks")
            time_function(write_chunks_to_s3)(s3_session, s3_bucket, self.s3_path, self.s3_chunk_file, chunks)
            chunk = chunks[0]

        print ("try to get the dumb index")
        # dumb_index = time_function(dumb_index_exists_on_s3)(s3_session, s3_bucket, self.s3_path, self.s3_index_file)
        dumb_index = time_function(read_dumb_index_from_s3)(s3_session, s3_bucket, self.s3_path, self.s3_index_file)

        if not dumb_index:
            def get_vector(chunk):
                # return chunk['embedding']
                embedding_bytes_b64 = chunk['embedding']
                embedding_bytes = base64.b64decode(embedding_bytes_b64)
                embedding = bytes_to_vector(embedding_bytes, 16)
                return embedding
            
            # create a dumb index
            print ("creating dumb index")
            dumb_index = time_function(create_dumb_index)(s3_session, s3_bucket, [self.s3_path], get_vector, dimension_threshold=0.08)
            
            self.dimension_mask = dumb_index.get("dimension_mask")
            print (f"dimension_mask: {self.dimension_mask}")

            # get the number of dimensions
            first_vector = dumb_index['triples'][0][0]
            num_dimensions = len(first_vector)
            print (f"num_dimensions: {num_dimensions}")

            # write the dumb index to S3
            print ("writing dumb index")
            time_function(write_dumb_index_to_s3)(s3_session, s3_bucket, self.s3_path, self.s3_index_file, dumb_index, 16, num_dimensions)


        # need to get the dimension mask from the dumb index
                                   
    def test_using_real_data1(self):
        credentials = read_credentials()
        s3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']

        openai.api_key = credentials['openaikey']
        
        print ("getting dumb index")
        dumb_index = time_function(read_dumb_index_from_s3)(s3_session, s3_bucket, self.s3_path, self.s3_index_file)

        topic = "memory problems in deep learning"
        topic_embedding = get_embedding(topic, "text-embedding-ada-002")

        filtered_topic_embedding = filter_vector_by_mask(topic_embedding, self.dimension_mask)

        # sort the chunks by distance to the topic
        print ("sorting dumb index")
        sorted_dumb_index = time_function(sort_dumb_index_by_similarity)(dumb_index, filtered_topic_embedding)
        
        # get the closest 10 chunks
        print ("getting chunks")
        @time_function
        def get_page_chunks():
            return list(yield_chunks_from_dumb_index(s3_session, s3_bucket, sorted_dumb_index, 0, 10))
        
        page_chunks = get_page_chunks()

        self.assertEqual(len(page_chunks), 10)
        self.assertEqual(page_chunks[0]['text'], "Deep learning's memory wall problem and")

        # print the chunks
        for chunk in page_chunks:
            print(chunk['text'])

        

        
