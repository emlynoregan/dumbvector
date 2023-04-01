# In this program we open some chunk files and create indices from them. We then write the indices to S3.

import time
start_time = time.time()

import argparse
from dumbvector.dumb_index import create_dumb_index, get_dumb_index_file_writer
from dumbvector.dumb_index_s3 import get_dumb_index_s3_file_and_cache_writer
from dumbvector.docs import path_to_docs_list, get_docs_file_writer
from dumbvector.docs_s3 import s3_to_yield_docs
from dumbvector.util import time_function
import openai
import json

end_time = time.time()
print (f'import time: {end_time - start_time}')

def read_credentials():
    with open('credentials.json', 'r') as f:
        return json.load(f)

def main():
    # usage: python create_index.py index_name index_path docs_path [--docs_from_s3]

    parser = argparse.ArgumentParser()

    parser.add_argument('index_name', help='the name of the index file to create')
    parser.add_argument('index_path', help='the path of the index file to create')
    parser.add_argument('docs_path', help='the path of the docs file to create')
    parser.add_argument('--docs_from_s3', help='if specified, the docs are read from S3', action='store_true')

    args = parser.parse_args()

    index_name = args.index_name
    index_path = args.index_path
    docs_path = args.docs_path
    docs_from_s3 = args.docs_from_s3

    # read the credentials
    credentials = read_credentials()

    has_aws = 'aws_access_key_id' in credentials

    if has_aws:
        import boto3
        boto3_session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name'],
        )
        s3_bucket = credentials['s3_bucket']
        docs_writer = get_docs_file_writer(docs_path, overwrite=False)
        index_writer = get_dumb_index_s3_file_and_cache_writer(index_path, boto3_session, s3_bucket, "openai_examples/docs")
    else:
        index_writer = get_dumb_index_file_writer(index_path)
        if docs_from_s3:
            raise Exception("docs_from_s3 specified but no aws credentials")

    openai.api_key = credentials['openaikey']

    # first read all the docs
    if docs_from_s3:
        all_docs = []
        print ("reading docs from s3")
        for docs in s3_to_yield_docs(boto3_session, s3_bucket, 'openai_examples/docs'):
            print (f"writing docs {docs['name']}")
            docs_writer(docs)
            all_docs.append(docs)
    else:
        all_docs = time_function(path_to_docs_list)(docs_path)

    # create the index
    def get_vector(doc):
        return doc['embedding']

    dumb_index = time_function(create_dumb_index)(index_name, all_docs, get_vector)

    time_function(index_writer)(dumb_index)

    print ("done")

if __name__ == '__main__':
    main()

