from dumbvector.docs import docs_to_binary, binary_to_docs, get_docs_file_and_cache_reader, get_docs_file_and_cache_writer, get_docs_file_writer
import botocore
from dumbvector.util import time_function

def sanitize_docs_name_for_s3(name):
    # disallowed characters for amazon s3: /, \, :, *, ?, ", <, >, |
    disallowed_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|"]
    fixed_name = name
    for char in disallowed_chars:
        fixed_name = fixed_name.replace(char, "_")
    return fixed_name

def s3_full_pathname_for_docs(name, s3_path):
    fixed_name = sanitize_docs_name_for_s3(name)
    if s3_path:
        return f"{s3_path}/{fixed_name}.docs"
    else:
        return f"{fixed_name}.docs"

def s3_docs_exists(boto3_session, s3_bucket, s3_path, name):
    s3 = boto3_session.resource('s3')
    path = s3_full_pathname_for_docs(name, s3_path)
    try:
        # an s3 error will be thrown if the object doesn't exist
        s3.ObjectSummary(s3_bucket, path).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise

def docs_to_s3(docs, boto3_session, s3_bucket, s3_path=None):
    s3 = boto3_session.resource('s3')
    name = docs.get("name")

    try:
        if not time_function(s3_docs_exists)(boto3_session, s3_bucket, s3_path, name):
            path = s3_full_pathname_for_docs(name, s3_path)

            binary = docs_to_binary(docs)

            s3_object = s3.Object(s3_bucket, path)

            print (f'writing {path} to s3, size {len(binary)} bytes')

            time_function(s3_object.put)(Body=binary)
    finally:
        s3.meta.client.close()

    return docs

def s3_to_docs(boto3_session, s3_bucket, s3_path, name):
    s3 = boto3_session.resource('s3')
    path = s3_full_pathname_for_docs(name, s3_path)
    try:
        # an s3 error will be thrown if the object doesn't exist
        s3_object = s3.Object(s3_bucket, path)
        binary = s3_object.get()['Body'].read()
        return binary_to_docs(binary)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return None
        else:
            # Something else has gone wrong.
            raise

def s3_to_yield_docs(boto3_session, s3_bucket, s3_path):
    s3 = boto3_session.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    try:
        for s3_object in bucket.objects.filter(Prefix=s3_path):
            if s3_object.key.endswith(".docs"):
                binary = s3_object.get()['Body'].read()
                yield binary_to_docs(binary)
    finally:
        s3.meta.client.close()

def get_docs_s3_reader(boto3_session, s3_bucket, s3_path=None, fallback_reader=None):
    def read_docs(name):
        docs = s3_to_docs(boto3_session, s3_bucket, s3_path, name)
        if docs is None and fallback_reader is not None:
            docs = fallback_reader(name)
        return docs
    return read_docs

def get_docs_s3_writer(boto3_session, s3_bucket, s3_path=None, overwrite=False, next_writer=None):
    def write_docs(docs):
        name = docs.get("name")
        if not name:
            raise Exception("docs must have a name")
        need_write = overwrite or not time_function(s3_docs_exists)(boto3_session, s3_bucket, s3_path, name)
        if need_write:
            time_function(docs_to_s3)(docs, boto3_session, s3_bucket, s3_path)
        if next_writer is not None:
            next_writer(docs)
    return write_docs

def get_docs_s3_file_and_cache_reader(file_path, boto3_session, s3_bucket, s3_path=None, fallback_reader=None):
    return get_docs_file_and_cache_reader(file_path, get_docs_s3_reader(boto3_session, s3_bucket, s3_path, fallback_reader))

def get_docs_s3_file_and_cache_writer(file_path, boto3_session, s3_bucket, s3_path=None, overwrite=False, next_writer=None):
    return get_docs_file_and_cache_writer(file_path, overwrite, get_docs_s3_writer(boto3_session, s3_bucket, s3_path, overwrite, next_writer))

def get_docs_s3_file_writer(file_path, boto3_session, s3_bucket, s3_path=None, overwrite=False, next_writer=None):
    return get_docs_file_writer(file_path, overwrite, get_docs_s3_writer(boto3_session, s3_bucket, s3_path, overwrite, next_writer))