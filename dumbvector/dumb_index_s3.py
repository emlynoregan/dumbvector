from dumbvector.dumb_index import dumb_index_to_binary, binary_to_dumb_index, get_dumb_index_file_and_cache_reader, get_dumb_index_file_and_cache_writer
import botocore

def sanitize_dumb_index_name_for_s3(name):
    # disallowed characters for amazon s3: /, \, :, *, ?, ", <, >, |
    disallowed_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|"]
    fixed_name = name
    for char in disallowed_chars:
        fixed_name = fixed_name.replace(char, "_")
    return fixed_name

def s3_full_pathname_for_dumb_index(name, s3_path):
    fixed_name = sanitize_dumb_index_name_for_s3(name)
    if s3_path:
        return f"{s3_path}/{fixed_name}.dumb_index"
    else:
        return f"{fixed_name}.dumb_index"
    
def s3_dumb_index_exists(boto3_session, s3_bucket, s3_path, name):
    s3 = boto3_session.resource('s3')
    path = s3_full_pathname_for_dumb_index(name, s3_path)
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

def dumb_index_to_s3(dumb_index, boto3_session, s3_bucket, s3_path=None):
    s3 = boto3_session.resource('s3')
    name = dumb_index.get("name")

    try:
        if not s3_dumb_index_exists(boto3_session, s3_bucket, s3_path, name):
            path = s3_full_pathname_for_dumb_index(name, s3_path)

            binary = dumb_index_to_binary(dumb_index)

            s3_object = s3.Object(s3_bucket, path)

            s3_object.put(Body=binary)
    finally:
        s3.meta.client.close()

    return dumb_index

def s3_to_dumb_index(boto3_session, s3_bucket, s3_path, name):
    s3 = boto3_session.resource('s3')
    path = s3_full_pathname_for_dumb_index(name, s3_path)
    try:
        # an s3 error will be thrown if the object doesn't exist
        s3_object = s3.Object(s3_bucket, path)
        binary = s3_object.get()['Body'].read()
        return binary_to_dumb_index(binary)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return None
        else:
            # Something else has gone wrong.
            raise

def s3_to_yield_dumb_indexes(boto3_session, s3_bucket, s3_path):
    s3 = boto3_session.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    try:
        for s3_object in bucket.objects.filter(Prefix=s3_path):
            if s3_object.key.endswith(".dumb_index"):
                binary = s3_object.get()['Body'].read()
                yield binary_to_dumb_index(binary)
    finally:
        s3.meta.client.close()

def get_dumb_index_s3_reader(boto3_session, s3_bucket, s3_path=None, fallback_reader=None):
    def read_dumb_index(name):
        dumb_index = s3_to_dumb_index(boto3_session, s3_bucket, s3_path, name)
        if dumb_index is None and fallback_reader is not None:
            dumb_index = fallback_reader(name)
        return dumb_index
    return read_dumb_index

def get_dumb_index_s3_writer(boto3_session, s3_bucket, s3_path=None, overwrite=False, next_writer=None):
    def write_dumb_index(dumb_index):
        name = dumb_index.get("name")
        if not name:
            raise Exception("dumb_index must have a name")
        need_write = overwrite or not s3_dumb_index_exists(boto3_session, s3_bucket, s3_path, name)
        if need_write:
            dumb_index_to_s3(dumb_index, boto3_session, s3_bucket, s3_path)
        if next_writer is not None:
            next_writer(dumb_index)
    return write_dumb_index

def get_dumb_index_s3_file_and_cache_reader(file_path, boto3_session, s3_bucket, s3_path=None, fallback_reader=None):
    return get_dumb_index_file_and_cache_reader(file_path, get_dumb_index_s3_reader(boto3_session, s3_bucket, s3_path, fallback_reader))

def get_dumb_index_s3_file_and_cache_writer(file_path, boto3_session, s3_bucket, s3_path=None, overwrite=False, next_writer=None):
    return get_dumb_index_file_and_cache_writer(file_path, overwrite, get_dumb_index_s3_writer(boto3_session, s3_bucket, s3_path, overwrite, next_writer))

