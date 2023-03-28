
'''
This file implements a simple system for writing a very simple vector datastore to S3. The caller must 
provide an s3 bucket to work with.

The process of semantic search is supported as follows:
1: We write "chunks" to S3. A chunk is a single record that we want to search on. It is a dictionary with 
one or more embeddings, and any other data and metadata. It will likely include the text that the embedding
was generated from, and the embedding itself. It is a json dictionary. 

Multiple chunks are written to S3 as a single file. The file is a json list of chunks. 

Note that chunks can be written to any s3 path in the given s3 bucket. The path is specified by the caller.

2: We write a "dumb index" to S3. A dumb index is a list of (vector, fileix, chunkix) triples.
The vector is any vector of numbers, the fileix is an index into a list of files in the s3 bucket, and 
the chunkix is the position in the file where the chunk is located.

This index is created by downloading desired chunks from s3, and mapping over them with an index creation function.
That function takes a chunk and returns a vector, or None if the chunk should be skipped. The dumb index can then 
be constructed and written to s3.

3: To use the index, we download it from s3 and load it into memory. We then search it by mapping over the 
entire index, using cosign similarity to calculate similarity scores between zero and one, and sorting
the results by similarity score. Then, as the caller asks for results from this list, we download the
chunks from s3 and return them to the caller.

-- 

We need a compact file format for the dumb index. We will use a binary format, with a header, and then a list of
pairs.

The header will be structured as follows:
- 4 bytes: magic number, 0xfeedface
- 4 bytes: version number, 0x00000001
- 4 bytes: number of dimensions in the vectors
- 1 byte: type of the vectors 
    0 = float32 
    1 = float64
    8 = int8
    9 = int16
    10 = int32
    16 = uint8
    17 = uint16
    18 = uint32
- 4 bytes: number of paths
- 4 bytes: number of files
- 4 bytes: number of pairs
- 4 bytes: number of bytes in the path table
- 4 bytes: number of bytes in the file table
- 4 bytes: number of bytes in the pair table

The path table will be structured as follows:
- for each path:
    - 4 bytes: number of bytes in the path
    - n bytes: the path (utf-8 encoded)

The file table will be structured as follows:
- for each file:
    - 4 bytes: pathix
    - 4 bytes: number of bytes in the file name
    - n bytes: the file name (utf-8 encoded)

The triple table will be structured as follows:
- for each triple:
    - for each dimension in the vector:
        - k bytes: the value of the dimension, where k is the number of bytes in the vector type
    - 4 bytes: the fileix
    - 4 bytes: the chunkix

'''

import json
import os
import boto3
import botocore
import uuid
import struct

C_MAGIC_NUMBER = 0xfeedface

C_VECTORTYPE_FLOAT32 = 0
C_VECTORTYPE_FLOAT64 = 1
C_VECTORTYPE_INT8 = 8
C_VECTORTYPE_INT16 = 9
C_VECTORTYPE_INT32 = 10
C_VECTORTYPE_UINT8 = 16
C_VECTORTYPE_UINT16 = 17
C_VECTORTYPE_UINT32 = 18

def convert_dimension_value_float_to_dumb_vector_bytes(value, vector_type):
    float_value = float(value)
    # we are expecting a value between -1 and 1
    if float_value < -1.0 or float_value > 1.0:
        raise Exception(f"Value {float_value} must be between -1 and 1 inclusive")

    # float_value is a float64, which is 8 bytes. We will convert it to the desired vector type.

    if vector_type == 0:
        return struct.pack('<f', float_value)
    elif vector_type == 1:
        return struct.pack('<d', float_value)
    elif vector_type == 8:
        # An int8 ranges from -128 to 127. We will scale the float32 value to be in that range.
        int8_value = int(float_value * 127.0)
        return int8_value.to_bytes(1, byteorder='little', signed=True)
    elif vector_type == 9:
        # An int16 ranges from -32768 to 32767. We will scale the float32 value to be in that range.
        int16_value = int(float_value * 32767.0)
        return int16_value.to_bytes(2, byteorder='little', signed=True)
    elif vector_type == 10:
        # An int32 ranges from -2147483648 to 2147483647. We will scale the float32 value to be in that range.
        int32_value = int(float_value * 2147483647.0)
        return int32_value.to_bytes(4, byteorder='little', signed=True)
    elif vector_type == 16:
        # An uint8 ranges from 0 to 255. We will scale the float32 value to be in that range.
        uint8_value = int((float_value + 1) * 127.0)
        return uint8_value.to_bytes(1, byteorder='little', signed=False)
    elif vector_type == 17:
        # An uint16 ranges from 0 to 65535. We will scale the float32 value to be in that range.
        uint16_value = int((float_value + 1) * 32767.0)
        return uint16_value.to_bytes(2, byteorder='little', signed=False)
    elif vector_type == 18:
        # An uint32 ranges from 0 to 4294967295. We will scale the float32 value to be in that range.
        uint32_value = int((float_value + 1) * 2147483647.0)
        return uint32_value.to_bytes(4, byteorder='little', signed=False)
    else:
        raise Exception(f"Unknown vector type {vector_type}")
    
def convert_dumb_vector_bytes_to_dimension_value_float(dumb_vector_bytes, vector_type):
    if vector_type == 0:
        return struct.unpack('<f', dumb_vector_bytes)[0]
    elif vector_type == 1:
        return struct.unpack('<d', dumb_vector_bytes)[0]
    elif vector_type == 8:
        # An int8 ranges from -128 to 127. We will scale the float32 value to be in that range.
        int8_value = int.from_bytes(dumb_vector_bytes, byteorder='little', signed=True)
        return float(int8_value) / 127.0
    elif vector_type == 9:
        # An int16 ranges from -32768 to 32767. We will scale the float32 value to be in that range.
        int16_value = int.from_bytes(dumb_vector_bytes, byteorder='little', signed=True)
        return float(int16_value) / 32767.0
    elif vector_type == 10:
        # An int32 ranges from -2147483648 to 2147483647. We will scale the float32 value to be in that range.
        int32_value = int.from_bytes(dumb_vector_bytes, byteorder='little', signed=True)
        return float(int32_value) / 2147483647.0
    elif vector_type == 16:
        # An uint8 ranges from 0 to 255. We will scale the float32 value to be in that range.
        uint8_value = int.from_bytes(dumb_vector_bytes, byteorder='little', signed=False)
        return float(uint8_value) / 127.0 - 1.0
    elif vector_type == 17:
        # An uint16 ranges from 0 to 65535. We will scale the float32 value to be in that range.
        uint16_value = int.from_bytes(dumb_vector_bytes, byteorder='little', signed=False)
        return float(uint16_value) / 32767.0 - 1.0
    elif vector_type == 18:
        # An uint32 ranges from 0 to 4294967295. We will scale the float32 value to be in that range.
        uint32_value = int.from_bytes(dumb_vector_bytes, byteorder='little', signed=False)
        return float(uint32_value) / 2147483647.0 - 1.0
    else:
        raise Exception(f"Unknown vector type {vector_type}")

def number_of_bytes_for_vector_type(vector_type):
    if vector_type == 0:
        return 4
    elif vector_type == 1:
        return 8
    elif vector_type == 8:
        return 1
    elif vector_type == 9:
        return 2
    elif vector_type == 10:
        return 4
    elif vector_type == 16:
        return 1
    elif vector_type == 17:
        return 2
    elif vector_type == 18:
        return 4
    else:
        raise Exception(f"Unknown vector type {vector_type}")

# dumb_vector_bytes = bytearray()

def vector_to_bytes(vector, vector_type):
    dumb_vector_bytes = bytearray()
    for vector_value in vector:
        vector_value_bytes = convert_dimension_value_float_to_dumb_vector_bytes(vector_value, vector_type)
        dumb_vector_bytes += vector_value_bytes
    return dumb_vector_bytes

def bytes_to_vector(dumb_vector_bytes, vector_type):
    vector = []
    num_dimensions = len(dumb_vector_bytes) // number_of_bytes_for_vector_type(vector_type)
    for i in range(num_dimensions):
        vector_value_bytes = dumb_vector_bytes[i * number_of_bytes_for_vector_type(vector_type): (i + 1) * number_of_bytes_for_vector_type(vector_type)]
        vector_value = convert_dumb_vector_bytes_to_dimension_value_float(vector_value_bytes, vector_type)
        vector.append(vector_value)
    return vector

def add_triple_table_bytes(dumb_vector_bytes, triples, vector_type):
    # each pair is (vector, fileix)
    for triple in triples:
        vector, fileix, chunkix = triple
        for vector_value in vector:
            vector_value_bytes = convert_dimension_value_float_to_dumb_vector_bytes(vector_value, vector_type)
            dumb_vector_bytes += vector_value_bytes
        dumb_vector_bytes += fileix.to_bytes(4, byteorder='little', signed=False) # pathix is a positive int or zero
        dumb_vector_bytes += chunkix.to_bytes(4, byteorder='little', signed=False) # chunkix is a positive int or zero

    return dumb_vector_bytes

def add_path_table_bytes(dumb_vector_bytes, paths):
    for path in paths:
        path_bytes = path.encode('utf-8')
        dumb_vector_bytes += len(path_bytes).to_bytes(4, byteorder='little', signed=False)
        dumb_vector_bytes += path_bytes

    return dumb_vector_bytes

def add_file_table_bytes(dumb_vector_bytes, file_pairs):
    for file_pair in file_pairs:
        pathix, file = file_pair
        dumb_vector_bytes += pathix.to_bytes(4, byteorder='little', signed=False)
        file_bytes = file.encode('utf-8')
        dumb_vector_bytes += len(file_bytes).to_bytes(4, byteorder='little', signed=False)
        dumb_vector_bytes += file_bytes

    return dumb_vector_bytes

def add_header_bytes(dumb_vector_bytes, vector_type, num_dimensions, num_triples, num_files, num_paths, num_triple_table_bytes, num_file_table_bytes, num_path_table_bytes):
    dumb_vector_bytes += C_MAGIC_NUMBER.to_bytes(4, byteorder='little', signed=False)
    version_number = 0x00000001
    dumb_vector_bytes += version_number.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += num_dimensions.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += vector_type.to_bytes(1, byteorder='little', signed=False)
    dumb_vector_bytes += num_paths.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += num_files.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += num_triples.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += num_path_table_bytes.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += num_file_table_bytes.to_bytes(4, byteorder='little', signed=False)
    dumb_vector_bytes += num_triple_table_bytes.to_bytes(4, byteorder='little', signed=False)
    return dumb_vector_bytes

def get_dumb_index_bytes(dumb_index, vector_type, num_dimensions):
    triples = dumb_index["triples"]
    paths = dumb_index["paths"]
    file_pairs = dumb_index["file_pairs"]

    path_table_bytes = bytearray()
    path_table_bytes = add_path_table_bytes(path_table_bytes, paths)

    file_table_bytes = bytearray()
    file_table_bytes = add_file_table_bytes(file_table_bytes, file_pairs)

    triple_table_bytes = bytearray()
    triple_table_bytes = add_triple_table_bytes(triple_table_bytes, triples, vector_type)

    dumb_index_bytes = bytearray()
    dumb_index_bytes = add_header_bytes(
        dumb_index_bytes, vector_type, num_dimensions, 
        len(triples), len(file_pairs), len(paths), 
        len(triple_table_bytes), len(file_table_bytes), len(path_table_bytes)
    )

    dumb_index_bytes += path_table_bytes
    dumb_index_bytes += file_table_bytes
    dumb_index_bytes += triple_table_bytes

    return dumb_index_bytes

def get_triples_from_triple_table_bytes(triple_table_bytes, vector_type, num_dimensions, num_triples):
    # reverse of add_triple_table_bytes
    triples = []
    vector_value_bytes_length = number_of_bytes_for_vector_type(vector_type)

    pos = 0

    for i in range(num_triples):
        # read one pair
        vector = []

        for j in range(num_dimensions):
            offset = pos + j * vector_value_bytes_length
            vector_value_bytes = triple_table_bytes[offset:offset+vector_value_bytes_length]
            vector_value = convert_dumb_vector_bytes_to_dimension_value_float(vector_value_bytes, vector_type)
            vector.append(vector_value)
        
        pos += num_dimensions * vector_value_bytes_length

        fileix_bytes = triple_table_bytes[pos:pos+4]
        fileix = int.from_bytes(fileix_bytes, byteorder='little', signed=False)

        pos += 4

        chunkix_bytes = triple_table_bytes[pos:pos+4]
        chunkix = int.from_bytes(chunkix_bytes, byteorder='little', signed=False)

        pos += 4

        triple = (vector, fileix, chunkix)
        triples.append(triple)
    
    return triples

def get_paths_from_path_table_bytes(path_table_bytes, num_paths):
    # reverse of add_path_table_bytes
    paths = []
    pos = 0
    for i in range(num_paths):
        path_length_bytes = path_table_bytes[pos:pos+4]
        path_length = int.from_bytes(path_length_bytes, byteorder='little', signed=False)
        pos += 4
        path_bytes = path_table_bytes[pos:pos+path_length]
        path = path_bytes.decode('utf-8')
        pos += path_length
        paths.append(path)
    return paths

def get_file_pairs_from_file_table_bytes(file_table_bytes, num_files):
    # reverse of add_file_table_bytes
    file_pairs = []
    pos = 0
    for i in range(num_files):
        pathix_bytes = file_table_bytes[pos:pos+4]
        pathix = int.from_bytes(pathix_bytes, byteorder='little', signed=False)
        pos += 4
        file_length_bytes = file_table_bytes[pos:pos+4]
        file_length = int.from_bytes(file_length_bytes, byteorder='little', signed=False)
        pos += 4
        file_bytes = file_table_bytes[pos:pos+file_length]
        file = file_bytes.decode('utf-8')
        pos += file_length
        file_pair = (pathix, file)
        file_pairs.append(file_pair)
    return file_pairs

def get_header_from_dumb_index_bytes(dumb_index_bytes):
    # reverse of add_header_bytes
    magic_number_bytes = dumb_index_bytes[0:4]
    magic_number = int.from_bytes(magic_number_bytes, byteorder='little', signed=False)
    if magic_number != C_MAGIC_NUMBER:
        raise Exception("This is not a dumb index file (magic number not found)")
    
    version_number_bytes = dumb_index_bytes[4:8]
    version_number = int.from_bytes(version_number_bytes, byteorder='little', signed=False)
    if version_number != 0x00000001:
        raise Exception("Version number not supported in dumb index file (expected 1, got " + str(version_number) + ")")

    num_dimensions_bytes = dumb_index_bytes[8:12]
    num_dimensions = int.from_bytes(num_dimensions_bytes, byteorder='little', signed=False)
    vector_type_bytes = dumb_index_bytes[12:13]
    vector_type = int.from_bytes(vector_type_bytes, byteorder='little', signed=False)
    num_paths_bytes = dumb_index_bytes[13:17]
    num_paths = int.from_bytes(num_paths_bytes, byteorder='little', signed=False)
    num_files_bytes = dumb_index_bytes[17:21]
    num_files = int.from_bytes(num_files_bytes, byteorder='little', signed=False)
    num_triples_bytes = dumb_index_bytes[21:25]
    num_triples = int.from_bytes(num_triples_bytes, byteorder='little', signed=False)
    num_path_table_bytes_bytes = dumb_index_bytes[25:29]
    num_path_table_bytes = int.from_bytes(num_path_table_bytes_bytes, byteorder='little', signed=False)
    num_file_table_bytes_bytes = dumb_index_bytes[29:33]
    num_file_table_bytes = int.from_bytes(num_file_table_bytes_bytes, byteorder='little', signed=False)
    num_triple_table_bytes_bytes = dumb_index_bytes[33:37]
    num_triple_table_bytes = int.from_bytes(num_triple_table_bytes_bytes, byteorder='little', signed=False)

    remainder_bytes = dumb_index_bytes[37:]

    return magic_number, version_number, num_dimensions, vector_type, \
        num_paths, num_files, num_triples, \
        num_path_table_bytes, num_file_table_bytes, num_triple_table_bytes, \
        remainder_bytes

def get_dumb_index_from_bytes(dumb_index_bytes):
    magic_number, version_number, num_dimensions, vector_type, \
        num_paths, num_files, num_triples, \
        num_path_table_bytes, num_file_table_bytes, num_triple_table_bytes, \
        remainder_bytes = get_header_from_dumb_index_bytes(dumb_index_bytes)
    
    path_table_bytes = remainder_bytes[0:num_path_table_bytes]
    file_table_bytes_offset = num_path_table_bytes
    file_table_bytes = remainder_bytes[file_table_bytes_offset:file_table_bytes_offset+num_file_table_bytes]
    triple_table_bytes_offset = num_path_table_bytes+num_file_table_bytes
    triple_table_bytes = remainder_bytes[triple_table_bytes_offset:triple_table_bytes_offset+num_triple_table_bytes]

    paths = get_paths_from_path_table_bytes(path_table_bytes, num_paths)
    file_pairs = get_file_pairs_from_file_table_bytes(file_table_bytes, num_files)
    triples = get_triples_from_triple_table_bytes(triple_table_bytes, vector_type, num_dimensions, num_triples)

    return {
        "paths": paths,
        "file_pairs": file_pairs,
        "triples": triples
    }

# C_CHUNKIX = "_chunkix_"

def write_chunks_to_s3(boto3_session, s3_bucket, s3_path, s3_file, chunks):
    s3 = boto3_session.resource('s3')

    try:
        # check the extension
        if not s3_file.endswith(".json"):
            raise Exception("s3_file must end with .json")
        
        # check there are no path separators in the file name
        if "/" in s3_file:
            raise Exception("s3_file must not contain any path separators")

        # for chunkix, chunk in enumerate(chunks):
        #     chunk[C_CHUNKIX] = chunkix

        path = f"{s3_path}/{s3_file}" if s3_path else f"{s3_file}"

        s3_object = s3.Object(s3_bucket, path)

        # REMOVED THIS CHECK, NOT WORTH IT
        # # check if the object already exists, using ObjectSummary

        # try:
        #     # an s3 error will be thrown if the object doesn't exist
        #     s3.ObjectSummary(s3_bucket, path).load()
        #     print ("chunk already exists in s3")
        #     return None
        #     # existing_chunk = read_chunk_from_s3(boto3_session, s3_bucket, s3_path, chunk_id, cache=False)
        #     # return existing_chunk
        # except botocore.exceptions.ClientError as e:
        #     print (e)
        #     #botocore.errorfactory.NoSuchKey
        #     if e.response['Error']['Code'] == "404":
        #         # The object does not exist.
        #         pass
        #     else:
        #         # Something else has gone wrong.
        #         raise

        # here we know the object doesn't exist, so we can write it
        chunks_json = json.dumps(chunks)
        s3_object.put(Body=chunks_json)
    finally:
        s3.meta.client.close()

    return chunks

def _get_chunks_from_s3_object(s3_object):
    try:
        chunks_json = s3_object.get()['Body'].read().decode('utf-8')
    except botocore.exceptions.ClientError as e:
        print (e)
        #botocore.errorfactory.NoSuchKey
        if e.response['Error']['Code'] == "NoSuchKey":
            # The object does not exist.
            return []
        else:
            # Something else has gone wrong.
            raise
    chunks = json.loads(chunks_json)
    return chunks

C_CHUNK_CACHE = {}
C_S3_KEY_CACHE = {}

def flush_cache():
    global C_CHUNK_CACHE
    C_CHUNK_CACHE = {}
    global C_S3_KEY_CACHE
    C_S3_KEY_CACHE = {}

def _calc_chunk_id(s3_bucket, s3_path, s3_file, chunkix):
    return f"{s3_bucket}/{s3_path}/{s3_file}/{chunkix}" if s3_path else f"{s3_bucket}/{s3_file}/{chunkix}"

def read_chunk_from_s3(boto3_session, s3_bucket, s3_path, s3_file, chunkix, read_through_cache=False):
    global C_CHUNK_CACHE
    
    s3 = boto3_session.resource('s3')

    try:
        chunk_id = _calc_chunk_id(s3_bucket, s3_path, s3_file, chunkix)
        
        if not read_through_cache:
            if chunk_id in C_CHUNK_CACHE:
                return C_CHUNK_CACHE[chunk_id]

        path = f"{s3_path}/{s3_file}" if s3_path else f"{s3_file}"
        s3_object = s3.Object(s3_bucket, path)
        chunks = _get_chunks_from_s3_object(s3_object)
        if chunks:
            chunkids = []
            for this_chunkix, chunk in enumerate(chunks):
                chunk_id = _calc_chunk_id(s3_bucket, s3_path, s3_file, this_chunkix)
                chunkids.append(chunk_id)
                C_CHUNK_CACHE[chunk_id] = chunk
            # we also need to cache the path as s3 key
            C_S3_KEY_CACHE[path] = chunkids

            if chunk_id in C_CHUNK_CACHE:
                return C_CHUNK_CACHE[chunk_id]
            else:
                return None
        else:
            return None # and don't cache the empty list!
    finally:
        s3.meta.client.close()

def yield_file_pairs_from_s3(boto3_session, s3_bucket, s3_paths):
    if not isinstance(s3_paths, list):
        raise Exception("s3_paths must be a list")

    s3 = boto3_session.resource('s3')
    try:
        bucket = s3.Bucket(s3_bucket)
        for pathix, s3_path in enumerate(s3_paths):
            for s3_object in bucket.objects.filter(Prefix=s3_path):
                if s3_object.key.endswith(".json"):
                    s3_file = os.path.basename(s3_object.key)
                    yield pathix, s3_file
    finally:
        s3.meta.client.close()

def yield_chunks_from_s3(boto3_session, s3_bucket, s3_paths, file_pair, read_through_cache=False):
    if not isinstance(s3_paths, list):
        s3_paths = [s3_paths]

    global C_CHUNK_CACHE
    global C_S3_KEY_CACHE

    s3 = boto3_session.resource('s3')

    try:
        pathix, s3_file = file_pair
        s3_path = s3_paths[pathix]
        path = f"{s3_path}/{s3_file}" if s3_path else f"{s3_file}"

        done = False

        if not read_through_cache:
            if path in C_S3_KEY_CACHE:
                # the value is a list of fileix, chunkix pairs
                chunk_ids = C_S3_KEY_CACHE[path]
                for chunk_id in chunk_ids:
                    if chunk_id in C_CHUNK_CACHE:
                        chunk = C_CHUNK_CACHE[chunk_id]
                        yield chunk
                done = True

        if not done:
            s3_object = s3.Object(s3_bucket, path)
            chunks = _get_chunks_from_s3_object(s3_object)
            chunk_ids = []
            for chunkix, chunk in enumerate(chunks):
                chunk_id = _calc_chunk_id(s3_bucket, s3_path, s3_file, chunkix)
                chunk_ids.append(chunk_id)
                C_CHUNK_CACHE[chunk_id] = chunk
                yield chunk
            
            C_S3_KEY_CACHE[path] = chunk_ids
    finally:
        s3.meta.client.close()

def create_dumb_index(boto3_session, s3_bucket, s3_paths, f_get_vector_from_chunk, read_through_cache=False, dimension_threshold=0):
    s3_file_pairs = []
    triples = []

    for fileix, file_pair in enumerate(yield_file_pairs_from_s3(boto3_session, s3_bucket, s3_paths)):
        s3_file_pairs.append(file_pair)
        for chunkix, chunk in enumerate(yield_chunks_from_s3(boto3_session, s3_bucket, s3_paths, file_pair, read_through_cache)):
            triple = (f_get_vector_from_chunk(chunk), fileix, chunkix)
            triples.append(triple)

    dimension_mask = None
    if dimension_threshold:
        dimension_mask = create_dimension_mask(triples, dimension_threshold)
        print (f"dimension_mask: {dimension_mask}")

        filtered_triples = []
        for triple in triples:
            filtered_vector = filter_vector_by_mask(triple[0], dimension_mask)
            filtered_triples.append((
                filtered_vector,
                triple[1],
                triple[2]
            ))

        triples = filtered_triples
    else:
        # dimension_mask needs to be all 1s
        num_dimensions = len(triples[0][0])
        dimension_mask = [1] * num_dimensions

    return {
        "triples": triples,
        "file_pairs": s3_file_pairs,
        "paths": s3_paths,
        "dimension_mask": dimension_mask
    }

def write_dumb_index_to_s3(boto3_session, s3_bucket, s3_path, dumb_index_name, dumb_index, vector_type, num_dimensions):
    s3 = boto3_session.resource('s3')
    try:
        dumb_index_bytes = get_dumb_index_bytes(dumb_index, vector_type, num_dimensions)

        path = f"{s3_path}/{dumb_index_name}" if s3_path else f"{dumb_index_name}"
        s3_object = s3.Object(s3_bucket, path)
        s3_object.put(Body=dumb_index_bytes)
    finally:
        s3.meta.client.close()

def write_dumb_index_to_file(filename, dumb_index, vector_type, num_dimensions):
    dumb_index_bytes = get_dumb_index_bytes(dumb_index, vector_type, num_dimensions)
    with open(filename, "wb") as f:
        f.write(dumb_index_bytes)

def read_dumb_index_from_s3(boto3_session, s3_bucket, s3_path, dumb_index_name):
    s3 = boto3_session.resource('s3')
    try:
        path = f"{s3_path}/{dumb_index_name}" if s3_path else f"{dumb_index_name}"
        s3_object = s3.Object(s3_bucket, path)
        try:
            dumb_index_bytes = s3_object.get()['Body'].read()
            dumb_index = get_dumb_index_from_bytes(dumb_index_bytes)
            return dumb_index
        except botocore.exceptions.ClientError as e:
            print (e)
            #botocore.errorfactory.NoSuchKey
            if e.response['Error']['Code'] == "NoSuchKey":
                return None
            else:
                # Something else has gone wrong.
                raise
    finally:
        s3.meta.client.close()

def read_dumb_index_from_file(filename):
    with open(filename, "rb") as f:
        dumb_index_bytes = f.read()
        dumb_index = get_dumb_index_from_bytes(dumb_index_bytes)
        return dumb_index

def dumb_index_exists_on_s3(boto3_session, s3_bucket, s3_path, dumb_index_name):
    s3 = boto3_session.resource('s3')
    try:
        path = f"{s3_path}/{dumb_index_name}" if s3_path else f"{dumb_index_name}"
        s3_object = s3.Object(s3_bucket, path)
        try:
            s3_object.load()
        except botocore.exceptions.ClientError as e:
            print (e)
            #botocore.errorfactory.NoSuchKey
            if e.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise
        else:
            return True
    finally:
        s3.meta.client.close()

def cosine_similarity(a, b):
    dot_product = sum([a[i] * b[i] for i in range(len(a))])
    a_magnitude = sum([a[i] * a[i] for i in range(len(a))])
    b_magnitude = sum([b[i] * b[i] for i in range(len(b))])
    return dot_product / (a_magnitude * b_magnitude)

def dot_product(a, b):
    return sum([a[i] * b[i] for i in range(len(a))])

def sort_dumb_index_by_similarity(dumb_index, vector, assume_normalized_vectors=True):
    f_cosign_similarity = dot_product if assume_normalized_vectors else cosine_similarity
    sorted_triples = sorted(dumb_index["triples"], key=lambda triple: f_cosign_similarity(triple[0], vector), reverse=True)
    return {
        "triples": sorted_triples,
        "paths": dumb_index["paths"],
        "file_pairs": dumb_index["file_pairs"]
    }

def get_chunks_from_dumb_index(boto3_session, s3_bucket, dumb_index, offset, amount, read_through_cache=False):
    return list(yield_chunks_from_dumb_index(boto3_session, s3_bucket, dumb_index, offset, amount, read_through_cache))

def yield_chunks_from_dumb_index(boto3_session, s3_bucket, dumb_index, offset, amount, read_through_cache=False):
    for i in range(offset, offset + amount):
        if i >= len(dumb_index["triples"]):
            break
        triple = dumb_index["triples"][i]
        _, fileix, chunkix = triple
        s3_file_pair = dumb_index["file_pairs"][fileix]
        pathix, s3_file = s3_file_pair
        s3_path = dumb_index["paths"][pathix]
        chunk = read_chunk_from_s3(boto3_session, s3_bucket, s3_path, s3_file, chunkix, read_through_cache)
        yield chunk


def create_dimension_mask(triples, threshold=0.1):
    '''
    In this function, we create a mask (a vector containing 0s and 1s) that will be used to filter out dimensions that
    are not valuable in our vectors. Our vectors are at position 0 in the triples.

    We traverse the triples and for each dimension, we calculate the max and min value of that dimension in all the vectors.
    If the difference between the max and min is less than the threshold, we set the mask value to 0, otherwise we set it to 1.
    '''

    if not triples:
        return None
    
    num_dimensions = len(triples[0][0])

    max_values = triples[0][0].copy()
    min_values = triples[0][0].copy()

    for triple in triples[1:]:
        vector = triple[0]
        for i in range(num_dimensions):
            if vector[i] > max_values[i]:
                max_values[i] = vector[i]
            if vector[i] < min_values[i]:
                min_values[i] = vector[i]

    dimension_mask = [
        0 if max_values[i] - min_values[i] < threshold else 1
        for i in range(num_dimensions)
    ]

    return dimension_mask

def filter_vector_by_mask(vector, mask):
    return [
        vector[i]
        for i in range(len(vector))
        if mask[i]
    ]