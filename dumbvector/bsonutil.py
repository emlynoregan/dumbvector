from dumbvector.numtypes import widest_of_numtypes_for_array, convert_number_to_bytes, convert_bytes_to_number, num_bytes_for_numtype
import numpy as np

C_BSU_BYTEARRAY = 0xee
C_NORMAL_BYTEARRAY = 0xdd
C_NUMPY_ARRAY = 0xcc

C_META_ATTR = "_meta_"

def is_numarray(array):
    for item in array:
        if not isinstance(item, (int, float)):
            return False
    return True

def numarray_to_bsu_bytearray(numarray):
    if not is_numarray(numarray):
        raise Exception("vector must be a list of numbers")
    numtype = widest_of_numtypes_for_array(numarray)
    #create a bytearray of zeroes, of length 2 + num_bytes_for_numtype(numtype) * len(numarray)
    ba = bytearray(2 + num_bytes_for_numtype(numtype) * len(numarray))
    #set the first two bytes to C_BSU_BYTEARRAY and numtype
    ba[0] = C_BSU_BYTEARRAY
    ba[1] = numtype
    for i in range(len(numarray)):
        #convert each number to bytes and set it in the bytearray
        ba[2 + i*num_bytes_for_numtype(numtype):2 + (i+1)*num_bytes_for_numtype(numtype)] = convert_number_to_bytes(numarray[i], numtype)

    # values = [convert_number_to_bytes(num, numtype) for num in numarray]
    # the_bytes = b"".join(values)
    # docs_bytearray = bytes([C_BSU_BYTEARRAY, numtype]) + the_bytes
    return bytes(ba)

def bsu_bytearray_to_numarray(docs_bytearray):
    if len(docs_bytearray) < 2:
        raise Exception("invalid docs_bytearray")
    if docs_bytearray[0] != C_BSU_BYTEARRAY:
        raise Exception("invalid docs_bytearray")
    numtype = docs_bytearray[1]
    return [convert_bytes_to_number(docs_bytearray[i:i+num_bytes_for_numtype(numtype)], numtype) for i in range(2, len(docs_bytearray), num_bytes_for_numtype(numtype))]

def replace_numarrays_with_bytearrays(obj):
    # first check if it's a numpy array
    if isinstance(obj, np.ndarray):
        return bytes([C_NUMPY_ARRAY]) + obj.tobytes()
    if isinstance(obj, list):
        if is_numarray(obj):
            return numarray_to_bsu_bytearray(obj)
        else:
            return [replace_numarrays_with_bytearrays(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: replace_numarrays_with_bytearrays(value) for key, value in obj.items()}
    elif isinstance(obj, bytes):
        return bytes([C_NORMAL_BYTEARRAY]) + obj
    else:
        return obj

def replace_bytearrays_with_numarrays(obj):
    if isinstance(obj, list):
        return [replace_bytearrays_with_numarrays(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: replace_bytearrays_with_numarrays(value) for key, value in obj.items()}
    elif isinstance(obj, bytes):
        if len(obj) > 0:
            if obj[0] == C_NORMAL_BYTEARRAY:
                return obj[1:]
            elif obj[0] == C_NUMPY_ARRAY:
                return np.frombuffer(obj[1:])
            elif obj[0] == C_BSU_BYTEARRAY:
                return bsu_bytearray_to_numarray(obj)
            else:
                return obj
        else:
            return obj
    else:
        return obj
    
def encode_ndarrays(obj):
    # first check if it's a numpy array
    if isinstance(obj, np.ndarray):
        # we need the shape and the dtype
        shape = obj.shape
        dtype = obj.dtype
        return {
            C_META_ATTR: C_NUMPY_ARRAY,
            "shape": shape,
            "dtype": dtype.str,
            "bytes": obj.tobytes()
        }
    elif isinstance(obj, list):
        return [encode_ndarrays(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: encode_ndarrays(value) for key, value in obj.items()}
    else:
        return obj

def decode_ndarrays(obj):
    if isinstance(obj, list):
        return [decode_ndarrays(item) for item in obj]
    elif isinstance(obj, dict):
        if C_META_ATTR in obj and obj[C_META_ATTR] == C_NUMPY_ARRAY:
            # we need the shape and the dtype
            shape = obj["shape"]
            dtype = np.dtype(obj["dtype"])
            return np.frombuffer(obj["bytes"], dtype=dtype).reshape(shape)
        else:
            return {key: decode_ndarrays(value) for key, value in obj.items()}
    else:
        return obj

def shrink_ndarrays(obj):
    if isinstance(obj, np.ndarray):
        # we're going to convert the ndarray to float32
        return obj.astype(np.float32)
    if isinstance(obj, list):
        return [shrink_ndarrays(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: shrink_ndarrays(value) for key, value in obj.items()}
    else:
        return obj