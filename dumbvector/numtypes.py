import struct

C_FLOAT32 = 0
C_FLOAT64 = 1
C_INT8 = 8
C_INT16 = 9
C_INT32 = 10
C_INT64 = 11
C_UINT8 = 16
C_UINT16 = 17
C_UINT32 = 18
C_UINT64 = 19

def narrowest_numtype_for_value(value):
    if isinstance(value, float):
        return C_FLOAT64
    elif isinstance(value, int):
        # don't use unsigned ints, because we want to be able to represent negative numbers
        if value >= -128 and value <= 127:
            return C_INT8
        elif value >= -32768 and value <= 32767:
            return C_INT16
        elif value >= -2147483648 and value <= 2147483647:
            return C_INT32
        else:
            return C_INT64
    else:
        raise Exception("unexpected type")

def widest_of_numtypes(*numtypes):
    if len(numtypes) == 0:
        return None
    elif len(numtypes) == 1:
        return numtypes[0]
    elif len(numtypes) == 2:
        numtype1 = numtypes[0]
        numtype2 = numtypes[1]
        if numtype1 == numtype2:
            return numtype1
        elif numtype1 == C_FLOAT64 or numtype2 == C_FLOAT64:
            return C_FLOAT64
        elif numtype1 == C_FLOAT32 or numtype2 == C_FLOAT32:
            return C_FLOAT32
        elif numtype1 == C_INT64 or numtype2 == C_INT64:
            return C_INT64
        elif numtype1 == C_INT32 or numtype2 == C_INT32:
            return C_INT32
        elif numtype1 == C_INT16 or numtype2 == C_INT16:
            return C_INT16
        elif numtype1 == C_INT8 or numtype2 == C_INT8:
            return C_INT8
        else:
            raise Exception("unexpected numtypes")
    else:
        widest_numtype = numtypes[0]
        for i in range(1, len(numtypes)):
            widest_numtype = widest_of_numtypes(widest_numtype, numtypes[i])
        return widest_numtype

def widest_of_numtypes_for_array(array):
    return widest_of_numtypes(*[narrowest_numtype_for_value(item) for item in array])

def convert_number_to_bytes(value, numtype):
    if numtype == C_FLOAT32:
        return struct.pack('<f', value)
    elif numtype == C_FLOAT64:
        return struct.pack('<d', value)
    elif numtype == C_INT8:
        return value.to_bytes(1, byteorder='little', signed=True)
    elif numtype == C_INT16:
        return value.to_bytes(2, byteorder='little', signed=True)
    elif numtype == C_INT32:
        return value.to_bytes(4, byteorder='little', signed=True)
    elif numtype == C_INT64:
        return value.to_bytes(8, byteorder='little', signed=True)
    elif numtype == C_UINT8:
        return value.to_bytes(1, byteorder='little', signed=False)
    elif numtype == C_UINT16:
        return value.to_bytes(2, byteorder='little', signed=False)
    elif numtype == C_UINT32:
        return value.to_bytes(4, byteorder='little', signed=False)
    elif numtype == C_UINT64:
        return value.to_bytes(8, byteorder='little', signed=False)
    else:
        raise Exception("unexpected numtype")
    
def convert_bytes_to_number(bytes, numtype):
    if numtype == C_FLOAT32:
        return struct.unpack('<f', bytes)[0]
    elif numtype == C_FLOAT64:
        return struct.unpack('<d', bytes)[0]
    elif numtype == C_INT8:
        return int.from_bytes(bytes, byteorder='little', signed=True)
    elif numtype == C_INT16:
        return int.from_bytes(bytes, byteorder='little', signed=True)
    elif numtype == C_INT32:
        return int.from_bytes(bytes, byteorder='little', signed=True)
    elif numtype == C_INT64:
        return int.from_bytes(bytes, byteorder='little', signed=True)
    elif numtype == C_UINT8:
        return int.from_bytes(bytes, byteorder='little', signed=False)
    elif numtype == C_UINT16:
        return int.from_bytes(bytes, byteorder='little', signed=False)
    elif numtype == C_UINT32:
        return int.from_bytes(bytes, byteorder='little', signed=False)
    elif numtype == C_UINT64:
        return int.from_bytes(bytes, byteorder='little', signed=False)
    else:
        raise Exception("unexpected numtype")
    
def num_bytes_for_numtype(numtype):
    if numtype == C_FLOAT32:
        return 4
    elif numtype == C_FLOAT64:
        return 8
    elif numtype == C_INT8 or numtype == C_UINT8:
        return 1
    elif numtype == C_INT16 or numtype == C_UINT16:
        return 2
    elif numtype == C_INT32 or numtype == C_UINT32:
        return 4
    elif numtype == C_INT64 or numtype == C_UINT64:
        return 8
    else:
        raise Exception("unexpected numtype")