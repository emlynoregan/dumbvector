import numpy as np

def top_k_similar(dumb_index, search_vector, k, new_dumb_index_name=None):
    '''
    We return a dumb index containing just the top k documents in order of similarity.
    Note that we expect the vectors in the dumb index to be normalized to unit length.
    '''
    # D = len(search_vector)

    np_content_vectors = dumb_index["vectors"] # this is an N X D matrix

    np_search_vector = np.array(search_vector) # this is a 1 X D vector
    # change type if necessary to match the index
    vector_dtype = np_content_vectors.dtype
    np_search_vector = np_search_vector.astype(vector_dtype)

    # we actually need a D X N matrix for the multiplication
    # # note: this caching doesn't seem to be necessary?
    # if "vectors_t" in dumb_index:
    #     np_content_matrix = dumb_index["vectors_t"]
    # else:
    # np_content_matrix = np.array(np_content_vectors).T # this is a D X N matrix
    # dumb_index["vectors_t"] = np_content_matrix
    np_content_matrix = np.array(np_content_vectors).T # this is a D X N matrix
    dumb_index["vectors_t"] = np_content_matrix

    cosine_similarities = np.dot(np_search_vector, np_content_matrix) # result of [1 X D] . [D X N] is a [1 X N] vector

    # now we want the top k indices
    top_k_indices = np.argpartition(cosine_similarities, -k)[-k:] # this is a 1 X k vector, just the indices of the highest k values

    # everything below here is fast as long as k is small

    # get the top k (similarity, vector, docref) triples
    top_k_triples = [(cosine_similarities[i], np_content_vectors[i], dumb_index["docrefs"][i]) for i in top_k_indices]

    # sort the pairs
    sorted_top_k_triples = sorted(top_k_triples, key=lambda item: item[0], reverse=True)

    # use unzip to get the top k vectors and docrefs
    _, top_k_vectors, top_k_docrefs = zip(*sorted_top_k_triples)

    # now make the new dumb index
    new_dumb_index = {
        "name": new_dumb_index_name or f"top_{k}_similar_{dumb_index['name']}",
        "version": dumb_index["version"],
        "docsnames": dumb_index["docsnames"],
        "vectors": np.array(top_k_vectors),
        "docrefs": top_k_docrefs
    }

    return new_dumb_index
