# In this program we create a dumb_index Docs object from a file specified on the command line, by breaking it into paragraphs, 
# and then creating embeddings from the paragraphs. We write this to file.

import time
start_time = time.time()

import argparse
from dumbvector.docs import file_docs_exists, make_docs_v1, get_docs_file_writer
from dumbvector.util import time_function
import json
import time
import os
import numpy as np
from sentence_transformers import SentenceTransformer

end_time = time.time()
print (f'import time: {end_time - start_time}')

C_MODEL = None

@time_function
def create_doclist_from_paragraphs(paragraphs):
    # create chunks from the paragraphs
    doclist = []
    for index, p in enumerate(paragraphs):
        print (f'creating doc {index}')
        # create a chunk from the paragraph and embedding

        embedding = C_MODEL.encode(p, convert_to_tensor=False)

        doc = {
            'text': p,
            'ix': index,
            'embedding': np.array(embedding)
        }

        doclist.append(doc)

    return doclist

def read_credentials():
    with open('credentials.json', 'r') as f:
        return json.load(f)

@time_function
def create_docs(filename, docs_path):
    # remove any path info from the filename
    docs_name = os.path.basename(filename)
    print (f'creating docs {docs_name} from file {filename}')

    docs_exists = file_docs_exists(docs_name, docs_path)

    if docs_exists:
        print (f'{docs_name} already exists')
        return

    # read the file (utf-8)
    print (f'reading {filename}')
    with open(filename, 'r', encoding='utf-8') as f:
        text = f.read()

    # break it into paragraphs. They are separated by two newlines
    print (f'breaking {filename} into paragraphs')
    paragraphs = text.split('\n\n')

    # break up any paragraphs that are too long
    print (f'breaking up any paragraphs that are too long')
    C_MAX_PARAGRAPH_LENGTH = 256
    new_paragraphs = []
    for p in paragraphs:
        if len(p) > C_MAX_PARAGRAPH_LENGTH:
            print (f'paragraph is too long: {len(p)} characters')
            while p:
                new_paragraphs.append(p[:C_MAX_PARAGRAPH_LENGTH])
                p = p[C_MAX_PARAGRAPH_LENGTH:]
        else:
            new_paragraphs.append(p)

    # remove any empty paragraphs
    paragraphs = [p for p in new_paragraphs if p]

    print (f'found {len(paragraphs)} paragraphs')

    # create doclist from the paragraphs
    doclist = create_doclist_from_paragraphs(paragraphs)

    # create the Docs object
    print (f'creating docs object')
    d = make_docs_v1(docs_name, doclist)

    # write the Docs object to file
    print (f'writing docs to file')
    writer = get_docs_file_writer(docs_path)
    writer(d)

def main():
    # usage: python create_docs.py <source_path> <docs_path>

    parser = argparse.ArgumentParser()

    parser.add_argument('source_path', help='path to the source file or directory')
    parser.add_argument('docs_path', help='path to the docs directory for writing the docs files')

    args = parser.parse_args()

    source_path = args.source_path
    docs_path = args.docs_path

    global C_MODEL
    C_MODEL = SentenceTransformer('all-MiniLM-L6-v2', cache_folder='modelcache')

    sourcepath_is_dir = os.path.isdir(source_path)

    if sourcepath_is_dir:
        # get all the files in the directory
        filenames = [os.path.join(source_path, f) for f in os.listdir(source_path)]
        for filename in filenames:
            create_docs(filename, docs_path)
    else:
        create_docs(source_path, docs_path)

    print ("done")

if __name__ == '__main__':
    main()

