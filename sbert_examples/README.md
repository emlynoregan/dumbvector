# SBert Examples
The SBert examples expect to work on a folder full of utf-8 encoded text files as source data.

I've put together this repo of Darwin's collected works, in utf-8 format, for this purpose:
https://github.com/emlynoregan/darwincorpus

Where you see "./source" below, you can put the path to the darwin files on your machine.

Note that none of these scripts will create folders for you, so you should create the folders yourself before running the scripts.

# Setting up your environment
Before running these scripts, you should run setupvenv.sh or setupvenv.ps1 to create a virtual environment and install the required dependencies.

NOTE: The sentence-transformers library from huggingface pulls in a lot of large dependencies, so your venv will take about 1.22GB of disk space.

# Running the examples

## create_docs.py
```
usage: create_docs.py [-h] source_path docs_path
eg: create_docs.py ./source ./docs
```
This script will traverse a folder full of text files at <source_path> and create a folder full of Docs files, one for each text file, at <docs_path>.

Each doc in a Docs file is a dictionary representing a record for searching on. They can have any structure in general, but this
script creates one per paragraph in the following format:
```
{
    'text': <paragraph text>,
    'ix': <paragraph index in the file>,
    'embedding': <openai embedding vector for the paragraph using "text-embedding-ada-002" model>
} 
```

The actual format is a binary format that is not human readable. See docs.py for more information.

Note: This can be slow! It takes hours to process the Darwin corpus on my machine.

## create_index.py
```
usage: create_index.py [-h] index_name index_path docs_path
eg: create_index.py darwin ./index ./docs
```

This script will traverse a folder full of Docs files at <docs_path> and create a dumbindex at <index_path> with the name <index_name>.

This dumbindex is what you need to perform a search.

To get the folder full of Docs files, you can use create_docs.py.

This is fast. It takes maybe 10 seconds to create the index for the Darwin corpus on my machine.

Note that it expects to find an embedding in the docs in the attribute "embedding". That's not a requirement of the dumbvector library,
just a requirement for this script.

## search_index.py
```
usage: search_index.py [-h] index_filename docs_path query [num_results]
eg: search_index.py ./index ./docs "tortoises" 10
```

This script will perform a search on the index at <index_filename> using the query <query> and return the top <num_results> results (defaults to 20).
To return the actual text of the results, it uses the Docs files at <docs_path> (the index only contains the index of the docs in these files).

To get the folder full of Docs files, you can use create_docs.py.
To get the index, you can use create_index.py.

This should be really fast! It takes a fraction of a second to search the Darwin corpus on my machine. In the example below, 
the step "Find the top 5 results using cosine similarity" is the actual search.

```
(venv) PS C:\Users\emlyn\OneDrive\Documents\dev\dumbvector\sbert_examples> python .\search_index.py E:\emlynpc\data\scratch\indices_darwin_sbert\darwin.dumbindex ..\..\vectorexp\docssbert\ "tortoises" 5
import time: 2.7342498302459717
load the index 'darwin' from filesystem  0.79 sec
Number of records in the index: 145683
get the embedding for the query 'tortoises'  0.01 sec
Find the top 5 results using cosine similarity  0.12 sec
load the original docs from filesystem  0.51 sec
**********
Results:
**********
0  Turtles, conversion into land-tortoises.
1  Tortoises, conversion of turtles into land-.
2 Tortoise, voice of the male.
3 I will first describe the habits of the tortoise (Testudo nigra,
formerly called Indica), which has been so frequently alluded to. These
animals are found, I believe, on all the islands of the archipelago;
certainly on the greater number. They frequent in 
4 The tortoise is very fond of water, drinking large quantities, and
wallowing in the mud.  The larger islands alone possess springs, and
these are always situated towards the central parts, and at a
considerable height.  The tortoises, therefore, which freq
done
```

Note that this script expects each doc to have an attribute "text" with the text in it. That's not a requirement of the dumbvector library,
just a requirement for this script.
