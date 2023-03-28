# OpenAI Examples
The OpenAI examples expect to work on a folder full of utf-8 encoded text files as source data.

I've put together this repo of Darwin's collected works, in utf-8 format, for this purpose:
https://github.com/emlynoregan/darwincorpus

Where you see "./source" below, you can put the path to the darwin files on your machine.

Note that none of these scripts will create folders for you, so you should create the folders yourself before running the scripts.

# Setting up your environment
Before running these scripts, you should run setupvenv.sh or setupvenv.ps1 to create a virtual environment and install the required dependencies.

Then, copy credentials_template.json to credentials.json and fill in your OpenAI API key.

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

This is fast. It takes maybe 20 seconds to create the index for the Darwin corpus on my machine.

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
(venv) PS C:\Users\emlyn\OneDrive\Documents\dev\dumbvector\openai_examples> python .\search_index.py E:\emlynpc\data\scratch\indices_darwin_openai\darwin.dumbindex ..\..\vectorexp\docs\ "tortoises" 5 
import time: 0.39823055267333984
load the index 'darwin' from filesystem  0.91 sec
Number of records in the index: 64673
Get an embedding for the query from OpenAI  0.94 sec
Find the top 5 results using cosine similarity  0.35 sec
load the original docs from filesystem  0.31 sec
**********
Results:
**********
0 Turtle.
1  Turtles, conversion into land-tortoises.
2 Tortoise, voice of the male.
3 Reptiles.
4  Tortoises, conversion of turtles into land-.
done
```

Note that this script expects each doc to have an attribute "text" with the text in it. That's not a requirement of the dumbvector library,
just a requirement for this script.
