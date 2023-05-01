# Install the necessary tools: 
Install the setuptools, wheel, and twine packages using pip:

``` bash
    pip install setuptools wheel twine
```

# Build your package: 
Run the following command in your library's root directory to generate the distribution packages:

``` bash
    python setup.py sdist bdist_wheel
```

This will create a dist directory containing the source distribution (.tar.gz) and the wheel distribution (.whl).

# Upload your package to PyPI:
Use twine to upload your package to PyPI:

``` bash
    twine upload dist/*
```

You'll be prompted to enter your PyPI username and password.

# Verify your package:
Visit https://pypi.org/project/dumbvector/ to check that your package is now available on PyPI. Users can now install your package using 

``` bash
    pip install dumbvector
```