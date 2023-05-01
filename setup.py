from setuptools import setup, find_packages

setup(
    name='dumbvector',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        "numpy",
        "bson"
    ],
    author='Emlyn O\'Regan',
    author_email='emlynoregan@gmail.com',
    description='Semantic Search done the dumb way.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/emlynoregan/dumbvector',
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
