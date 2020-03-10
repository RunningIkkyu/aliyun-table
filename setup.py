"""A setuptools based setup module.
 
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""
 
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path
 
here = path.abspath(path.dirname(__file__))
 
# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
 
# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.
 
setup(
    name='aliyun-table',
    version='0.1.1',
    author='Lane',
    author_email='GeekerLane@gmail.com',
    description='Aliyun Tablestore Operations, make aliyun tablestore more simple.',
    url='https://github.com/RunningIkkyu/aliyun-table'
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=['pprint', 'tablestore', 'prettytable']

)
