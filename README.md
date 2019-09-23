# Prometheus Coding Test

This is a short coding assignment, for Prometheus Research. This is a Python command line utility that will create a database and perform a basic ETL process upon it.


## Dependencies

You need postgres and python 3 to run this code
Internet is needed to fetch data from remote source


## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install -r requirements.txt
configure db in the config.ini using your own db settings
```
## Note
This is a multiprocess event, so there is a tendency for it to take a chunk of your CPU Process while running. 
This was done so as to enable the application to run faster. You can reduce the processor pool to your specified number 
in each of the resource manager class.

## Usage

```python
1. Install all dependencies as described in the installation section above
2. Run code using python run.py
3. To run tests, python tests.py




