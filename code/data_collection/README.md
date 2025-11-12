# Crawling and Data Collection 

This module collects data from account trading marketplaces and social media platforms. 

## Quick Start

Create a `python3` virtual env and install the dependencies:
```
$ python3 -m venv venv
$ venv/bin/pip3 install -r requirements.txt
```

Create a customized version of the configuration file:
```
cp config.yaml local.config.yaml
vim local.config.yaml # edit paths
```

Run a data collection script:
```
$ venv/bin/python3 scripts.FILENAME --conf=$(pwd)/config.yaml
```

where FILENAME is the name of the script you want to run.

Alternatively, run parallel instances with:

```
$ run_pipeline.sh
```


## Dependencies

Update using:

```
$ venv/bin/pip3 freeze > requirements.txt
```


