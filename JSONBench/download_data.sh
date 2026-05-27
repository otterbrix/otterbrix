#!/bin/bash

wget --continue --timestamping --progress=dot:giga --directory-prefix "$PWD" "https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_0001.json.gz"

gunzip file_0001.json.gz

python3 filter_json_paths.py file_0001.json file_0001_filtered.json
