#!/bin/sh

wget -i ./data_url.md

/bin/cat yellow_tripdata_2020-.*\.csv >> yellow_tripdata_2020.csv

sort -t, -k 3n,3 yellow_tripdata_2020.csv > sorted_yellow_tripdata_2020.csv
