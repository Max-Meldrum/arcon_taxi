#!/bin/sh

wget -i ./data_url.md

/bin/cat yellow_tripdata_2020-*\.csv \
  | sed '/2020-[0-9][0-9]-[0-9][0-9]/!d'
  | sort -t, -k 3n,3 \
  >> sorted_yellow_tripdata_2020.csv
