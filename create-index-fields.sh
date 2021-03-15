#!/bin/bash

# To delete an index-pattern:
#
# $ curl -XDELETE localhost:9200/arcon_data_stream
#


# To create an index-pattern:

curl \
  -H "Content-Type: application/json" \
  -XPUT "http://localhost:9200/arcon_data_stream" \
  -d' {
    "mappings": {
        "properties": {
            "time": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"
            },
            "pu_location_id":      {"type": "integer"},
            "pu_location_name":    {"type": "keyword"},
            "count":               {"type": "integer"},
            "sum_fare_amount":     {"type": "integer"},
            "max_fare_amount":     {"type": "integer"},
            "avg_fare_amount":     {"type": "integer"},
            "min_fare_amount":     {"type": "integer"},
            "sum_trip_distance":   {"type": "float"},
            "avg_trip_distance":   {"type": "float"},
            "sum_passenger_count": {"type": "integer"},
            "max_passenger_count": {"type": "integer"},
            "avg_passenger_count": {"type": "integer"},
            "min_passenger_count": {"type": "integer"},
            "sum_duration":        {"type": "integer"},
            "max_duration":        {"type": "integer"},
            "avg_duration":        {"type": "integer"},
            "min_duration":        {"type": "integer"}
        }
    }
}'

# After running the above, go to:
#
#   http://localhost:8000/app/management/kibana/indexPatterns
#
# and add the arcon_data_stream as an index-pattern
