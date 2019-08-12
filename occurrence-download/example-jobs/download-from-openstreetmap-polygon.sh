#!/bin/zsh -e
#
# Download an OSM relation, and submit it as a download request.
#
# Search for polygons using the "Query features" button on https://www.openstreetmap.org/.
#

id=$1

[[ -f $id.xml ]] || curl -Ssfo $id.xml https://www.openstreetmap.org/api/0.6/relation/$id/full

ogr2ogr -f CSV $id.csv $id.xml -lco GEOMETRY=AS_WKT multipolygons

awk 'length > max_length { max_length = length; longest_line = $0 } END { print longest_line }' $id.csv | tail -n 1 | cut -d'"' -f2 > $id.wkt

wkt=$(cat $id.wkt)

points=$(tr -dc ',\n' < $id.wkt | awk '{print length + 1}')
echo "Geometry contains $points points"
factor_millionths=100
while [[ $points -gt 10000 ]]; do
  factor_millionths=$(printf "%06d" $factor_millionths)
  echo "Geometry contains $points points, simplifying with factor 0.$factor_millionths"
  ogr2ogr -f CSV $id.csv $id.xml -lco GEOMETRY=AS_WKT -simplify 0.$factor_millionths multipolygons
  awk 'length > max_length { max_length = length; longest_line = $0 } END { print longest_line }' $id.csv | tail -n 1 | cut -d'"' -f2 > $id.wkt
  wkt=$(cat $id.wkt)
  points=$(tr -dc ',\n' < $id.wkt | awk '{print length + 1}')
  echo "Simplified geometry contains $points points"
  factor_millionths=$(($factor_millionths + 100))
done

echo 'Continue?'
read

cat > $id-request.json <<EOF
{
  "predicate": {
    "type": "and",
    "predicates": [
      {
        "type": "within",
        "geometry": "$wkt"
      },
      {
        "type": "equals",
        "key": "HAS_GEOSPATIAL_ISSUE",
        "value": "false"
      }
    ]
  },
  "send_notification": true,
  "format": "SIMPLE_CSV"
}
EOF

curl -i --user $GBIF_AUTH -H "Content-Type: application/json" -X POST -d @$id-request.json 'https://api.gbif-uat.org/v1/occurrence/download/request'
