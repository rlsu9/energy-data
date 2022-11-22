#!/bin/zsh

cd "$(dirname "$0")" || exit 1

function lookup_gps() {
    local in=$1
    if [ -z "$in" ]; then read in; fi
    echo "$(cat azure-regions.gps.json | jq -j '.[] | select(.RegionName=="'"$in"'") | .Latitude,",",.Longitude')";
}

for line in $(cat azure-regions.iso.txt); do
    region="$(echo "$line" | cut -d, -f 1)"
    iso="$(echo "$line" | cut -d, -f 2)"
    # CSV format
    : '
    echo -n $region,$iso,
    lookup_gps $region
    '
    # YAML format
    gps="$(lookup_gps $region)"
    lat="$(echo $gps | awk -F, '{print $1}')"
    lon="$(echo $gps | awk -F, '{print $2}')"
    echo "- code: $region"
    echo "  name: $region ($iso)"
    echo "  iso: $iso"
    echo "  gps:"
    echo "  - $lat"
    echo "  - $lon"
done
