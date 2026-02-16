#!/bin/sh
#
# Get license info csv first:
#  go-licenses report ./... > licenses.csv

echo "Third-Party Software Notices" > NOTICE.txt
echo "=============================" >> NOTICE.txt
echo "" >> NOTICE.txt
echo "This software includes the following third-party dependencies:" >> NOTICE.txt
echo "" >> NOTICE.txt

tail -n +1 licenses.csv | while IFS=, read -r package url license_type; do
    if [ "$package" != "Package" ]; then
        echo "- $package" >> NOTICE.txt
        echo "  License: $license_type" >> NOTICE.txt
        echo "  URL: $url" >> NOTICE.txt
        echo "" >> NOTICE.txt
    fi
done
