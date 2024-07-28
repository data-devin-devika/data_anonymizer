#!/bin/bash


# Generate encryption key
python /usr/src/app/scripts/generate_key.py

# Generate sample data
python /usr/src/app/utils/data_generator.py /usr/src/app/data/sample_data.csv 1000000

# Anonymize the data
python /usr/src/app/scripts/anonymize_data.py /usr/src/app/data/sample_data.csv /usr/src/app/data/output/ first_name,last_name,address
# List the contents of the output directory
echo "Contents of /usr/src/app/data/output directory:"
ls -l /usr/src/app/data/output/