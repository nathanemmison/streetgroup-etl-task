# Price Paid Data Set to JSON
# Author - Nathan Emmison

import requests
import os
import logging
import apache_beam as beam
import json
import csv

# Setting up logger format
logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s', datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger('price-paid-to-json')
logger.setLevel(logging.DEBUG)

def getPricePaidFile(url):

	logger.info('Downloading source file from %s', url)

	# Download latest file from source
	r = requests.get(url)

	# Catch any errors when downloading file
	if r.status_code != requests.codes.ok:
		logger.error('Error downloading source file from %s', url)
		r.raise_for_status()

	# Write file to disk
	with open('price-paid-data.csv', 'wb') as file:
		file.write(r.content)

def cleanUp(file):

	logger.info('Cleaning up file %s', file)

	# Ensure file exists before removing
	if os.path.isfile(file):
		os.remove(file)
		logger.info('File %s has been removed', file)
	else:
		logger.error('File %s not found.', file)
		exit(1)

def convertListToCsv(element):
	# Create a list version of the CSV, using the Python package allows for better parsiing e.g quotes, escaping commas in addresses etc.
	for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL):
		return line

def convertListToJson(element):

	# List of Headers
	headers = ["transactionUniqueIndentifier", "price", "dateOfTransfer", "postcode", "propertyType", "oldOrNew", "duration", "PAON", "SAON", "street", "locality", "townCity", "district", "county", "PPDType", "recordStatus"]

	# Create blank dictionary
	line = {}

	# Loop through row and header to create the dictionary
	for index, value in enumerate(element):
		line[headers[index]] = (element[index])

	return json.dumps(line)

def convertPriceToInt(element):

	line = json.loads(element)

	line['price'] = int(line['price'])

	return json.dumps(line)

def runPipeline(input, output):

	logger.info('Starting pipeline')

	# Start of Pipeline
	try:
		with beam.Pipeline() as p:
			parsed_csv = (	p
							| 'Read input file' >> beam.io.ReadFromText(input) # Read the CSV file
							| 'Parse file' >> beam.Map(convertListToCsv) # Parse each row in the CSV format
							| 'Convert to JSON' >> beam.Map(convertListToJson) # Parse each row into JSON
							| 'Convert Price to Integer' >> beam.Map(convertPriceToInt) # Parse each row into JSON
							| 'Print output' >> beam.io.WriteToText(file_path_prefix=output, file_name_suffix='.json') # Write Output
			             )
	except Exception as e:
		logger.error('Pipeline encountered error', exc_info=True)
		exit(1)

	logger.info('End of pipeline')

## Start of Code ##

# Retrieve File from Source
getPricePaidFile('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv')

# Run Beam Pipeline
runPipeline('price-paid-data.csv', 'price-paid-data')

# Clean Up
cleanUp('price-paid-data.csv')
