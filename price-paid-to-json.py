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

def parse_row(element):
	for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL):
		return line

def readCsv(input):

	logger.info('Starting pipeline')

	headers = ['transactionUniqueIndentifier', 'Price', 'dateOfTransfer', 'Postcode', 'propertyType', 'oldOrNew', 'Duration', 'PAON', 'SAON', 'Street', 'Locality', 'townCity', 'District', 'County', 'PPDType', 'recordStatus']

	try:
		with beam.Pipeline() as p:
			parsed_csv = (	p
			           		| 'Read input file' >> beam.io.ReadFromText(input) # Read the CSV file
			           		| 'Parse file' >> beam.Map(parse_row) # Parse each row in the CSV format
			           		| 'Print output' >> beam.io.WriteToText(file_path_prefix='price-paid-data', file_name_suffix='.json') # Write Output
			             )
	except Exception as e:
		logger.error('Pipeline encountered error', exc_info=True)
		exit(1)

	logger.info('End of pipeline')

## Start of Code ##

# Retrieve File from Source
getPricePaidFile('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv')

# Read CSV into Beam
readCsv('price-paid-data.csv')

# Clean Up
cleanUp('price-paid-data.csv')
