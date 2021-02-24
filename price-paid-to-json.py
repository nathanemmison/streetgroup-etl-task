# Price Paid Data Set to JSON
# Author - Nathan Emmison

import requests
import os
import logging

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

## Start of Code ##

# Retrieve File from Source
getPricePaidFile('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv')

# Read CSV into Beam
readCsv('price-paid-data.csv')

# Clean Up
cleanUp('price-paid-data.csv')
