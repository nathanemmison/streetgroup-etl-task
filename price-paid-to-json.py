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

def cleanUp():

	logger.info('Cleaning up file price-paid-data.csv')

	# Ensure file exists before removing
	if os.path.isfile('price-paid-data.csv'):
		os.remove('price-paid-data.csv')
	else:
		logger.error('File price-paid-data.csv not found.')
		exit(1)

## Start of Code ##

# Retrieve File from Source
getPricePaidFile('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv')

# Clean Up
cleanUp()
