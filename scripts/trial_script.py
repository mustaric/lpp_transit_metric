#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 13:46:12 2018

@author: smullally
"""

# This script queries MAST for TESS FFI data for a single sector/camera/chip 
# combination and downloads the data from the AWS public dataset rather than 
# from MAST servers.

# Working with http://astroquery.readthedocs.io/en/latest/mast/mast.html
# Make sure you're running the latest version of Astroquery:
# pip install https://github.com/astropy/astroquery/archive/master.zip

from astroquery.mast import Observations
import boto3

# Query for observations in sector 1 (s0001), camera 1, chip 1 (1-1)
obsTable = Observations.query_criteria(obs_id="tess-s0001-1-1")

# Get the products associated with these observations
products = Observations.get_product_list(obsTable)

# Return only the calibrated FFIs (.ffic.fits)
filtered = Observations.filter_products(products, 
                                        productSubGroupDescription="FFIC",
                                        mrp_only=False)
#%%
len(filtered)
# > 1282

# Enable 'cloud mode' for module which will return S3-like URLs for FITs files
# e.g. s3://stpubdata/tess/.../tess2018206192942-s0001-1-1-0120-s_ffic.fits
Observations.enable_cloud_dataset()

# Grab the S3 URLs for each of the observations
s3_urls = Observations.get_cloud_uris(filtered)

s3 = boto3.resource('s3')
#%%
# Create an authenticated S3 session. Note, download within US-East is free
# e.g. to a node on EC2.
s3_client = boto3.client('s3')
                        # aws_access_key_id='YOURAWSACCESSKEY',
                        # aws_secret_access_key='YOURSECRETACCESSKEY')

bucket = s3.Bucket('stpubdata')

# Just download a few of the files (remove the [0:3] to download them all)
for url in s3_urls[0:1]:
  # Extract the S3 key from the S3 URL
  fits_s3_key = url.replace("s3://stpubdata/", "")
  root = "data/tess/" + url.split('/')[-1]
bucket.download_file(fits_s3_key, root, ExtraArgs={"RequestPayer": "requester"})