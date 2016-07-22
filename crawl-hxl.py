#!/usr/bin/python3
"""Crawl HXL datasets on HDX and generate stats.

See README.md for more details.

Started 2016-07-22 by David Megginson
"""

import ckanapi
import hxl

import pprint

# read configuration values from config.py
import config

ckan = ckanapi.RemoteCKAN(config.CONFIG['ckanurl'], apikey=config.CONFIG['apikey'], user_agent=config.CONFIG.get('user_agent', None))
"""The CKAN API object"""

def process_datasets(datasets):
    """Do something with a dataset tagged hxl"""
    for dataset in datasets:
        print(dataset['name'])
        for resource in dataset['resources']:
            if is_hxl(resource['url']):
                print("  {} ({})".format(resource['name'], resource['format']))

def find_hxl_datasets(start, rows):
    """Return a page of HXL datasets."""
    return ckan.action.package_search(start=start, rows=rows, fq="tags:hxl")

def is_hxl(url):
    """Try to parse as a HXL dataset."""
    try:
        hxl.data(url).columns
        return True
    except:
        return False
    
def crawl_datasets():
    """Crawl through all datasets tagged 'hxl'"""
    start = 0
    rows = 50
    result = find_hxl_datasets(start, rows)
    total = result['count']

    print("Found {} datasets.".format(total))

    while start < total:
        process_datasets(result['results'])
        start += rows
        result = find_hxl_datasets(start, rows)

#
# Main loop
#
crawl_datasets()

# end
