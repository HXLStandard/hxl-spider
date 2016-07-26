#!/usr/bin/python3
"""Crawl HXL datasets on HDX and generate stats.

See README.md for more details.

Started 2016-07-22 by David Megginson
"""

import ckanapi, hxl, json, sys, time

# read configuration values from config.py
import config

delay = 5
"""Time delay in seconds between files, to give source APIs a rest."""

ckan = ckanapi.RemoteCKAN(config.CONFIG['ckanurl'], apikey=config.CONFIG['apikey'], user_agent=config.CONFIG.get('user_agent', None))
"""The CKAN API object"""

def process_dataset(dataset, count_maps):
    """Do something with a dataset tagged hxl"""

    def increment(count_maps, type, key):
        if not count_maps.get(type):
            count_maps[type] = dict()
        if count_maps[type].get(key):
            count_maps[type][key] += 1
        else:
            count_maps[type][key] = 1
            
    for resource in dataset['resources']:
        try:
            columns = hxl.data(resource['url']).columns
        except:
            print("  Skipped {}".format(resource['name']), file=sys.stderr)
            return
        for column in columns:
            increment(count_maps, 'tags', column.tag)
            increment(count_maps, 'display_tags', column.display_tag)
            for attribute in column.attributes:
                increment(count_maps, 'attributes', attribute)
        if count_maps.get('total'):
            count_maps['total'] +=1
        else:
            count_maps['total'] = 1
        print("  Indexed {}".format(resource['name']), file=sys.stderr)
        print("  Waiting {} seconds ...".format(delay), file-sys.stderr)
        time.sleep(delay)

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
    index = 0
    start = 0
    rows = 25
    result = find_hxl_datasets(start, rows)
    total = result['count']
    count_maps = dict()

    print("Found {} datasets.".format(total), file=sys.stderr)

    while start < total:
        for dataset in result['results']:
            index += 1
            print("Dataset {}: {}".format(index, dataset['name']), file=sys.stderr)
            process_dataset(dataset, count_maps)
        start += rows
        result = find_hxl_datasets(start, rows)

    return count_maps

#
# Main loop
#
count_maps = crawl_datasets()
print(json.dumps(count_maps, indent=4))

# end
