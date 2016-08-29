#!/usr/bin/python3
"""Crawl HXL datasets on HDX and generate stats.

See README.md for more details.

Started 2016-07-22 by David Megginson
"""

import ckanapi, hxl, json, sys, time

# read configuration values from config.py
import config

DELAY = 1
"""Time delay in seconds between files, to give source APIs a rest."""

ckan = ckanapi.RemoteCKAN(config.CONFIG['ckanurl'], apikey=config.CONFIG['apikey'], user_agent=config.CONFIG.get('user_agent', None))
"""The CKAN API object"""


def process_dataset(dataset):
    """Do something with a dataset tagged hxl"""
    record = {
        'type': 'dataset',
        'name': dataset['name'],
        'title': dataset['title'],
        'source': dataset['dataset_source'],
        'resources': []
    }
    for resource in dataset['resources']:
        try:
            resource_info = {
                'type': 'resource',
                'name': resource['name'],
                'url': resource['url'],
                'columns': []
            }
            columns = hxl.data(resource['url']).columns
            resource_info['columns'].append([{
                'tag': column.tag,
                'display_tag': column.display_tag,
                'attributes': list(column.attributes)
            } for column in columns])
            record['resources'].append(resource_info)
        except:
            print("  Skipped {} (not valid HXL)".format(resource['name']), file=sys.stderr)
            return None
    if record['resources']:
        return record
    else:
        return False
        

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
    result_start_pos = 0
    result_page_size = 25
    result = find_hxl_datasets(result_start_pos, result_page_size)
    result_total_count = result['count']

    print("Found {} datasets.".format(result_total_count), file=sys.stderr)

    while result_start_pos < result_total_count:
        for dataset in result['results']:
            index += 1
            print("Dataset {}: {}".format(index, dataset['name']), file=sys.stderr)
            record = process_dataset(dataset)
            if record:
                print(json.dumps(record, indent=4) + ',')
        result_start_pos += result_page_size
        result = find_hxl_datasets(result_start_pos, result_page_size)
        time.sleep(DELAY)

#
# Main loop
#
print("[");
crawl_datasets()
print("]")

# end
