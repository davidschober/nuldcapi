import elasticsearch
from elasticsearch import helpers

def get_fields_from_string(field_string):
    """returns a list from a comma separated string"""
    return [f.strip() for f in field_string.split(',')]

def get_search_results(environment, query):
    """Takes an environment and a query and returns an iterable of all results
    using the 'scan' function in es.helpers. Scan is an efficient pager.

    >>> get_search_results('staging', query)
    """

    # pick an environment 
    if environment == 'production':
        proxy = 'https://5et90kbva4.execute-api.us-east-1.amazonaws.com/latest/search/'
    if environment == 'staging':
        proxy = 'https://bxzhc8nucl.execute-api.us-east-1.amazonaws.com/latest/search/'
    # create an es instance 
    es = elasticsearch.Elasticsearch(proxy, send_get_body_as='POST')
    # return the results 
    return helpers.scan(es, index='common', query=query)

def build_collection_query(collection_id):
    """ Build a query for a collection based on ID. This is just
    a helper function. ES queries can be a bit nested and gnarly, so 
    this function just helps
    """

    query = {
            "size": "100",
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "model.name": "Image"
                                }
                            },
                        {
                            "match": {
                                "collection.id": collection_id 
                                }
                            }
                        ]
                    }
                },
            }
    
    return query 

def get_results_as_list(search_results, fields):
    """ Gets all items in a collection and returns the identified fields(list)
    This function flattens all nested data ham-fistedly, favoring labels over URIs for
    all metadata. Any list elements are separated by a semi-colon and turned to a string. 
    """

    # loop through items and get the important bits, stitch lists together with semicolons
    for work in search_results:
        work_metadata = []
        for field in fields:
            field_data = work.get('_source').get(field) 
            # Titles are special, if you request just 'title' get primary.
            if field == 'title':
                # there may be more than one title, join it by a semicolon
                work_metadata.append('; '.join(field_data.get('primary')))
            elif field == 'alternate-title':
                # grab the alternate title. Hardcoded this one. It's a special case
                field = 'title'
                field_data = work.get('_source').get(field) 
                work_metadata.append('; '.join(field_data.get('alternate')))
            elif field == 'permalink':
                # prepend the resolver url to the front of the ark
                work_metadata.append('https://n2t.net/'+field_data)
            elif field == 'thumbnail_url':
                # This just makes resolve to a jpg. I should make this configurable at the commandline
                work_metadata.append(field_data+'/full/!300,300/0/default.jpg')
            elif type(field_data) is dict:
                # print(field_data)
                work_metadata.append(field_data.get('label', field_data))
            elif type(field_data) is list:
                # create a flattened list of items like a list of subjects, return the full item if there's 
                # no label. Join it by a semi-colon. 
                flattened_list = [item.get('label', item) for item in field_data]
                work_metadata.append(';'.join(flattened_list))
            else:
                # These should be straight strings. 
                work_metadata.append(field_data)
        # build the list. I think this should be a generator
        yield work_metadata

def save_as_csv(headers, data, output_file):
    import unicodecsv as csv
    with open(output_file, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

