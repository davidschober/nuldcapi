import elasticsearch
from elasticsearch import helpers
import unicodecsv as csv

def get_fields_from_string(field_string):
    """returns a list from a comma separated string"""
    return [f.strip() for f in field_string.split(',')]

def format_for_csv(item):
    # returns a list of items as a string delimited by the delimiter
    if type(item) is list: 
        return ' | '.join([str(i) for i in item])
    else:
        return str(item)

def get_search_results(environment, query):
    """Takes an environment and a query and returns an iterable of all results
    using the 'scan' function in es.helpers. Scan is an efficient pager.
    """

    # pick an environment 
    if environment == 'production':
        # proxy = 'https://5et90kbva4.execute-api.us-east-1.amazonaws.com/latest/search/'
        proxy = 'https://dcapi.stack.rdc.library.northwestern.edu/search/'
        print(proxy)
    if environment == 'staging':
        # proxy = 'https://bxzhc8nucl.execute-api.us-east-1.amazonaws.com/latest/search/'
        proxy = 'https://dcapi.stack.rdc-staging.library.northwestern.edu/search/'
    # create an es instance 
    # added ssl and port 443 to see if it solves timeout issue
    es = elasticsearch.Elasticsearch(proxy, send_get_body_as='POST', timeout=30, max_retries=10, retry_on_timeout=True)
    # return the results 
    return helpers.scan(es, index='common', query=query)

def build_collection_query(collection_id):
    """ Build a query for a collection based on ID. This is just
    a helper function. ES queries can be a bit nested and gnarly, so 
    this function just helps
    """

    query = {
            "size": "1000",
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

def flatten_metadata(source_dict, field):
    """ Takes a nested dictionary of a work's metadata, gets and flattens a field. 
    Special cases are handled. It returns a simple string 


    ## Example

    >>> source = {'title':{'primary':['title1','title2'],'alternate':['alt1','alt2']}, 
    ...          'thumbnail_url':'http://thumb', 
    ...          'list_field':['1','2','3'],
    ...          'list_of_dicts':[{'label':'label1','uri':'uri1'}, {'label':'label2','uri':'uri2'}],
    ...          'dict_field': {'label':'dict_label', 'uri':'http://uri'}, 
    ...          'string':'string'}


    >>> flatten_metadata(source, 'title')
    'title1 | title2 | alt1 | alt2'

    >>> flatten_metadata(source, 'title.primary')
    'title1 | title2'

    >>> flatten_metadata(source, 'dict_field.label')
    'dict_label'

    >>> flatten_metadata(source, 'string')
    'string'

    >>> flatten_metadata(source, 'thumbnail_url')
    'http://thumb/full/!300,300/0/default.jpg'

    >>> flatten_metadata(source, 'list_of_dicts.label')
    'label1 | label2'

    >>> flatten_metadata(source, 'list_field')
    '1 | 2 | 3'

    """

    field_data = source_dict.get(field)
    field_metadata = field_data

    if field == 'title':
        #join a bunch of title together, regardless of primary or alternate
        # Note, this could work to join any multi-dimensional field hard, but I'm not 
        # That makes sense. 
        field_metadata  = [title for title_lists in field_data.values() for title in title_lists]
        

    if field == 'permalink':
        # prepend the resolver url to the front of the ark
        field_metadata = f"https://n2t.net/{field_data}"

    if field == 'thumbnail_url':
        # This just makes resolve to a jpg. I should make this configurable at the commandline
        field_metadata = f"{field_data}/full/!300,300/0/default.jpg"

    if '-json' in field:
        # Get pseudo json back and separate lists with pipes. This ugly bit is for a prototype of meadow
        field =  field.rstrip('-json')
        if type(field_data) is list:
            field_metadata = [json for json in source_dict.get(field)]
        else:
            field_metadata = source_dict.get(field)

    if '-values' in field:
        # Get pseudo json back and separate lists with pipes. This ugly bit is for a prototype of meadow
        # I think I can make this generic
        field =  field.rstrip('-values')
        field_data = source_dict.get(field)
        if type(field_data) is list:
            field_metadata = [tuple(meta.values()) for meta in source_dict.get(field)]
        elif type(field_data) is dict:
            field_metadata = list(field_data.values())
        else:
            field_metadata = field_data

    if '.' in field:
        # This allows you to pull from nested 
        field, key = field.split('.')
        field_data = source_dict.get(field)

        if type(field_data) is list:
            field_metadata = [meta.get(key) for meta in field_data]
        elif type(field_data) is dict:
            field_metadata = format_for_csv(field_data.get(key))
        else:
            field_metadata = field_data

    return format_for_csv(field_metadata)

def get_results_as_list(search_results, fields):
    """ Gets all items in a collection and returns the identified fields(list)
    This function flattens all nested data ham-fistedly, favoring labels over URIs for
    all metadata. Any list elements are separated by a semi-colon and turned to a string. 
    """

    for work in search_results:
        #Get the metadata dictionary
        work_metadata = work.get('_source')
        yield [flatten_metadata(work_metadata, field) for field in fields]

def get_all_fields_from_set(search_results):
    """ returns a flat, unique list of all fields from a search query. This can be fed back
    into a fresh query result to flatten the results for a CSV. It is not as efficient as 
    passing fields directly as you have to make two queries
    """

    return list(set([field for work in search_results for field in work.get('_source').keys()]))

def save_as_csv(headers, data, output_file):
    """outputs a CSV using unicodecsv"""
    with open(output_file, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

