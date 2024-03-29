from nuldcapi import terms
import elasticsearch
from elasticsearch import helpers
import unicodecsv as csv

def format_raw(field, source_dict): 
    """get raw field and stringify"""
    return str(source_dict.get(field,""))

def format_default(field, source_dict):
    """Handles standard fields and flattens nested data"""
    #see if there's a dot notation
     
    field_metadata = source_dict
    for f in field.split('.'):
        if isinstance(field_metadata, dict):
            field_metadata = field_metadata.get(f)
        elif isinstance(field_metadata, list) and all(isinstance(d, dict) for d in field_metadata):
            field_metadata = [i.get(f) for i in field_metadata]
 
    return field_metadata

def format_permalink(field, source_dict):
    """formats permalink"""

    ark = format_default("descriptiveMetadata.ark", source_dict)
    #permalink = format_default(field, source_dict)
    return f"https://n2t.net/{ark}"

def format_thumbnail(field, source_dict):
    """formats thumnbnail"""
    field = format_default("representativeFileSet.url", source_dict)
    return f"{field}/full/!300,300/0/default.jpg"

def flatten_metadata(source_dict, field):
    """ Takes a nested dictionary of a works metadata, gets and flattens a field. 
    Special cases are handled. It returns a simple string 

    ## Example
    >>> source = {'title':{'primary':['title1','title2'],'alternate':['alt1','alt2']}, 
    ...          'representativeFileSet':{'url':'http://thumb'}, 
    ...          'list_field':['1','2','3'],
    ...          'list_of_dicts':[{'label':'label1','uri':'uri1'}, {'label':'label2','uri':'uri2'}],
    ...          'dict_field': {'label':'dict_label', 'uri':'http://uri'}, 
    ...          'string':'string'}
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

    # Helpers to handle special fields
    handler = {'raw' : format_raw,
            'permalink' : format_permalink,
            'thumbnail_url' : format_thumbnail,
            } 

    #Return the proper formatted metadata
    field_metadata = handler.get(field, format_default)(field, source_dict)
    # take all metadata and flatten_to_list    
    flatten_to_list = lambda l: sum(map(flatten_to_list,l),[]) if isinstance(l,list) else [str(l)]    
    return ' | '.join(flatten_to_list(field_metadata)) 

def get_search_results(environment, query):
    """Takes an environment and a query and returns an iterable of all results
    using the 'scan' function in es.helpers. Scan is an efficient pager.
    """

    # pick an environment 
    proxy = {'production' : 'https://dcapi.stack.rdc.library.northwestern.edu/search/',
            'staging' : 'https://dcapi.stack.rdc-staging.library.northwestern.edu/search/'
            }
    # added ssl and port 443 to see if it solves timeout issue
    es = elasticsearch.Elasticsearch(proxy[environment], send_get_body_as='POST', timeout=30, max_retries=10, retry_on_timeout=True)
    # return the results 
    # return es.search(index='meadow', body={"query":query})
    return helpers.scan(es, query=query, index='meadow', size=100)
    
def get_all_fields_from_set(search_results):
    """ returns a flat, unique list of all fields from a search query. This can be fed back
    into a fresh query result to flatten the results for a CSV. It is not as efficient as 
    passing fields directly as you have to make two queries

    ## Example
    >>> res = [{'_source': {'key':'1', 'key2':'2', 'key3':'3'}}, 
    ...     {'_source':{'key1':'1-2', 'key3':'1-3', 'key2':'1-2'}}]
    >>> k = get_all_fields_from_set(res)
    >>> k.sort()
    >>> print(k)
    ['key', 'key1', 'key2', 'key3']
    """
    
    return list(set([field for work in search_results for field in work.get('_source').keys()]))

def get_results_as_list(search_results, fields):
    """ Gets all items in a collection and returns the identified fields(list)
    This function flattens all nested data ham-fistedly, favoring labels over URIs for
    all metadata. Any list elements are separated by a semi-colon and turned to a string. 

    ## Example
    >>> res = [{'_source': {'key':'1', 'key2':'2', 'key3':'3'}}, 
    ...    {'_source':{'key1':'1-2', 'key3':'1-3', 'key2':'1-2'}}]
    >>> list(get_results_as_list(res, ['key','key3']))
    [['1', '3'], ['None', '1-3']]
    """

    for work in search_results:
        #Get the metadata dictionary
        work_metadata = work.get('_source')
        yield [flatten_metadata(work_metadata, field) for field in fields]

def query_for_query_string(model, match):
    """ Uses teh query string query to return results. Examples on the elasticsearch
    site <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html>

    Examples:
        >>> query_for_query_string('Image', '"Chicago" AND "New York"')
        {'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': '"Chicago" AND "New York"'}}]}}}
        
        >>> #Colection with poster somewhere (Note had to double escape to show. Not sure how
        >>> #To deal with that in python
        >>> query_for_query_string('Image', 'collection.\*:Poster*')
        {'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': 'collection.\\\\*:Poster*'}}]}}}
        >>> #Collection matching a specific ID
        >>> query_for_query_string('Image', 'collection.id:1c2e2200-c12d-4c7f-8b87-a935c349898a')
        {'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': 'collection.id:1c2e2200-c12d-4c7f-8b87-a935c349898a'}}]}}}

        >>> #Collection with either Smakey and Bear OR a date range in date
        >>> query_for_query_string('Image', 'description:(Smokey AND Bear) OR date:[1930-01-01 TO 1937-01-01]')
        {'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': 'description:(Smokey AND Bear) OR date:[1930-01-01 TO 1937-01-01]'}}]}}}
    """

    query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"model.name": model}},
                        {"query_string": {"query": match}}
                    ]
                    }
                }
            }

    return query

def query_works_with_multiple_filesets(model, match, number_of_filesets):
    """ returns a query that looks for works with multiple fileset_id_list. Doesn't work yet. """

    query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"model.name": model}},
                        {"query_string": {"query": match}}
                        ],
                    "filter": [{"script": {"script": "doc['fileSets.id'].values.size()>="+number_of_filesets}}]
                }
            }
        }
    return query

def results_to_simple_dict(results, fields, fieldmap=None):
    """Takes a list of formatted results and a set of fields and maps to a simple dict.
    This can be passed to something like dicttoxml to generate xml. 

    EXAMPLE:
    >>> res = [{'_source': {'key':'1', 'key2':'2', 'key3':'3'}}, 
    ...    {'_source':{'key1':'1-2', 'key3':'1-3', 'key2':'1-2'}}]
    >>> list(results_to_simple_dict(res, ['key','key2'], ['newfield','newfield2']))
    [{'newfield': '1', 'newfield2': '2'}, {'newfield': 'None', 'newfield2': '1-2'}]
    """
    
    results_list = get_results_as_list(results, fields)
    # if there's a fieldmap, use that
    if fieldmap:
        fields = fieldmap
    # create a generator so that we can reserve memory on large datasets
    return (dict(zip(fields,work_meta)) for work_meta in results_list)

def save_as_csv(headers, data, output_file):
    """outputs a CSV using unicodecsv"""
    with open(output_file, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)
   
def save_xml(res_dict, output_file):
    """takes results as a list of dicts and writes them out to xml"""
    import dicttoxml

    xml = dicttoxml.dicttoxml(res_dict, attr_type=False)
    with open(output_file, 'wb') as xmlfile:
        xmlfile.write(xml)
    
