import elasticsearch
from elasticsearch import helpers
import unicodecsv as csv

def filter_works_by_fileset_matching(work_results, fileset_id_list):
    """Matches a fileset name against match. This function is used to grab all filesets 
    matching a wildcard if the results have said fileset, then it will return a generator 
    with those works

    ## Example
    >>> works = [{'_source':{'member_ids':['1','2','3']}},
    ...     {'_source':{'member_ids':['5','6','7']}},
    ...     {'_source':{'member_ids':['2','3','4']}}]
    >>> fids = ['1','2']
    >>> list(filter_works_by_fileset_matching(works,fids))
    [{'_source': {'member_ids': ['1', '2', '3']}}, {'_source': {'member_ids': ['2', '3', '4']}}]
    """

    for work in work_results:
        if any(fid in fileset_id_list for fid in work.get('_source').get('member_ids')):
            yield work 

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
    # Most folks are looking for the human readable data, so filter for those on basic fields (e.g. title)
    find_fields = ['label', 'title', 'primary', 'alternate']

    if '-raw' in field:
        field =  field.split('-')[0]
        field_metadata = str(source_dict.get(field))
    # This is to prototype our mass update tool
    if '-batch' in field:
        field = field.split('-')[0]
        field_metadata = [f"{meta.get('role')}:{meta.get('uri')}" for meta in source_dict.get(field)]

    if field == 'permalink':
        # prepend the resolver url to the front of the ark
        field_metadata = f"https://n2t.net/{field_data}"

    if field == 'thumbnail_url':
        # This just makes resolve to a jpg. I should make this configurable at the commandline
        field_metadata = f"{field_data}/full/!300,300/0/default.jpg"

    if '.' in field:
        # This allows you to pull from nested 
        field, key = field.split('.')
        field_data = source_dict.get(field)
        find_fields = [key]

    # This deals with the nested nature of our metadata 
    if isinstance(field_data, dict):
        field_metadata = [v for k,v in field_data.items() if k in find_fields]
    # This makes me feel odd but sometimes lists have dicts and sometimes they're just lists
    if isinstance(field_data, list) and all(isinstance(d, dict) for d in field_data):
        field_metadata = [v for i in field_data for k,v in i.items() if k in find_fields]
    
    # take all metadata and flatten_to_list    
    flatten_to_list = lambda l: sum(map(flatten_to_list,l),[]) if isinstance(l,list) else [str(l)]    
    
    return ' | '.join(flatten_to_list(field_metadata)) 

def get_search_results(environment, query):
    """Takes an environment and a query and returns an iterable of all results
    using the 'scan' function in es.helpers. Scan is an efficient pager.
    """

    # pick an environment 
    if environment == 'production':
        proxy = 'https://dcapi.stack.rdc.library.northwestern.edu/search/'
        print(proxy)
    if environment == 'staging':
        proxy = 'https://dcapi.stack.rdc-staging.library.northwestern.edu/search/'
    # create an es instance 
    # added ssl and port 443 to see if it solves timeout issue
    es = elasticsearch.Elasticsearch(proxy, send_get_body_as='POST', timeout=30, max_retries=10, retry_on_timeout=True)
    # return the results 
    # return es.search(index='common', body=query)
    return helpers.scan(es, query=query, index='common')

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

def get_fileset_ids_with_title_matching(environment, match):
    """ Returns a list of ids with titles matching a wildcard. e.g. '*.tif' to
    find all filesets that still have a title with 'tif' in it."""
    
    file_results = get_search_results(environment, query_for_query_string('FileSet', match))
    return [f.get('_id') for f in file_results]

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
        {'size': '500', 'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': '"Chicago" AND "New York"'}}]}}}
        
        >>> #Colection with poster somewhere (Note had to double escape to show. Not sure how
        >>> #To deal with that in python
        >>> query_for_query_string('Image', 'collection.\*:Poster*')
        {'size': '500', 'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': 'collection.\\\\*:Poster*'}}]}}}
        >>> #Collection matching a specific ID
        >>> query_for_query_string('Image', 'collection.id:1c2e2200-c12d-4c7f-8b87-a935c349898a')
        {'size': '500', 'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': 'collection.id:1c2e2200-c12d-4c7f-8b87-a935c349898a'}}]}}}

        >>> #Collection with either Smakey and Bear OR a date range in date
        >>> query_for_query_string('Image', 'description:(Smokey AND Bear) OR date:[1930-01-01 TO 1937-01-01]')
        {'size': '500', 'query': {'bool': {'must': [{'match': {'model.name': 'Image'}}, {'query_string': {'query': 'description:(Smokey AND Bear) OR date:[1930-01-01 TO 1937-01-01]'}}]}}}
    """

    query = {
            "size": "500",
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

def query_works_with_multiple_filesets():
    """ returns a query that looks for works with multiple filesets"""
    query =  {
            "query": {
                "bool": {
                    "filter": {
                        "script": {
                            "script": {
                                "lang": "painless",
                                "source": "doc['member_ids.keyword'].values.length >= 2"
                                }
                            }
                        },
                    "must": [
                        {"term": {"model.name.keyword": "Image"}},
                        ]
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
    
