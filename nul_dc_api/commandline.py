from nul_dc_api import helpers
from docopt import docopt

def dc2csv():
    """DC2CSV:
    USAGE:
      dc2csv (-c <collection_id> | -q <query>) [(-f <fields> | -a) -e <environment>] <output>

    OPTIONS:
      -h --help                     Show this screen.
      -c --collection <collection>  Collection ID! (e.g. 1c2e2200-c12d-4c7f-8b87-a935c349898a)
      -q --query <query>            Query string style query (e.g. "New York"+Chicago)
      -f --fields <fields>          A comma-separated list of fields 
                                    [default: id,title,permalink,subject.label]
      -e --env <env>                environment to run against [default: production]
      -a --allfields                Get all available fields. Ineffiecient. Use sparingly.

    COMMON FIELDS:
    id, title, permalink, subject(.label), thumbnail_url, creator(.label), 
    collection(.title), iiif_manifest, member_ids,

    EXAMPLES:
    Get all works from the collection with ID 
    $ dc2csv -c 1c2e2200-c12d-4c7f-8b87-a935c349898a ~/test.csv
    
    Get all works from a collection with "poster" somewhere in the title
    $ dc2csv -q 'collection.\*:Poster*' ~/test.csv

    Get some works that have "smokey" and "bear" in the description and were 
    created between 1930 and 1937
    $ dc2csv -q 'description:(Smokey AND Bear) OR date:[1930-01-01 TO 1937-01-01]' ~/test.csv
    """
    args = docopt(dc2csv.__doc__, version='.1') 

    if args['--collection']:
        # Set the query to the collection ID
        args['--query'] = f'collection.id:{args["--collection"]}'

    query = helpers.query_for_query_string('Image', args['--query'])

    # kick it off
    if args['--allfields']:
        # If someone threw the flag, get all the fields. 
        fields = helpers.get_all_fields_from_set(helpers.get_search_results(args['--env'], query))
        fields.sort()

    else:
        fields = args['--fields'].split(',')
        
    results = helpers.get_search_results(args['--env'], query) 
    data = helpers.get_results_as_list(results, fields) 
    helpers.save_as_csv(fields, data, args['<output>'])

def dcfilesmatch():
    """dcfilesmatch:
    Gets multifile works with default filenames matching the match e.g. *.tif

    USAGE:
    dcfilesmatch [-m <match> -f <fields> -e <env>] <output>

    OPTIONS:
    -m <match>, --match <match>     match a fileset title with wildcard [default: *.tif]
    -f <fields>, --fields <fields>  comma separated [default: id,title,permalink,collection.title,member_ids]
    -e <env>, --env <env>           environment [default: production]
    -h, --help                      display this help
    """

    args = docopt(dcfilesmatch.__doc__, version='.1')
    fields = args['--fields'].split(',')
    fids = helpers.get_fileset_ids_with_title_matching(args['--env'], f"simple_title:{args['--match']}")
    works = helpers.get_search_results(args['--env'], helpers.query_works_with_multiple_filesets())
    results = helpers.filter_works_by_fileset_matching(works, fids)
    data = helpers.get_results_as_list(results, fields)
    helpers.save_as_csv(fields, data, args['<output>'])

def dc2xml():
    """dc2xml:
    Gets results and formats them as simple xml based on field names. You can also
    Map fields using a fieldmap. 

    USAGE:
    dc2xml -q <query> [-f <fields> -m <map> -e <env>] <output>

    OPTIONS:
    -q <query>, --query <query>     match a fileset title with wildcard [default: *.tif]
    -f <fields>, --fields <fields>  comma separated [default: id,title,subject.label,permalink,collection.title]
    -e <env>, --env <env>           environment [default: production]
    -m <map>, --map <map>           a list of fields to map
    -h, --help                      display this help
    """
    
    args = docopt(dc2xml.__doc__, version='.1')
    fields = args['--fields'].split(',')
    fieldmap = args['--map']
    if fieldmap:
        fieldmap = args['--map'].split(',')
        #verify we can zip them
        if len(fields) != len(fieldmap):
            raise SystemExit('ERROR: the fieldmap and fields do not have the same number of elements')

    query = helpers.query_for_query_string('Image', args['--query'])
    res = helpers.get_search_results(args['--env'], query)
    res_dict = helpers.results_to_simple_dict(res, fields, fieldmap)
    
    helpers.save_xml(res_dict, args['<output>'])

if __name__ == '__main__':
    dc2csv()


