from nuldcapi import helpers 
from docopt import docopt

def dc2csv():
    """DC2CSV:
    USAGE:
      dc2csv (-c <collection> | -q <query>) [(-f <fields> | -a) -e <environment>] <output>
      dc2csv (-c <collection> | -q <query>) [(-f <fields> | -a | -m) -e <environment>] <output>

    OPTIONS:
      -h --help                     Show this screen.
      -c --collection <collection>  Collection ID! (e.g. 1c2e2200-c12d-4c7f-8b87-a935c349898a)
      -q --query <query>            Query string style query (e.g. "New York"+Chicago)
      -f --fields <fields>          A comma-separated list of fields 
                                    [default: id,descriptiveMetadata.title,ark,collection,descriptiveMetadata.subject.displayFacet]
      -e --env <env>                environment to run against [default: production]
      -a --allfields                Get all available fields. Ineffiecient. Use sparingly.
      -m --meadow                   meadow style metadata dump

    COMMON FIELDS:
    id, title, permalink, subject(.label), thumbnail_url, creator(.uri), 
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
    fields = args['--fields'].split(',')

    if args['--collection']:
        # Set the query to the collection ID
        print(args['--collection'])
        args['--query'] = f'collection.id:{args["--collection"]}'
        print(args['--query'])

    query = helpers.query_for_query_string('work', args['--query'])

    # kick it off
    if args['--allfields']:
        # If someone threw the flag, get all the fields. 
        fields = helpers.get_all_fields_from_set(helpers.get_search_results(args['--env'], query))
        fields.sort()

    if args['--meadow']:
        fields = ["id", "accession_number","collection.id","published","visibility","admin_set-batch","preservation_level","project_name","project_desc","project_proposer","project_manager","project_task_number","project_cycle","status","abstract","alternate_title","ark","box_name","box_number","caption","catalog_key","citation","description","folder_name","folder_number","identifier","keywords","legacy_identifier","notes","terms_of_use","physical_description_material","physical_description_size","provenance","publisher","related_material","rights_holder","scope_and_contents","series","source","table_of_contents","title","license.uri","rights_statement.uri","contributor-batch","creator.uri","genre.uri","language.uri","location.uri","style_period.uri","subject-batch","technique.uri","date_created","related_url"]
        
    results = helpers.get_search_results(args['--env'], query) 
    data = helpers.get_results_as_list(results, fields) 
    #print(list(data))
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
    results = helpers.get_search_results(args['--env'], helpers.query_works_with_multiple_filesets('Image', args['--match']))
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
    -f <fields>, --fields <fields>  comma separated [default: id,title,subject,permalink,collection]
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


