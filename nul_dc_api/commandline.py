from nul_dc_api import helpers
from docopt import docopt

def dc2csv():
    """DC2CSV:
    USAGE:
      dc2csv -c <collection_id> [(-f <fields> | -a) -e <environment>] <output>

    OPTIONS:
      -h --help                     Show this screen.
      -c --collection <collection>  Collection ID (e.g. 1c2e2200-c12d-4c7f-8b87-a935c349898a)
      -f --fields <fields>          A comma-separated list of fields [default: id,title,permalink,subject.label]
      -e --env <env>                environment to run against [default: production]
      -a --allfields                Get all available fields. Ineffiecient. Use sparingly.

    COMMON FIELDS:
    id, title, permalink, subject(.label), thumbnail_url, creator(.label), collection(.title),
    iiif_manifest, member_ids,
    """
    args = docopt(dc2csv.__doc__, version='.1') 
    query = helpers.query_for_collection_with_id(args['--collection']) 
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
    fids = helpers.get_fileset_ids_with_title_matching(args['--env'], args['--match'])
    works = helpers.get_search_results(args['--env'], helpers.query_works_with_multiple_filesets())
    results = helpers.filter_works_by_fileset_matching(works, fids)
    data = helpers.get_results_as_list(results, fields)
    helpers.save_as_csv(fields, data, args['<output>'])

if __name__ == '__main__':
    dc2csv()


