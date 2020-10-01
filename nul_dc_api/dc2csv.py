from nul_dc_api import helpers
from docopt import docopt

def main():
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
    args = docopt(main.__doc__, version='.1') 
    print(args)
    query = helpers.build_collection_query(args['--collection']) 
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

if __name__ == '__main__':
    main()


