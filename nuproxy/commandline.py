import sys
from . import helpers

def main():
    """ Example showing how to save a collection as a csv
    
    ## Example Collections    
    ### production, hesler 
    collection_id = "c4f30015-88b5-4291-b3a6-8ac9b7c7069c"
    
    ### staging, berkeley
    collection_id = "52c177f6-97d2-4f58-a57e-2e87dc297aa8"
    """
    import argparse, textwrap
    
    parser = argparse.ArgumentParser()
    # parser.add_argument("-r","--raw", help="dump raw output")
    parser.add_argument("-f", "--field", action="append", dest="fields",
                        default=['id', 'accession_number', 'primary-title'],
                        help= '''\
                                Additional field to include in CSV, default fields include id, primary-title, accession_number repeatable. Additional fields include:
                        thumbnail_url; 
                        permalink;
                        subject;
                        creator;
                        iiif_manifest''')
    parser.add_argument("-o", "--out", action="store", dest="outputfile", required=True,
                        help="Output File")
    parser.add_argument("-a", "--allfields", 
            action="store_true", 
            dest="allfields", 
            help="This will pull all fields available in the set. This may take some time with a large collection.")
    parser.add_argument("-e", "--environment", action="store", dest="environment", default="production", required=True,
            help="environment 'production' or 'staging'. Default: production")
    parser.add_argument("-c", "--collection", action="store", dest = "collection_id", required=True, help="collection ID")
    
    args = parser.parse_args()
    query = helpers.build_collection_query(args.collection_id) 
    # kick it off
    if args.allfields:
        # If someone threw the flag, get all the fields. 
        fields = helpers.get_all_fields_from_set(helpers.get_search_results(args.environment, query))

        fields.sort()
    else:
        fields = args.fields
    results = helpers.get_search_results(args.environment, query) 
    data = helpers.get_results_as_list(results, fields) 
    helpers.save_as_csv(fields, data, args.outputfile)

if __name__ == '__main__':
    main()
    
    
