from nuproxy import helpers 
import sys

def main():
    """ Example showing how to save a collection as a csv
    
    ## Example Collections    
    ### production, hesler 
    collection_id = "c4f30015-88b5-4291-b3a6-8ac9b7c7069c"
    
    ### staging, berkeley
    collection_id = "52c177f6-97d2-4f58-a57e-2e87dc297aa8"
    """
    import argparse
    
    parser = argparse.ArgumentParser()
    # parser.add_argument("-r","--raw", help="dump raw output")
    parser.add_argument("-f", action="append", dest="fields",
                        default=['id', 'accession_number', 'title'],
                        help="""Additional field to include in CSV, default fields include id, title, accession_number repeatable. Additional fields include
                        - thumbnail_url
                        - permalink
                        - subject""")
    parser.add_argument("-o", action="store", dest="outputfile",
                        help="Output File")
    parser.add_argument("-e", action="store", dest="environment", default="production",
                        help="environment 'production' or 'staging' ")
    parser.add_argument("-c", action="store", dest = "collection_id", help="collection ID")
    
    args = parser.parse_args()

    # kick it off
    results = helpers.get_search_results(args.environment, helpers.build_collection_query(args.collection_id)) 
    data = helpers.get_results_as_list(results, args.fields) 
    helpers.save_as_csv(args.fields, data, args.outputfile)

if __name__ == '__main__':
    main()
    
    
