import nuproxy
import sys
def save_collection_as_csv(output_fields, outputfile, environment, collection_id):
    """ Example showing how to save a collection as a csv
    
    ## Example Collections    
    ### production, hesler 
    collection_id = "c4f30015-88b5-4291-b3a6-8ac9b7c7069c"
    
    ### staging, berkeley
    collection_id = "52c177f6-97d2-4f58-a57e-2e87dc297aa8"
    """
    
    results = nuproxy.get_search_results(environment, nuproxy.build_collection_query(collection_id)) 
    data = nuproxy.get_results_as_list(results, output_fields) 
    nuproxy.save_as_csv(output_fields, data, outputfile)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    # parser.add_argument("-r","--raw", help="dump raw output")
    parser.add_argument("-f", action="append", dest="fields",
                        default=['id', 'accession_number', 'title'],
                        help="""Additional field to include in CSV, default fields include id, title, accession_number repeatable. Additional fields include
                        - thumbnail_url
                        - permalink
                        - subject""")
    parser.add_argument("-o", action="store", dest="output_file",
                        help="Output File")
    parser.add_argument("-e", action="store", dest="environment", default="production",
                        help="environment 'production' or 'staging' ")
    parser.add_argument("-c", action="store", dest = "collection_id", help="collection ID")
    
    args = parser.parse_args()

    # kick it off
    save_collection_as_csv(args.fields, args.output_file, args.environment, args.collection_id)
    
    
