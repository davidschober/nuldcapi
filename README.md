# nul_dc_api

## Overview
A set of functions and sample CLI scripts to make dealing with data in our the nul_dc_api easier to deal with. In includes query builders for collections, works with multiple filesets and filesets matching wildcard values. In addition, there's a couple of helper functions to ham-fistedly flatten metadata down for export into CSV (or possibly other formats). By default it implements elasticsearch scrolling so it grabs all the results.

## Quickstart / Install
Install with PIP
`pip install git+https://github.com/davidschober/nul-dc-api.git`

## Included Commandline Scripts 

Turn a collection to CSV
`$ dc2csv -c <collection_id> <output>`

See all options
`$ dc2csv --help`

get just a couple of fields from the collection

`$ dc2csv -c <collection_id> -f id,title,subject,permalink,thumbnail_url

Grab all works that have filesets with \*.tif in the title
`$ dcfilesmatch <output>

## Using helpers in a script

Mostly this is just a bunch of quick and dirty helper functions. They will grow as people ask for different things. Import helpers, search on a collection, and export some fields to a CSV
```
>>> from dc_nul_api import helpers
>>> collection_id = '1c2e2200-c12d-4c7f-8b87-a935c349898a'
>>> q = helpers.query_for_collection_with_id(collection_id)
>>> res = helpers.get_search_results('production', q)
>>> fields = 'id,permalink,thumbnail_url,subject.label'.split(',')
>>> list_of_results = helpers.get_results_as_list(res, fields)
>>> helpers.save_as_csv(fields,data,'file_out.csv')
```

## Contributing and tests
This is built on Python 3.8.x and the elasticsearch library. Poetry was used for dependancy management and packaging. It makes life way easier than it used to be. Seriously, use it. 

There's some rudimentary sanity-checking doctests. Run them with
`python -m python -m doctest -v nul_dc_api/helpers.py`

Or if you're using poetry

`poetry run python -m doctest -v nul_dc_api/helpers.py`
