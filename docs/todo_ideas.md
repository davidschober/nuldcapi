
## 2021-03-02
```
es = elasticsearch.Elasticsearch('https://dcapi.stack.rdc-staging.library.northwestern.edu/search/', send_get_body_as='POST', timeout=30)

### query
q = {"query":{"bool":{"must":{"match":{"model.name":"Image"}}}}}
#### Works
res=es.search(index="meadow", body=q)
l = [hit['_source'] for hit in res['hits']['hits']]

#### 502s
res=elasticsearch.helpers.scan(es, query=q, index='meadow')

### Works 
res=elasticsearch.helpers.scan(es, query=q, index='common')
l = [r['_source'] for r in res]

```

## Using es dsl instead of raw es
Generator of a scan object

>>> s = Search(using=client, index='common').query('match', collection__id='c4f30015-88b5-4291-b3a6-8ac9b7c7069c').query('match', model__name="Image").scan()

s = Search(using=client, index='common').query('match', model__name="Image").scan()


It uses a special data type so you can access using dot notation
>>> items = [item for item in s]
>>> items[0].title.primary
['Anderson, M.L.']

It's a special type
>>> type(items[0].title.primary)
<class 'elasticsearch_dsl.utils.AttrList'>


You can turn it into a dict and operate on it like a dict
items[0].to_dict()

You can also attack them at the 
>>> [s.label for s in items[0].subject]
['Illinois--Evanston', 'Photographs', 'College students', 'Northwestern University (Evanston, Ill.)']

fields=['label', 'alternate', 'test']
Values={'alternate':[1,2,3], 'primary':[5,6,7]}
If len(values)>0
[join('|').field for values.field in fi]

Possibly use pandas 

https://stackoverflow.com/questions/25186148/creating-dataframe-from-elasticsearch-results#41092377


## Useful old queries

def query_fileset_title_matching(match):

    query = {
            "size": "500",
            "query": {
                "bool": {
                    "must": [
                        {"term": {"model.name.keyword": "FileSet"}},
                        {
                            "wildcard": {
                                "simple_title.keyword": {
                                    "value": match 
                                    }
                                }
                            }
                        ]
                    }
                }
            }
    return query

