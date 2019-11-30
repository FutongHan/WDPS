import requests

def search(domain, query, size):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size': size})
    id_labels = []
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):

            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            freebase_score = hit.get('_score', {})
			
            id_labels.append( (freebase_id, freebase_label, freebase_score) )
           
    return id_labels  

if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY, SIZE = sys.argv
    except Exception as e:
        print('Usage: python elasticsearch.py DOMAIN QUERY SIZE')
        sys.exit(0)

    print(search(DOMAIN, QUERY, SIZE))
